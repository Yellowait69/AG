import pandas as pd
import os
import logging
from src.database import DatabaseManager
from src.comparator import compare_dataframes
from sql.queries import QUERIES
from config.settings import INPUT_FILE, OUTPUT_DIR

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("--- Démarrage du Comparateur Auto-Activator (Mode Snapshot) ---")

    # 0. Vérification de l'environnement
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Dossier où chercher les snapshots sources (J0)
    snapshot_dir = os.path.join(OUTPUT_DIR, 'snapshots')

    # 1. Initialisation de la base de données
    try:
        db = DatabaseManager()
        if not db.test_connection():
            logger.error("Impossible de se connecter à la base de données. Arrêt.")
            return
    except Exception as e:
        logger.error(f"Erreur critique d'initialisation DB: {e}")
        return

    # 2. Chargement du fichier Excel d'entrée
    try:
        logger.info(f"Lecture du fichier d'entrée : {INPUT_FILE}")
        df_input = pd.read_excel(INPUT_FILE)
        df_input.columns = df_input.columns.str.strip()
    except FileNotFoundError:
        logger.error(f"Fichier d'entrée introuvable : {INPUT_FILE}")
        return
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier Excel : {e}")
        return

    required_cols = ['Ancien_Contrat', 'Nouveau_Contrat']
    if not all(col in df_input.columns for col in required_cols):
        logger.error(f"Le fichier Excel doit contenir les colonnes : {required_cols}")
        return

    # Stockage des résultats
    report_data = []
    stats_list = []

    # ---------------------------------------------------------
    # 3. Boucle principale sur chaque ligne du fichier Excel
    # ---------------------------------------------------------
    for index, row in df_input.iterrows():
        ref_contract = str(row['Ancien_Contrat']).strip().replace('.0', '')
        new_contract = str(row['Nouveau_Contrat']).strip().replace('.0', '')

        # --- CORRECTION 1 : Ignorer les contrats dont l'activation a échoué ---
        if 'Statut' in row:
            status_activation = str(row['Statut']).strip()
            if not status_activation.upper().startswith('OK'):
                logger.warning(f"Skip [{index+1}] {ref_contract} : Activation précédente en échec ({status_activation}).")
                stats_list.append({'Product': 'UNKNOWN', 'Contract': ref_contract, 'Status': 'SKIP_ACTIVATION_KO'})
                continue

        if not ref_contract or ref_contract == 'nan' or not new_contract or new_contract == 'nan':
            continue

        logger.info(f"Traitement [{index+1}/{len(df_input)}] : Ref {ref_contract} (J0) vs New {new_contract} (Live)")

        # A. Récupération des IDs internes (NO_CNT)
        try:
            # On a besoin des IDs pour les requêtes LIVE (Cible) et Fallback (Source)
            q_id_ref = QUERIES["GET_INTERNAL_ID"].format(contract_number=ref_contract)
            df_id_ref = db.get_data(q_id_ref)

            q_id_new = QUERIES["GET_INTERNAL_ID"].format(contract_number=new_contract)
            df_id_new = db.get_data(q_id_new)

            if df_id_ref.empty or df_id_new.empty:
                logger.warning(f"  -> ID interne introuvable pour l'un des contrats. Skip.")
                stats_list.append({'Product': 'UNKNOWN', 'Contract': ref_contract, 'Status': 'ERROR_ID_LISA'})
                continue

            id_ref = df_id_ref.iloc[0]['NO_CNT']
            id_new = df_id_new.iloc[0]['NO_CNT']

            # Récupération Code Produit pour les stats
            try:
                if hasattr(db, 'get_product_code'):
                    product_code = db.get_product_code(id_ref)
                else:
                    q_prod = f"SELECT TOP 1 C_PROP_PRINC FROM LV.SCNTT0 WHERE NO_CNT = {id_ref}"
                    df_prod = db.get_data(q_prod)
                    product_code = str(df_prod.iloc[0]['C_PROP_PRINC']).strip() if not df_prod.empty else "UNKNOWN"

                if not product_code: product_code = "UNKNOWN"
            except:
                product_code = "ERROR_PROD"

        except Exception as e:
            logger.error(f"  -> Erreur technique récupération ID: {e}")
            stats_list.append({'Product': 'UNKNOWN', 'Contract': ref_contract, 'Status': 'CRASH_ID'})
            continue

        # B. Comparaison Table par Table
        tables_to_check = [
            "LV.SCNTT0", "LV.SAVTT0", "LV.PRCTT0",
            "LV.SWBGT0", "LV.SCLST0", "LV.SCLRT0",
            "LV.BSPDT0", "LV.BSPGT0"
        ]

        contract_global_status = "OK"

        for table in tables_to_check:
            if table not in QUERIES: continue

            # --- 1. CHARGEMENT SOURCE (REFERENCE) ---
            # Stratégie : Priorité au Snapshot (.pkl) > Fallback SQL Live
            df_ref_data = pd.DataFrame()
            snapshot_path = os.path.join(snapshot_dir, f"{ref_contract}_{table}.pkl")
            is_snapshot = False

            if os.path.exists(snapshot_path):
                try:
                    df_ref_data = pd.read_pickle(snapshot_path)
                    is_snapshot = True
                except Exception as e:
                    logger.warning(f"   [!] Erreur lecture snapshot {snapshot_path}: {e}")

            # Si pas de snapshot, on construit la requête Live pour la source (Fallback)
            if not is_snapshot:
                logger.info(f"   [Info] Pas de snapshot pour {table}. Utilisation SQL Live (Attention aux écarts temporels).")
                query_template = QUERIES[table]
                if "{internal_id}" in query_template:
                    q_ref = query_template.format(internal_id=id_ref)
                elif "{contract_number}" in query_template:
                    q_ref = query_template.format(contract_number=ref_contract)
                else:
                    q_ref = None

                if q_ref:
                    df_ref_data = db.get_data(q_ref)

            # --- 2. CHARGEMENT CIBLE (NEW) ---
            # Toujours en Live DB
            query_template = QUERIES[table]
            if "{internal_id}" in query_template:
                q_new = query_template.format(internal_id=id_new)
            elif "{contract_number}" in query_template:
                q_new = query_template.format(contract_number=new_contract)
            else:
                continue # Skip si pas de params matchant

            df_new_data = db.get_data(q_new)

            # --- 3. COMPARAISON ---
            try:
                status, diff_details = compare_dataframes(df_ref_data, df_new_data, table)
                details_str = ""

                # --- GESTION DU DÉTAIL DES ERREURS ---
                if status == "KO" or str(status).startswith("KO_"):
                    contract_global_status = "KO"
                    if hasattr(diff_details, 'to_string'):
                        details_str = diff_details.to_string(na_rep='-', max_rows=None, max_cols=None)
                    else:
                        details_str = str(diff_details)

                    logger.error(f"❌ ÉCHEC SUR CONTRAT {ref_contract} (Table: {table})")
                    logger.error(f"DIFFÉRENCES (Aperçu) :\n{str(details_str)[:500]}...\n{'-'*50}")

                # Ajout au rapport
                report_data.append({
                    'Reference_Contract': ref_contract,
                    'New_Contract': new_contract,
                    'Product': product_code,
                    'Table': table,
                    'Status': status,
                    'Source_Type': 'SNAPSHOT' if is_snapshot else 'LIVE_DB', # Info utile pour le debug
                    'Details': details_str
                })

            except Exception as e:
                logger.error(f"  -> Crash sur {table}: {e}")
                contract_global_status = "KO"
                report_data.append({
                    'Reference_Contract': ref_contract, 'New_Contract': new_contract,
                    'Product': product_code, 'Table': table,
                    'Status': 'CRITICAL_ERROR', 'Details': str(e)
                })

        # Statut global du contrat
        stats_list.append({
            'Product': product_code,
            'Contract': ref_contract,
            'Status': contract_global_status
        })

    # ---------------------------------------------------------
    # 4. Génération des rapports (CSV)
    # ---------------------------------------------------------
    if report_data or stats_list:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # --- A. Rapport Détaillé ---
        if report_data:
            df_report = pd.DataFrame(report_data)
            output_filename = f'rapport_detaille_{timestamp}.csv'
            output_path = os.path.join(OUTPUT_DIR, output_filename)
            df_report.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
            logger.info(f"Rapport détaillé généré : {output_path}")

        # --- B. Synthèse par Produit ---
        if stats_list:
            df_stats = pd.DataFrame(stats_list)
            summary = df_stats.groupby(['Product', 'Status']).size().unstack(fill_value=0)

            for col in ['OK', 'KO']:
                if col not in summary.columns: summary[col] = 0

            summary['Total'] = summary.sum(axis=1)
            # Taux de succès
            if 'Total' in summary.columns and (summary['Total'] > 0).any():
                summary['Success_Rate (%)'] = (summary['OK'] / summary['Total'] * 100).round(1)
            else:
                summary['Success_Rate (%)'] = 0.0

            print("\n" + "="*60)
            print(" SYNTHÈSE DES RÉSULTATS PAR PRODUIT")
            print("="*60)
            print(summary)
            print("="*60 + "\n")

            summary_path = os.path.join(OUTPUT_DIR, f'synthese_par_produit_{timestamp}.csv')
            summary.to_csv(summary_path, sep=';')
            logger.info(f"Synthèse générée : {summary_path}")

        logger.info("--- Fin du traitement ---")
    else:
        logger.warning("Aucune donnée traitée ou fichier vide.")

if __name__ == "__main__":
    main()