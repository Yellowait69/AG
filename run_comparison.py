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
    logger.info("--- Démarrage du Comparateur Auto-Activator ---")

    # 0. Vérification de l'environnement
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

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

        if not ref_contract or ref_contract == 'nan' or not new_contract or new_contract == 'nan':
            continue

        logger.info(f"Traitement [{index+1}/{len(df_input)}] : Ref {ref_contract} vs New {new_contract}")

        # A. Récupération des IDs internes (NO_CNT)
        try:
            q_id_ref = QUERIES["GET_INTERNAL_ID"].format(contract_number=ref_contract)
            df_id_ref = db.get_data(q_id_ref)

            q_id_new = QUERIES["GET_INTERNAL_ID"].format(contract_number=new_contract)
            df_id_new = db.get_data(q_id_new)

            if df_id_ref.empty or df_id_new.empty:
                logger.warning(f"  -> ID interne introuvable pour l'un des contrats. Skip.")
                stats_list.append({'Product': 'UNKNOWN', 'Contract': ref_contract, 'Status': 'ERROR_ID'})
                continue

            id_ref = df_id_ref.iloc[0]['NO_CNT']
            id_new = df_id_new.iloc[0]['NO_CNT']

            # Récupération Code Produit
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
            stats_list.append({'Product': 'UNKNOWN', 'Contract': ref_contract, 'Status': 'CRASH'})
            continue

        # B. Comparaison Table par Table
        tables_to_check = [
            "LV.SCNTT0", "LV.SAVTT0", "LV.SWBGT0",
            "LV.SCLST0", "LV.SCLRT0",
            "LV.BSPDT0", "LV.BSPGT0"
        ]

        contract_global_status = "OK"

        for table in tables_to_check:
            if table not in QUERIES: continue

            query_template = QUERIES[table]
            try:
                if "{internal_id}" in query_template:
                    q_ref = query_template.format(internal_id=id_ref)
                    q_new = query_template.format(internal_id=id_new)
                elif "{contract_number}" in query_template:
                    q_ref = query_template.format(contract_number=ref_contract)
                    q_new = query_template.format(contract_number=new_contract)
                else:
                    continue

                df_ref_data = db.get_data(q_ref)
                df_new_data = db.get_data(q_new)

                status, diff_details = compare_dataframes(df_ref_data, df_new_data, table)

                details_str = ""

                # --- GESTION DU DÉTAIL DES ERREURS ---
                if status == "KO" or str(status).startswith("KO_"):
                    contract_global_status = "KO"

                    # Conversion des différences en texte pour le CSV et la Console
                    if hasattr(diff_details, 'to_string'):
                        # On force l'affichage complet pour ne pas avoir de "..."
                        details_str = diff_details.to_string(na_rep='-', max_rows=None, max_cols=None)
                    else:
                        details_str = str(diff_details)

                    # LOG CONSOLE
                    logger.error(f"❌ ÉCHEC SUR CONTRAT {ref_contract} (Table: {table})")
                    logger.error(f"DIFFÉRENCES :\n{details_str}\n{'-'*50}")

                # Ajout au rapport CSV
                report_data.append({
                    'Reference_Contract': ref_contract,
                    'New_Contract': new_contract,
                    'Product': product_code,
                    'Table': table,
                    'Status': status,
                    'Details': details_str  # <--- C'est ici que l'erreur exacte est sauvegardée
                })

            except Exception as e:
                logger.error(f"  -> Crash sur {table}: {e}")
                contract_global_status = "KO"
                report_data.append({
                    'Reference_Contract': ref_contract, 'New_Contract': new_contract,
                    'Product': product_code, 'Table': table,
                    'Status': 'CRITICAL_ERROR', 'Details': str(e)
                })

        stats_list.append({
            'Product': product_code,
            'Contract': ref_contract,
            'Status': contract_global_status
        })

    # ---------------------------------------------------------
    # 4. Génération des rapports (CSV)
    # ---------------------------------------------------------
    if report_data:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # --- A. Rapport Détaillé (avec la colonne Details remplie) ---
        df_report = pd.DataFrame(report_data)
        output_filename = f'rapport_detaille_{timestamp}.csv'
        output_path = os.path.join(OUTPUT_DIR, output_filename)

        # Encodage utf-8-sig pour que Excel lise bien les accents
        # Séparateur point-virgule pour Excel
        df_report.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')

        # --- B. Synthèse par Produit ---
        if stats_list:
            df_stats = pd.DataFrame(stats_list)
            summary = df_stats.groupby(['Product', 'Status']).size().unstack(fill_value=0)

            if 'OK' not in summary.columns: summary['OK'] = 0
            if 'KO' not in summary.columns: summary['KO'] = 0

            summary['Total'] = summary['OK'] + summary['KO']
            summary['Success_Rate (%)'] = (summary['OK'] / summary['Total'] * 100).round(1)

            print("\n" + "="*60)
            print(" SYNTHÈSE DES RÉSULTATS PAR PRODUIT")
            print("="*60)
            print(summary[['OK', 'KO', 'Total', 'Success_Rate (%)']])
            print("="*60 + "\n")

            summary_path = os.path.join(OUTPUT_DIR, f'synthese_par_produit_{timestamp}.csv')
            summary.to_csv(summary_path, sep=';')

        logger.info(f"--- Terminé. Rapport détaillé généré : {output_path} ---")
    else:
        logger.warning("Aucune donnée traitée.")

if __name__ == "__main__":
    main()