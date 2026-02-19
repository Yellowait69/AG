import pandas as pd
import os
import logging
from src.database import DatabaseManager
from src.comparator import compare_dataframes
from sql.queries import QUERIES
from config.settings import INPUT_FILE, OUTPUT_DIR

# Configuration du logger pour le suivi de l'exécution
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    Script principal de comparaison (Phase 2 du processus Auto-Activator).

    Objectif :
    Comparer les données d'un contrat cible (nouvellement activé via batch) avec
    les données de son contrat source (figées lors de la phase d'activation).
    Ce script est conçu pour tourner de manière asynchrone (ex: J+7 après l'activation).
    """
    logger.info("--- Démarrage du Comparateur Auto-Activator (Mode Snapshot) ---")

    # ÉTAPE 1 : Préparation de l'environnement physique
    # Création du dossier de sortie s'il n'existe pas, et ciblage du dossier contenant les sauvegardes (snapshots)
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    snapshot_dir = os.path.join(OUTPUT_DIR, 'snapshots')

    # ÉTAPE 2 : Initialisation des connexions
    try:
        db = DatabaseManager()
        if not db.test_connection():
            logger.error("Impossible de se connecter à la base de données LISA. Arrêt du processus.")
            return
    except Exception as e:
        logger.error(f"Erreur critique lors de l'initialisation de la DB : {e}")
        return

    # ÉTAPE 3 : Chargement du fichier de mapping (Généré par run_activation.py)
    # Ce fichier contient la liste des contrats source/cible et le statut de leur injection.
    try:
        logger.info(f"Lecture du fichier d'entrée (Mapping J0) : {INPUT_FILE}")
        df_input = pd.read_excel(INPUT_FILE)
        df_input.columns = df_input.columns.str.strip()
    except FileNotFoundError:
        logger.error(f"Fichier d'entrée introuvable : {INPUT_FILE}. Avez-vous exécuté le script d'activation ?")
        return
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier Excel : {e}")
        return

    # Contrôle d'intégrité du fichier d'entrée
    required_cols = ['Ancien_Contrat', 'Nouveau_Contrat']
    if not all(col in df_input.columns for col in required_cols):
        logger.error(f"Structure invalide. Le fichier Excel doit contenir au minimum les colonnes : {required_cols}")
        return

    # Initialisation des structures de stockage pour le reporting
    report_data = [] # Détail des erreurs par table
    stats_list = []  # Statut global par contrat pour la synthèse

    # ÉTAPE 4 : Boucle d'analyse des contrats
    for index, row in df_input.iterrows():
        ref_contract = str(row['Ancien_Contrat']).strip().replace('.0', '')
        new_contract = str(row['Nouveau_Contrat']).strip().replace('.0', '')

        # Filtre métier : Exclusion des échecs d'activation
        # Inutile de comparer un contrat cible si l'étape d'injection ou de duplication (J0) a échoué.
        if 'Statut' in row:
            status_activation = str(row['Statut']).strip()
            if not status_activation.upper().startswith('OK'):
                logger.warning(f"Skip [{index+1}] {ref_contract} : Contrat ignoré car l'activation (J0) était en échec ({status_activation}).")
                stats_list.append({'Product': 'UNKNOWN', 'Contract': ref_contract, 'Status': 'SKIP_ACTIVATION_KO'})
                continue

        # Sécurité contre les lignes vides dans Excel
        if not ref_contract or ref_contract == 'nan' or not new_contract or new_contract == 'nan':
            continue

        logger.info(f"Traitement [{index+1}/{len(df_input)}] : Réf {ref_contract} (Snapshot J0) vs Nouveau {new_contract} (Live LISA)")

        # ÉTAPE 4.1 : Traduction des ID (Externe -> Interne)
        # LISA utilise un identifiant interne (NO_CNT) différent du numéro de police (NO_CNT_EXTENDED).
        try:
            q_id_ref = QUERIES["GET_INTERNAL_ID"].format(contract_number=ref_contract)
            df_id_ref = db.get_data(q_id_ref)

            q_id_new = QUERIES["GET_INTERNAL_ID"].format(contract_number=new_contract)
            df_id_new = db.get_data(q_id_new)

            if df_id_ref.empty or df_id_new.empty:
                logger.warning(f"  -> ID interne (NO_CNT) introuvable pour l'un des contrats. Contrat ignoré.")
                stats_list.append({'Product': 'UNKNOWN', 'Contract': ref_contract, 'Status': 'ERROR_ID_LISA'})
                continue

            id_ref = df_id_ref.iloc[0]['NO_CNT']
            id_new = df_id_new.iloc[0]['NO_CNT']

            # Récupération du code produit (C_PROP_PRINC) pour pouvoir grouper les statistiques par produit à la fin.
            try:
                if hasattr(db, 'get_product_code'):
                    product_code = db.get_product_code(id_ref)
                else:
                    q_prod = f"SELECT TOP 1 C_PROP_PRINC FROM LV.SCNTT0 WHERE NO_CNT = {id_ref}"
                    df_prod = db.get_data(q_prod)
                    product_code = str(df_prod.iloc[0]['C_PROP_PRINC']).strip() if not df_prod.empty else "UNKNOWN"
            except:
                product_code = "ERROR_PROD"

        except Exception as e:
            logger.error(f"  -> Erreur technique lors de la récupération des identifiants : {e}")
            stats_list.append({'Product': 'UNKNOWN', 'Contract': ref_contract, 'Status': 'CRASH_ID'})
            continue

        # ÉTAPE 4.2 : Analyse comparative table par table
        # Liste exhaustive des tables définies dans le périmètre du test C01
        tables_to_check = [
            "LV.SCNTT0", "LV.SAVTT0", "LV.PRCTT0",
            "LV.SWBGT0", "LV.SCLST0", "LV.SCLRT0",
            "LV.BSPDT0", "LV.BSPGT0"
        ]

        contract_global_status = "OK"

        for table in tables_to_check:
            if table not in QUERIES:
                continue

            # --- A. CHARGEMENT DES DONNÉES SOURCES (RÉFÉRENCE) ---
            # Méthode prioritaire : Chargement depuis le fichier Pickle (.pkl).
            # Cela garantit que l'on compare avec l'état exact du contrat au moment de son clonage (J0),
            # évitant ainsi les faux positifs si le contrat source a été modifié entre temps.
            df_ref_data = pd.DataFrame()
            snapshot_path = os.path.join(snapshot_dir, f"{ref_contract}_{table}.pkl")
            is_snapshot = False

            if os.path.exists(snapshot_path):
                try:
                    df_ref_data = pd.read_pickle(snapshot_path)
                    is_snapshot = True
                except Exception as e:
                    logger.warning(f"   [!] Erreur de lecture du snapshot {snapshot_path} : {e}")

            # Mode dégradé (Fallback) : Si le snapshot est absent, on interroge la base de données en direct.
            # Attention : Risque d'écarts temporels.
            if not is_snapshot:
                logger.info(f"   [Info] Snapshot introuvable pour {table}. Interrogation Live de la base source (Mode dégradé).")
                query_template = QUERIES[table]
                if "{internal_id}" in query_template:
                    q_ref = query_template.format(internal_id=id_ref)
                elif "{contract_number}" in query_template:
                    q_ref = query_template.format(contract_number=ref_contract)
                else:
                    q_ref = None

                if q_ref:
                    df_ref_data = db.get_data(q_ref)

            # --- B. CHARGEMENT DES DONNÉES CIBLES (NOUVEAU CONTRAT) ---
            # Le contrat cible est toujours interrogé en live dans la base de données LISA pour vérifier
            # que les batchs de nuit l'ont correctement traité.
            query_template = QUERIES[table]
            if "{internal_id}" in query_template:
                q_new = query_template.format(internal_id=id_new)
            elif "{contract_number}" in query_template:
                q_new = query_template.format(contract_number=new_contract)
            else:
                continue

            df_new_data = db.get_data(q_new)

            # --- C. EXÉCUTION DE LA COMPARAISON ---
            try:
                # Appel au module central de comparaison qui gère le nettoyage et le différentiel
                status, diff_details = compare_dataframes(df_ref_data, df_new_data, table)
                details_str = ""

                # Traitement des anomalies détectées
                if status == "KO" or str(status).startswith("KO_"):
                    contract_global_status = "KO"

                    # Sérialisation du DataFrame de différences en texte brut pour sauvegarde
                    if hasattr(diff_details, 'to_string'):
                        details_str = diff_details.to_string(na_rep='-', max_rows=None, max_cols=None)
                    else:
                        details_str = str(diff_details)

                    # Affichage restreint dans la console pour ne pas saturer les logs
                    logger.error(f" ÉCHEC SUR LE CONTRAT {ref_contract} (Table: {table})")
                    logger.error(f"DIFFÉRENCES (Aperçu) :\n{str(details_str)[:500]}...\n{'-'*50}")

                # Historisation du résultat (pour le rapport détaillé)
                report_data.append({
                    'Reference_Contract': ref_contract,
                    'New_Contract': new_contract,
                    'Product': product_code,
                    'Table': table,
                    'Status': status,
                    'Source_Type': 'SNAPSHOT' if is_snapshot else 'LIVE_DB',
                    'Details': details_str
                })

            except Exception as e:
                logger.error(f"  -> Crash applicatif inattendu sur la table {table} : {e}")
                contract_global_status = "KO"
                report_data.append({
                    'Reference_Contract': ref_contract, 'New_Contract': new_contract,
                    'Product': product_code, 'Table': table,
                    'Status': 'CRITICAL_ERROR', 'Details': str(e)
                })

        # Mise à jour des KPIs globaux pour le contrat
        stats_list.append({
            'Product': product_code,
            'Contract': ref_contract,
            'Status': contract_global_status
        })

    # ÉTAPE 5 : Génération des résultats (Fichiers CSV)
    if report_data or stats_list:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Résultat 1 : Rapport technique détaillé (utile pour l'investigation des bugs par les développeurs)
        if report_data:
            df_report = pd.DataFrame(report_data)
            output_filename = f'rapport_detaille_{timestamp}.csv'
            output_path = os.path.join(OUTPUT_DIR, output_filename)
            # Encodage utf-8-sig pour une ouverture native sans problème d'accents dans MS Excel
            df_report.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
            logger.info(f"Rapport technique détaillé généré avec succès : {output_path}")

        # Résultat 2 : Rapport de synthèse croisé (utile pour le suivi de la Qualité et la validation des versions)
        if stats_list:
            df_stats = pd.DataFrame(stats_list)
            # Agrégation des statuts OK/KO par produit
            summary = df_stats.groupby(['Product', 'Status']).size().unstack(fill_value=0)

            # Normalisation des colonnes pour éviter les KeyError si un statut manque
            for col in ['OK', 'KO']:
                if col not in summary.columns:
                    summary[col] = 0

            # Calcul des indicateurs de performance (Total et Taux de succès)
            summary['Total'] = summary.sum(axis=1)
            if 'Total' in summary.columns and (summary['Total'] > 0).any():
                summary['Success_Rate (%)'] = (summary['OK'] / summary['Total'] * 100).round(1)
            else:
                summary['Success_Rate (%)'] = 0.0

            # Affichage console pour retour immédiat à l'opérateur
            print("\n" + "="*60)
            print(" SYNTHÈSE DES RÉSULTATS PAR PRODUIT (KPIs)")
            print("="*60)
            print(summary)
            print("="*60 + "\n")

            summary_path = os.path.join(OUTPUT_DIR, f'synthese_par_produit_{timestamp}.csv')
            summary.to_csv(summary_path, sep=';')
            logger.info(f"Rapport de synthèse généré avec succès : {summary_path}")

        logger.info("--- Fin de la comparaison. Tous les processus sont terminés. ---")
    else:
        logger.warning("Aucune donnée n'a été traitée (fichier source vide ou ne contenant que des lignes ignorées).")

if __name__ == "__main__":
    main()