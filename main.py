import pandas as pd
import os
import logging
from src.database import DatabaseManager
from src.comparator import compare_dataframes
from sql.queries import QUERIES
from config.settings import INPUT_FILE, OUTPUT_DIR

# Configuration du logging pour voir ce qui se passe dans la console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("--- Démarrage du Comparateur Auto-Activator ---")

    # 0. Vérification de l'environnement
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logger.info(f"Dossier de sortie créé : {OUTPUT_DIR}")

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
    # Colonnes attendues : 'Ancien_Contrat', 'Nouveau_Contrat'
    try:
        df_input = pd.read_excel(INPUT_FILE)
        # Nettoyage des noms de colonnes (supprime les espaces éventuels)
        df_input.columns = df_input.columns.str.strip()
    except FileNotFoundError:
        logger.error(f"Fichier d'entrée introuvable : {INPUT_FILE}")
        return
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier Excel : {e}")
        return

    # Vérification des colonnes requises
    required_cols = ['Ancien_Contrat', 'Nouveau_Contrat']
    if not all(col in df_input.columns for col in required_cols):
        logger.error(f"Le fichier Excel doit contenir les colonnes : {required_cols}")
        return

    report_data = []

    # ---------------------------------------------------------
    # 3. Boucle principale sur chaque ligne du fichier Excel
    # ---------------------------------------------------------
    for index, row in df_input.iterrows():
        # Conversion en string et nettoyage
        ref_contract = str(row['Ancien_Contrat']).strip().replace('.0', '') # Gestion cas 123.0
        new_contract = str(row['Nouveau_Contrat']).strip().replace('.0', '')

        # Si l'une des cellules est vide, on saute
        if not ref_contract or ref_contract == 'nan' or not new_contract or new_contract == 'nan':
            continue

        logger.info(f"Traitement [{index+1}/{len(df_input)}] : Ref {ref_contract} vs New {new_contract}")

        # A. Récupération des IDs internes (NO_CNT)
        try:
            # On cherche l'ID interne pour l'ancien contrat
            q_id_ref = QUERIES["GET_INTERNAL_ID"].format(contract_number=ref_contract)
            df_id_ref = db.get_data(q_id_ref)

            # On cherche l'ID interne pour le nouveau contrat
            q_id_new = QUERIES["GET_INTERNAL_ID"].format(contract_number=new_contract)
            df_id_new = db.get_data(q_id_new)

            if df_id_ref.empty:
                logger.warning(f"  -> ID interne introuvable pour l'ancien contrat {ref_contract}. Skip.")
                report_data.append({'Ref': ref_contract, 'New': new_contract, 'Status': 'ERROR_ID_REF_NOT_FOUND'})
                continue

            if df_id_new.empty:
                logger.warning(f"  -> ID interne introuvable pour le nouveau contrat {new_contract}. Skip.")
                report_data.append({'Ref': ref_contract, 'New': new_contract, 'Status': 'ERROR_ID_NEW_NOT_FOUND'})
                continue

            # Extraction des valeurs scalaires
            id_ref = df_id_ref.iloc[0]['NO_CNT']
            id_new = df_id_new.iloc[0]['NO_CNT']

        except Exception as e:
            logger.error(f"  -> Erreur technique récupération ID: {e}")
            continue

        # B. Comparaison Table par Table
        # Liste explicite pour contrôler l'ordre d'exécution
        tables_to_check = [
            "LV.SCNTT0", # Contrat
            "LV.SAVTT0", # Avenants
            "LV.SWBGT0", # Garanties
            "LV.SCLST0", # Clauses (Header)
            "LV.SCLRT0", # Clauses (Détail)
            "LV.BSPDT0", # Financier (Détail)
            "LV.BSPGT0", # Financier (Global)
            # "FJ1.TB5UCON" # Décommente si tu as cette table externe
        ]

        contract_has_error = False

        for table in tables_to_check:
            if table not in QUERIES:
                logger.warning(f"  -> Table {table} non définie dans queries.py. Skip.")
                continue

            query_template = QUERIES[table]

            try:
                # C. Construction dynamique de la requête
                # On détecte quel paramètre la requête attend ({internal_id} ou {contract_number})
                if "{internal_id}" in query_template:
                    q_ref = query_template.format(internal_id=id_ref)
                    q_new = query_template.format(internal_id=id_new)
                elif "{contract_number}" in query_template:
                    q_ref = query_template.format(contract_number=ref_contract)
                    q_new = query_template.format(contract_number=new_contract)
                else:
                    logger.warning(f"  -> Format de requête inconnu pour {table}.")
                    continue

                # D. Exécution SQL
                df_ref_data = db.get_data(q_ref)
                df_new_data = db.get_data(q_new)

                # E. Comparaison
                status, diff_details = compare_dataframes(df_ref_data, df_new_data, table)

                # F. Enregistrement du résultat
                details_str = ""
                if status == "KO":
                    contract_has_error = True
                    # On convertit le DataFrame des différences en string lisible pour le CSV
                    details_str = diff_details.to_string() if diff_details is not None else "Différences détectées"
                    logger.warning(f"  -> KO sur {table}")
                elif str(status).startswith("KO_"):
                    contract_has_error = True
                    details_str = str(diff_details)
                    logger.warning(f"  -> Erreur technique sur {table}: {status}")

                report_data.append({
                    'Reference_Contract': ref_contract,
                    'New_Contract': new_contract,
                    'Internal_ID_Ref': id_ref,
                    'Internal_ID_New': id_new,
                    'Table': table,
                    'Status': status,
                    'Details': details_str
                })

            except Exception as e:
                logger.error(f"  -> Crash lors du traitement de {table}: {e}")
                report_data.append({
                    'Reference_Contract': ref_contract,
                    'New_Contract': new_contract,
                    'Table': table,
                    'Status': 'CRITICAL_ERROR',
                    'Details': str(e)
                })

    # ---------------------------------------------------------
    # 4. Génération du rapport final
    # ---------------------------------------------------------
    if report_data:
        df_report = pd.DataFrame(report_data)

        # Nom de fichier horodaté pour ne pas écraser les précédents
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f'rapport_comparaison_{timestamp}.csv'
        output_path = os.path.join(OUTPUT_DIR, output_filename)

        # Enregistrement CSV avec séparateur point-virgule (meilleur pour Excel en FR/BE)
        df_report.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')

        logger.info(f"--- Terminé. Rapport généré : {output_path} ---")
    else:
        logger.warning("Aucune donnée n'a été traitée (fichier vide ou erreurs).")

if __name__ == "__main__":
    main()