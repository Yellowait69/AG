import os
import pandas as pd
import logging
from src.database import DatabaseManager
from sql.queries import QUERIES
from config.settings import OUTPUT_DIR

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("--- Démarrage du Test d'Extraction LISA ---")

    # Le numéro de contrat cible fourni
    TARGET_CONTRACT = "182-2728195-31"

    # 1. Vérification du dossier de sortie
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # 2. Initialisation de la base de données
    try:
        db = DatabaseManager()
        if not db.test_connection():
            logger.error("Impossible de se connecter à la base de données. Arrêt.")
            return
    except Exception as e:
        logger.error(f"Erreur d'initialisation DB: {e}")
        return

    # 3. Récupération de l'ID interne (NO_CNT)
    logger.info(f"Recherche de l'ID interne pour le contrat externe : {TARGET_CONTRACT}")

    q_id = QUERIES["GET_INTERNAL_ID"].format(contract_number=TARGET_CONTRACT)
    df_id = db.get_data(q_id)

    # Petite sécurité : parfois les numéros sont stockés sans tirets en base
    if df_id.empty:
        alt_contract = TARGET_CONTRACT.replace("-", "")
        logger.warning(f"Contrat introuvable avec tirets. Essai sans tirets : {alt_contract}")
        q_id = QUERIES["GET_INTERNAL_ID"].format(contract_number=alt_contract)
        df_id = db.get_data(q_id)

        if df_id.empty:
            logger.error(f"Le contrat {TARGET_CONTRACT} est totalement introuvable dans LV.SCNTT0.")
            return
        else:
            TARGET_CONTRACT = alt_contract # On garde le format qui a fonctionné

    internal_id = df_id.iloc[0]['NO_CNT']
    logger.info(f"✅ Contrat trouvé ! ID Interne (NO_CNT) = {internal_id}")

    # 4. Liste des tables à extraire (basée sur vos requêtes)
    tables_to_extract = [
        "LV.SCNTT0", "LV.SAVTT0", "LV.PRCTT0",
        "LV.SWBGT0", "LV.SCLST0", "LV.SCLRT0",
        "LV.BSPDT0", "LV.BSPGT0"
    ]

    # Fichier de sortie Excel
    output_filename = f"extraction_brute_{TARGET_CONTRACT}.xlsx"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    logger.info("Début de l'extraction des données table par table...")

    # 5. Extraction et écriture dans le fichier Excel (un onglet par table)
    try:
        # On utilise pd.ExcelWriter pour écrire plusieurs onglets dans le même fichier
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for table in tables_to_extract:
                if table not in QUERIES:
                    continue

                # Formatage de la requête avec l'ID interne
                query = QUERIES[table].format(internal_id=internal_id)
                df_table = db.get_data(query)

                # Nom de l'onglet (On enlève 'LV.' pour que ce soit plus propre, ex: 'SCNTT0')
                sheet_name = table.replace("LV.", "")

                if not df_table.empty:
                    df_table.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(f"  -> {table} : {len(df_table)} lignes extraites.")
                else:
                    # Si la table est vide, on crée quand même l'onglet avec un message
                    pd.DataFrame({"Info": ["Aucune donnée trouvée"]}).to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.warning(f"  -> {table} : Vide (0 ligne).")

        logger.info(f"🎉 Extraction terminée avec succès !")
        logger.info(f"📁 Fichier généré : {output_path}")
        logger.info("Ouvrez ce fichier Excel pour vérifier visuellement si l'extraction SQL fonctionne bien.")

    except Exception as e:
        logger.error(f"Erreur lors de la génération du fichier Excel : {e}")

if __name__ == "__main__":
    main()