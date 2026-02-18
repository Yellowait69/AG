import pandas as pd
import os
import time
import logging
from datetime import datetime
from src.database import DatabaseManager
from sql.queries import QUERIES

# --- CONFIGURATION ---
INPUT_FILE_SOURCES = 'data/input/contrats_sources.xlsx' # Fichier contenant les ID sources si dispo
OUTPUT_FILE_MAPPING = 'data/input/contrats_en_attente_activation.xlsx'
DEFAULT_PREMIUM_AMOUNT = 100.00  # Montant par défaut si introuvable

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_source_premium_amount(db, internal_id_source):
    """
    Tente de récupérer le montant du premier paiement du contrat source
    pour le répliquer à l'identique.
    """
    try:
        query = f"SELECT TOP 1 M_PAY FROM LV.PRCTT0 WHERE NO_CNT = {internal_id_source} ORDER BY D_REF_PRM ASC"
        df = db.get_data(query)
        if not df.empty and 'M_PAY' in df.columns:
            return float(df.iloc[0]['M_PAY'])
    except Exception as e:
        logger.warning(f"Impossible de récupérer la prime source : {e}")

    return DEFAULT_PREMIUM_AMOUNT

def duplicate_contract_in_elia(source_contract_ext, db_manager):
    """
    SIMULATION DE LA DUPLICATION ELIA/ETL.

    C'est ici que tu dois implémenter la logique d'injection dans les tables ELIA.
    Comme décrit dans le doc: 'Création des records dans les différentes tables ELIA'.

    Args:
        source_contract_ext (str): Numéro de contrat source (ex: '123456')
        db_manager: Instance du manager (si besoin d'une autre connexion DB, l'ajouter ici)

    Returns:
        str: Le numéro du NOUVEAU contrat créé (ex: '999456')
    """

    # -------------------------------------------------------------------------
    # TODO: IMPLÉMENTER LA LOGIQUE RÉELLE ICI
    # 1. Lire les données du contrat source (via SQL ELIA ou fichier)
    # 2. Générer un nouveau numéro de contrat unique
    # 3. INSERT INTO ELIA_TABLE_X ...
    # 4. INSERT INTO ELIA_TABLE_Y ...
    # -------------------------------------------------------------------------

    # --- POUR L'INSTANT : SIMULATION ---
    # On simule un nouveau numéro en remplaçant les premiers chiffres
    # Ex: Si source '7891234' -> Nouveau '9991234'
    new_contract_ext = "999" + str(source_contract_ext)[-6:]

    logger.info(f"   [Simulation] Duplication ELIA : Source {source_contract_ext} -> Cible {new_contract_ext}")

    # Simulation d'un petit temps de traitement ETL
    time.sleep(0.5)

    return new_contract_ext

def get_internal_id_with_retry(db, contract_ext, max_retries=3):
    """
    Récupère le NO_CNT (Interne) à partir du NO_CNT_EXTENDED (Externe).
    Avec tentative de réessai si le contrat vient d'être créé et n'est pas encore visible.
    """
    for i in range(max_retries):
        try:
            q = QUERIES["GET_INTERNAL_ID"].format(contract_number=contract_ext)
            df = db.get_data(q)

            if not df.empty:
                return df.iloc[0]['NO_CNT']

            logger.info(f"   ... ID interne introuvable (Essai {i+1}/{max_retries}). Attente...")
            time.sleep(2) # Attendre 2 secondes avant de réessayer
        except Exception:
            pass

    return None

def main():
    logger.info("--- Démarrage du Script d'Activation (Duplication & Paiement) ---")

    # 1. Initialisation
    db = DatabaseManager()
    if not db.test_connection():
        return

    # 2. Liste des contrats sources
    # Option A: Depuis fichier
    if os.path.exists(INPUT_FILE_SOURCES):
        df_src = pd.read_excel(INPUT_FILE_SOURCES)
        contrats_sources = df_src['Contrat_Source'].astype(str).tolist()
    else:
        # Option B: Hardcodé pour test
        logger.warning(f"Fichier {INPUT_FILE_SOURCES} non trouvé. Utilisation liste par défaut.")
        contrats_sources = ['12345678', '87654321']

    mapping_resultats = []

    # 3. Boucle de traitement
    for old_contract in contrats_sources:
        old_contract = str(old_contract).strip()
        logger.info(f"--- Traitement Source : {old_contract} ---")

        # --- ÉTAPE A : DUPLICATION (ELIA) ---
        try:
            new_contract_ext = duplicate_contract_in_elia(old_contract, db)
        except Exception as e:
            logger.error(f"   [!] Erreur lors de la duplication : {e}")
            mapping_resultats.append({
                'Ancien_Contrat': old_contract, 'Statut': 'KO_DUPLICATION', 'Error': str(e)
            })
            continue

        # --- ÉTAPE B : RÉCUPÉRATION DONNÉES TECHNIQUES ---
        # B1. Récup ID Interne Source (pour trouver le montant de la prime)
        id_int_source = get_internal_id_with_retry(db, old_contract, max_retries=1)

        # B2. Détermination du montant à payer
        if id_int_source:
            montant_prime = get_source_premium_amount(db, id_int_source)
        else:
            montant_prime = DEFAULT_PREMIUM_AMOUNT
            logger.warning("   Impossible de trouver ID source, utilisation montant par défaut.")

        # B3. Récup ID Interne NOUVEAU (Crucial pour injecter le paiement)
        # Note : Si la duplication ELIA n'a pas encore atteint la table LV.SCNTT0 (LISA),
        # cela échouera ici. Dans ce cas, on ne peut PAS faire le paiement auto.
        id_int_new = get_internal_id_with_retry(db, new_contract_ext, max_retries=5)

        if not id_int_new:
            logger.error(f"   [!] Nouveau contrat {new_contract_ext} introuvable dans LISA (LV.SCNTT0).")
            logger.error("       -> Impossible d'injecter le paiement. Vérifier la synchro ELIA->LISA.")
            mapping_resultats.append({
                'Ancien_Contrat': old_contract,
                'Nouveau_Contrat': new_contract_ext,
                'Statut': 'KO_NOT_FOUND_IN_LISA'
            })
            continue

        # --- ÉTAPE C : PAIEMENT (LISA) ---
        logger.info(f"   -> Injection paiement de {montant_prime}€ sur contrat {id_int_new}...")
        payment_success = db.inject_payment(contract_internal_id=id_int_new, amount=montant_prime)

        status = 'OK_PAID' if payment_success else 'KO_PAYMENT'

        # --- ÉTAPE D : STOCKAGE RÉSULTAT ---
        mapping_resultats.append({
            'Ancien_Contrat': old_contract,
            'Nouveau_Contrat': new_contract_ext,
            'ID_Interne_New': id_int_new,
            'Montant_Paye': montant_prime,
            'Date_Injection': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'Statut': status
        })

    # 4. Sauvegarde du fichier pour le Comparateur (Script B)
    if mapping_resultats:
        os.makedirs(os.path.dirname(OUTPUT_FILE_MAPPING), exist_ok=True)
        df_mapping = pd.DataFrame(mapping_resultats)

        # On sauvegarde
        df_mapping.to_excel(OUTPUT_FILE_MAPPING, index=False)
        logger.info(f"--- Terminé. Fichier de suivi généré : {OUTPUT_FILE_MAPPING} ---")
        logger.info("NB: Ce fichier servira d'entrée au script 'main.py' (comparateur) une fois les batchs passés.")
    else:
        logger.warning("Aucun résultat généré.")

if __name__ == "__main__":
    main()