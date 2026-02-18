import pandas as pd
import urllib.parse
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from config.settings import DB_CONFIG

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.engine = self._create_db_engine()

    def _create_db_engine(self):
        try:
            # Construction de la chaîne de connexion ODBC brute
            params = [
                f"DRIVER={{{DB_CONFIG['DRIVER']}}}",
                f"SERVER={DB_CONFIG['SERVER']}",
                f"DATABASE={DB_CONFIG['DATABASE']}"
            ]

            # Gestion de l'authentification
            if DB_CONFIG.get('TRUSTED_CONNECTION', 'no').lower() == 'yes':
                params.append("Trusted_Connection=yes")
            else:
                # Si non 'Trusted', on attend un UID et PWD
                if 'UID' in DB_CONFIG and 'PWD' in DB_CONFIG:
                    params.append(f"UID={DB_CONFIG['UID']}")
                    params.append(f"PWD={DB_CONFIG['PWD']}")
                else:
                    logger.warning("Attention: Pas d'utilisateur/mot de passe ni de connexion approuvée configurés.")

            conn_str = ";".join(params)
            encoded_conn_str = urllib.parse.quote_plus(conn_str)

            engine_url = f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}"

            return create_engine(engine_url, fast_executemany=True)

        except Exception as e:
            logger.error(f"Erreur lors de la création de l'engine: {e}")
            raise

    def get_data(self, query: str) -> pd.DataFrame:
        """
        Exécute une requête SQL SELECT et retourne un DataFrame Pandas.

        Args:
            query (str): La requête SQL à exécuter.

        Returns:
            pd.DataFrame: Les résultats sous forme de DataFrame.
        """
        try:
            # Utilisation d'une connexion explicite avec gestionnaire de contexte
            with self.engine.connect() as connection:
                # Pandas lit directement via la connexion ouverte
                return pd.read_sql(text(query), connection)

        except SQLAlchemyError as e:
            logger.error(f"Erreur SQL lors de l'exécution de la requête : {e}")
            # On retourne un DataFrame vide en cas d'erreur pour ne pas faire planter le script de comparaison
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Erreur inattendue : {e}")
            raise

    def inject_payment(self, contract_internal_id, amount, payment_date=None):
        """
        Insère un paiement dans LV.PRCTT0 pour activer le contrat.
        Se base sur la structure de données fournie (Mode 6).
        """
        # Si pas de date fournie, on prend maintenant
        if payment_date is None:
            now = datetime.now()
            d_ref = now.strftime('%Y-%m-%d')
            tstamp = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            d_ref = payment_date
            # Si payment_date est juste une string 'YYYY-MM-DD', on ajoute l'heure pour le timestamp
            if len(str(payment_date)) == 10:
                tstamp = f"{payment_date} 12:00:00.000000"
            else:
                tstamp = payment_date

        # Génération d'une communication structurée fictive basée sur l'ID contrat
        # Format : 820 + 9 chiffres ID + 99 (juste pour l'unicité)
        fake_commu = f"820{str(contract_internal_id)[:9]}99"

        # Requête INSERT paramétrée
        query = text("""
                     INSERT INTO LV.PRCTT0 (
                         C_STE, NO_CNT, C_MD_PMT, D_REF_PRM, NO_ORD_RCP, TSTAMP_CRT_RCT,
                         C_TY_RCT, D_BISM_DVA, D_BISM_DCOR, M_PAY, NM_CP,
                         T_ADR_1_CP, T_ADR_2_CP, C_ETAT_RCP, T_COMMU, NO_BUR_SERV,
                         NO_AVT, PC_COM, PC_FR_GEST, NO_IBAN_CP, C_BIC_CP,
                         NM_AUTEUR_CRT, D_CRT, TY_DMOD, D_ORGN_DEV, C_ORGN_DEV
                     ) VALUES (
                                  'A', :no_cnt, '6', :d_ref, '1', :tstamp,
                                  '1', :d_ref, :d_ref, :amount, 'TEST AUTOMATION',
                                  'RUE DU TEST 1', '1000 BRUXELLES', 'B', :commu, '12831',
                                  '0', 0.0245, 0.0105, 'BE47001304609580', 'GEBABEBB',
                                  'AUTO_TEST', :d_ref, 'O', :d_ref, 'EUR'
                              )
                     """)

        params = {
            'no_cnt': contract_internal_id,
            'd_ref': d_ref,
            'tstamp': tstamp,
            'amount': amount,
            'commu': fake_commu
        }

        try:
            # .begin() gère la transaction et le commit automatique
            with self.engine.begin() as connection:
                connection.execute(query, params)
                logger.info(f"SUCCÈS: Paiement de {amount} EUR injecté pour le contrat {contract_internal_id} (Date: {d_ref})")
                return True
        except Exception as e:
            logger.error(f"ÉCHEC: Erreur lors de l'injection du paiement pour {contract_internal_id} : {e}")
            return False

    def test_connection(self):
        """Méthode utilitaire pour vérifier si la connexion fonctionne."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT 1")).scalar()
                if result == 1:
                    logger.info(f"Connexion réussie à la base : {DB_CONFIG['DATABASE']}")
                    return True
        except Exception as e:
            logger.error(f"Échec de la connexion : {e}")
            return False