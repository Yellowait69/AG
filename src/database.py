import pandas as pd
import urllib.parse
import logging
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