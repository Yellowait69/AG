import os

# -----------------------------------------------------------------------------
# 1. CONFIGURATION DES CHEMINS (FILESYSTEM)
# -----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Dossiers d'entrée/sortie
INPUT_DIR = os.path.join(BASE_DIR, 'data', 'input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'output')

# --- DEFINITION DES FICHIERS CLES ---

# A. Fichier source (Optionnel : liste des ID à dupliquer pour le script d'activation)
SOURCE_FILE = os.path.join(INPUT_DIR, 'contrats_sources.xlsx')

# B. Fichier pivot (Sortie de l'Activation -> Entrée du Comparateur)
# CORRECTION MAJEURE : On pointe vers le fichier généré par run_activation.py
ACTIVATION_OUTPUT_FILE = os.path.join(INPUT_DIR, 'contrats_en_attente_activation.xlsx')

# C. Variable utilisée par run_comparison.py (doit pointer sur le fichier pivot)
INPUT_FILE = ACTIVATION_OUTPUT_FILE

# -----------------------------------------------------------------------------
# 2. CONFIGURATION BASES DE DONNÉES
# -----------------------------------------------------------------------------

# A. Configuration LISA (SQL Server) - Utilisée par src/database.py par défaut
DB_CONFIG = {
    'DRIVER': 'SQL Server',        # Ou 'ODBC Driver 17 for SQL Server' si erreur
    'SERVER': 'SQLMFDBD01',
    'DATABASE': 'FJ0AGDB_D000',
    'TRUSTED_CONNECTION': 'yes',   # 'yes' utilise l'auth Windows, sinon utilise UID/PWD
    'UID': 'XA3894',
    'PWD': os.getenv('DB_PWD', '*****************') # Bonne pratique : lire depuis var d'env
}

# B. Configuration ELIA (Pour l'injection/duplication - À ADAPTER)
DB_CONFIG_ELIA = {
    'DRIVER': 'Oracle in OraClient19Home1', # Exemple courant pour ELIA
    'SERVER': 'ELIA_PROD_DB',
    'DATABASE': 'ELIA_SCHEMA',
    'UID': 'USER_ELIA',
    'PWD': 'PASSWORD_ELIA'
}