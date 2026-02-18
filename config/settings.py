import os

# Configuration Base de Données
DB_CONFIG = {
    'DRIVER': 'SQL Server',
    'SERVER': 'SQLMFDBD01',
    'DATABASE': 'FJ0AGDB_D000',
    'TRUSTED_CONNECTION': 'yes',
    'UID': 'XA3894',
    'PWD': '*****************'
}

# Chemins des fichiers
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(BASE_DIR, 'data', 'input', 'contrats_a_tester.xlsx')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'output')