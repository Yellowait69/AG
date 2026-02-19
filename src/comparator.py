import pandas as pd
import numpy as np
from config.exclusions import IGNORE_COLUMNS, SPECIFIC_EXCLUSIONS

def compare_dataframes(df_ref, df_new, table_name):
    """
    Fonction centrale de comparaison entre deux jeux de données (DataFrames).

    Objectif :
    Comparer le contrat source (référence) et le contrat cible (nouveau) pour une table donnée,
    en ignorant les champs techniques (dates de création, ID générés, etc.) et en s'assurant
    que les données métiers sont strictement identiques.

    Args:
        df_ref (pd.DataFrame): Les données extraites du contrat source (généralement depuis le snapshot).
        df_new (pd.DataFrame): Les données extraites du nouveau contrat nouvellement activé.
        table_name (str): Le nom de la table analysée (ex: 'LV.SCNTT0'), utilisé pour les règles d'exclusion.

    Returns:
        tuple: (Statut de la comparaison (str), Détails des différences (pd.DataFrame ou str))
    """

    # ÉTAPE 1 : Contrôles de validité initiaux
    # On vérifie d'abord si les jeux de données sont vides pour éviter des traitements inutiles et des plantages.
    if df_ref.empty and df_new.empty:
        return "OK_EMPTY", None

    if df_ref.empty or df_new.empty:
        return "KO_MISSING_DATA", f"L'un des deux DataFrames est vide pour la table {table_name}."

    # ÉTAPE 2 : Isolation des données
    # On travaille systématiquement sur des copies pour éviter que nos transformations
    # (arrondis, suppressions de colonnes) n'altèrent les DataFrames originaux passés en paramètre.
    df1 = df_ref.copy()
    df2 = df_new.copy()

    # ÉTAPE 3 : Application des règles d'exclusion
    # Certaines colonnes sont purement techniques (clés primaires, timestamps de mise à jour, auteurs)
    # et seront TOUJOURS différentes d'un contrat à l'autre. On doit les exclure avant la comparaison.
    cols_to_drop = list(IGNORE_COLUMNS)

    if table_name in SPECIFIC_EXCLUSIONS:
        cols_to_drop.extend(SPECIFIC_EXCLUSIONS[table_name])

    # On s'assure de ne tenter de supprimer que les colonnes qui existent réellement dans le dataset
    existing_cols_to_drop = [col for col in cols_to_drop if col in df1.columns]

    df1 = df1.drop(columns=existing_cols_to_drop, errors='ignore')
    df2 = df2.drop(columns=existing_cols_to_drop, errors='ignore')

    # ÉTAPE 4 : Alignement des schémas de données
    # On détermine l'intersection exacte des colonnes entre les deux DataFrames.
    # Cela permet d'éviter les erreurs si une nouvelle colonne a été ajoutée dans l'environnement cible
    # entre le moment de la création du snapshot (source) et le moment de la comparaison.
    common_cols = sorted(list(df1.columns.intersection(df2.columns)))

    if not common_cols:
        return "KO_NO_COMMON_COLS", "Aucune colonne commune trouvée après l'application des filtres d'exclusion."

    df1 = df1[common_cols]
    df2 = df2[common_cols]

    # ÉTAPE 5 : Normalisation et formatage des données
    # Les systèmes peuvent renvoyer des données équivalentes sous des formats légèrement différents.
    # Il faut nettoyer ces données pour éviter de lever des erreurs sur des détails non métiers.
    for col in common_cols:

        # Traitement des chaînes de caractères (Varchar/String)
        # On supprime les espaces superflus (strip) et on uniformise les représentations des valeurs nulles.
        if df1[col].dtype == object:
            df1[col] = df1[col].astype(str).str.strip().replace({'nan': np.nan, 'None': np.nan})
            df2[col] = df2[col].astype(str).str.strip().replace({'nan': np.nan, 'None': np.nan})

        # Traitement des valeurs numériques (Float)
        # On arrondit à 4 décimales pour éviter les faux positifs liés à l'imprécision des bases de données
        # sur les nombres à virgule flottante (ex: 12.00000001 n'est pas vu comme égal à 12.00000000 sans arrondi).
        elif pd.api.types.is_float_dtype(df1[col]):
            df1[col] = df1[col].round(4)
            df2[col] = df2[col].round(4)

    # ÉTAPE 6 : Alignement des enregistrements (Tri)
    # Pour que la comparaison croisée fonctionne, l'ordre des lignes doit être parfaitement identique.
    # On trie l'intégralité du dataset en se basant sur toutes les colonnes restantes.
    try:
        df1 = df1.sort_values(by=common_cols).reset_index(drop=True)
        df2 = df2.sort_values(by=common_cols).reset_index(drop=True)
    except Exception as e:
        print(f"Attention: Le tri technique a échoué sur la table {table_name}. Raison : {e}")

    # ÉTAPE 7 : Comparaison finale et génération du rapport d'écarts
    # La méthode equals() vérifie si les valeurs sont strictement identiques après le nettoyage.
    if df1.equals(df2):
        return "OK", None

    # S'il y a des différences, on tente de générer un rapport détaillé des écarts.
    try:
        # La fonction compare() de pandas extrait uniquement les cellules présentant des différences.
        # align_axis=0 permet d'empiler les lignes (Source puis Cible) pour une lecture plus aisée dans les exports Excel/CSV.
        diff = df1.compare(df2, align_axis=0, keep_shape=False, keep_equal=False)

        # On renomme l'index technique ('self' et 'other') généré par pandas par des termes clairs.
        diff.index = diff.index.set_levels(['Source', 'Cible'], level=1)

        return "KO", diff

    except ValueError:
        # L'exception ValueError est levée par pandas si les deux DataFrames n'ont pas le même nombre de lignes.
        # Dans ce cas, une comparaison cellule par cellule est impossible.
        return "KO_ROW_COUNT", f"Écart sur le volume de données : Source = {len(df1)} lignes vs Cible = {len(df2)} lignes."

    except Exception as e:
        # Catch global pour s'assurer que le script global ne crashe pas si une table a des données corrompues.
        return "KO_ERROR", f"Erreur technique lors de la génération du différentiel : {str(e)}"