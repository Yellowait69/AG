import pandas as pd
import numpy as np
from config.exclusions import IGNORE_COLUMNS, SPECIFIC_EXCLUSIONS

def compare_dataframes(df_ref, df_new, table_name):
    """
    Compare deux DataFrames et retourne le statut et les différences.
    Gère le nettoyage, l'alignement, le tri et la normalisation des types.
    """

    # 0. Vérification préliminaire
    if df_ref.empty and df_new.empty:
        return "OK_EMPTY", None # Les deux sont vides, c'est bon
    if df_ref.empty or df_new.empty:
        return "KO_MISSING_DATA", f"Un des deux DataFrames est vide ({table_name})"

    # On travaille sur des copies pour ne pas casser les originaux
    df1 = df_ref.copy()
    df2 = df_new.copy()

    # ---------------------------------------------------------
    # 1. GESTION DES EXCLUSIONS (Globales + Spécifiques)
    # ---------------------------------------------------------
    # On commence par la liste globale
    cols_to_drop = list(IGNORE_COLUMNS)

    # On ajoute les colonnes spécifiques à la table en cours
    if table_name in SPECIFIC_EXCLUSIONS:
        cols_to_drop.extend(SPECIFIC_EXCLUSIONS[table_name])

    # On supprime uniquement les colonnes qui existent vraiment dans le DataFrame
    existing_cols_to_drop = [c for c in cols_to_drop if c in df1.columns]

    df1 = df1.drop(columns=existing_cols_to_drop, errors='ignore')
    df2 = df2.drop(columns=existing_cols_to_drop, errors='ignore')

    # ---------------------------------------------------------
    # 2. ALIGNEMENT DES COLONNES
    # ---------------------------------------------------------
    # On ne garde que les colonnes communes pour éviter de planter si une colonne
    # technique a été ajoutée dans la base cible mais pas source.
    common_cols = sorted(list(df1.columns.intersection(df2.columns)))

    if not common_cols:
        return "KO_NO_COMMON_COLS", "Aucune colonne commune trouvée après exclusions."

    df1 = df1[common_cols]
    df2 = df2[common_cols]

    # ---------------------------------------------------------
    # 3. NORMALISATION (Nettoyage des données)
    # ---------------------------------------------------------

    for col in common_cols:
        # A. Nettoyage des chaines de caractères (Strip)
        if df1[col].dtype == object:
            df1[col] = df1[col].astype(str).str.strip().replace({'nan': np.nan, 'None': np.nan})
            df2[col] = df2[col].astype(str).str.strip().replace({'nan': np.nan, 'None': np.nan})

        # B. Arrondi des nombres flottants (4 décimales par défaut)
        elif pd.api.types.is_float_dtype(df1[col]):
            df1[col] = df1[col].round(4)
            df2[col] = df2[col].round(4)

    # ---------------------------------------------------------
    # 4. TRI DES LIGNES (Crucial pour la comparaison)
    # ---------------------------------------------------------
    # On trie par TOUTES les colonnes.
    try:
        df1 = df1.sort_values(by=common_cols).reset_index(drop=True)
        df2 = df2.sort_values(by=common_cols).reset_index(drop=True)
    except Exception as e:
        # Fallback si le tri échoue (ex: types mixtes non triables)
        print(f"Attention: Tri impossible sur {table_name} - {e}")

    # ---------------------------------------------------------
    # 5. COMPARAISON STRICTE
    # ---------------------------------------------------------
    if df1.equals(df2):
        return "OK", None
    else:
        # Si différence, on génère le rapport
        try:
            # align_axis=0 met les différences l'une sous l'autre
            # keep_shape=False ne montre que les colonnes différentes
            diff = df1.compare(df2, align_axis=0, keep_shape=False, keep_equal=False)

            # On renomme les index pour que ce soit plus lisible dans le rapport
            diff.index = diff.index.set_levels(['Source', 'Cible'], level=1)

            return "KO", diff
        except ValueError as e:
            # Arrive si les dimensions ne sont pas les mêmes (nombre de lignes différent)
            nb_lignes_1 = len(df1)
            nb_lignes_2 = len(df2)
            return "KO_ROW_COUNT", f"Nombre de lignes différent : Source={nb_lignes_1} vs Cible={nb_lignes_2}"
        except Exception as e:
            return "KO_ERROR", str(e)