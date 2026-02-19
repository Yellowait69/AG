using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

public static class DataComparer
{
    /// <summary>
    /// Fonction centrale de comparaison entre deux jeux de données (DataTable).
    /// </summary>
    /// <returns>Un tuple contenant le statut, un éventuel message d'erreur, et un DataTable contenant les différences.</returns>
    public static (string Status, string ErrorMessage, DataTable DiffDetails) CompareDataTables(DataTable dtRef, DataTable dtNew, string tableName)
    {
        // ÉTAPE 1 : Contrôles de validité initiaux
        if (dtRef.Rows.Count == 0 && dtNew.Rows.Count == 0)
            return ("OK_EMPTY", null, null);

        if (dtRef.Rows.Count == 0 || dtNew.Rows.Count == 0)
            return ("KO_MISSING_DATA", $"L'un des deux DataTables est vide pour la table {tableName}.", null);

        // ÉTAPE 2 : Isolation des données (Copies)
        DataTable dt1 = dtRef.Copy();
        DataTable dt2 = dtNew.Copy();

        // ÉTAPE 3 : Application des règles d'exclusion
        // (Nécessite la classe ColumnConfig générée précédemment)
        var colsToDrop = new HashSet<string>(ColumnConfig.IgnoreColumns, StringComparer.OrdinalIgnoreCase);
        if (ColumnConfig.SpecificExclusions.TryGetValue(tableName, out var specificCols))
        {
            colsToDrop.UnionWith(specificCols);
        }

        RemoveColumns(dt1, colsToDrop);
        RemoveColumns(dt2, colsToDrop);

        // ÉTAPE 4 : Alignement des schémas de données (Colonnes communes)
        var cols1 = dt1.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        var cols2 = dt2.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        var commonCols = cols1.Intersect(cols2, StringComparer.OrdinalIgnoreCase).OrderBy(c => c).ToList();

        if (!commonCols.Any())
            return ("KO_NO_COMMON_COLS", "Aucune colonne commune trouvée après l'application des filtres d'exclusion.", null);

        KeepOnlyCommonColumns(dt1, commonCols);
        KeepOnlyCommonColumns(dt2, commonCols);

        // ÉTAPE 5 : Normalisation et formatage des données
        NormalizeData(dt1, commonCols);
        NormalizeData(dt2, commonCols);

        // ÉTAPE 6 : Alignement des enregistrements (Tri)
        try
        {
            string sortString = string.Join(", ", commonCols);
            
            dt1.DefaultView.Sort = sortString;
            dt1 = dt1.DefaultView.ToTable();

            dt2.DefaultView.Sort = sortString;
            dt2 = dt2.DefaultView.ToTable();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Attention: Le tri technique a échoué sur la table {tableName}. Raison : {ex.Message}");
        }

        // ÉTAPE 7 : Comparaison finale et génération du rapport d'écarts
        if (dt1.Rows.Count != dt2.Rows.Count)
        {
            return ("KO_ROW_COUNT", $"Écart sur le volume de données : Source = {dt1.Rows.Count} lignes vs Cible = {dt2.Rows.Count} lignes.", null);
        }

        try
        {
            DataTable diffTable = CreateDiffTable();
            bool hasDifferences = false;

            for (int i = 0; i < dt1.Rows.Count; i++)
            {
                foreach (string colName in commonCols)
                {
                    object val1 = dt1.Rows[i][colName];
                    object val2 = dt2.Rows[i][colName];

                    if (!EqualsNormalized(val1, val2))
                    {
                        hasDifferences = true;
                        diffTable.Rows.Add(i + 1, colName, val1 == DBNull.Value ? "NULL" : val1, val2 == DBNull.Value ? "NULL" : val2);
                    }
                }
            }

            if (!hasDifferences)
                return ("OK", null, null);

            return ("KO", null, diffTable);
        }
        catch (Exception ex)
        {
            return ("KO_ERROR", $"Erreur technique lors de la génération du différentiel : {ex.Message}", null);
        }
    }

    // --- MÉTHODES UTILITAIRES PRIVÉES ---

    private static void RemoveColumns(DataTable dt, HashSet<string> colsToDrop)
    {
        for (int i = dt.Columns.Count - 1; i >= 0; i--)
        {
            if (colsToDrop.Contains(dt.Columns[i].ColumnName))
                dt.Columns.RemoveAt(i);
        }
    }

    private static void KeepOnlyCommonColumns(DataTable dt, List<string> commonCols)
    {
        var commonSet = new HashSet<string>(commonCols, StringComparer.OrdinalIgnoreCase);
        for (int i = dt.Columns.Count - 1; i >= 0; i--)
        {
            if (!commonSet.Contains(dt.Columns[i].ColumnName))
                dt.Columns.RemoveAt(i);
        }
    }

    private static void NormalizeData(DataTable dt, List<string> commonCols)
    {
        foreach (DataRow row in dt.Rows)
        {
            foreach (string colName in commonCols)
            {
                if (row[colName] == DBNull.Value) continue;

                Type colType = dt.Columns[colName].DataType;

                if (colType == typeof(string))
                {
                    string strVal = row[colName].ToString().Trim();
                    if (strVal == "nan" || strVal == "None" || string.IsNullOrEmpty(strVal))
                        row[colName] = DBNull.Value;
                    else
                        row[colName] = strVal;
                }
                else if (colType == typeof(float) || colType == typeof(double) || colType == typeof(decimal))
                {
                    // On convertit en double pour l'arrondi mathématique à 4 décimales
                    if (double.TryParse(row[colName].ToString(), out double numVal))
                    {
                        row[colName] = Math.Round(numVal, 4);
                    }
                }
            }
        }
    }

    private static bool EqualsNormalized(object val1, object val2)
    {
        if (val1 == DBNull.Value && val2 == DBNull.Value) return true;
        if (val1 == DBNull.Value || val2 == DBNull.Value) return false;
        
        return val1.ToString() == val2.ToString();
    }

    private static DataTable CreateDiffTable()
    {
        DataTable diff = new DataTable("Differences");
        diff.Columns.Add("RowIndex", typeof(int));
        diff.Columns.Add("ColumnName", typeof(string));
        diff.Columns.Add("SourceValue", typeof(string));
        diff.Columns.Add("TargetValue", typeof(string));
        return diff;
    }
}