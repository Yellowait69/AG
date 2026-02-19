using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;

public static class AppConfig
{
    // -----------------------------------------------------------------------------
    // 1. CONFIGURATION DES CHEMINS (FILESYSTEM)
    // -----------------------------------------------------------------------------
    
    // Équivalent de os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    // Remonte de deux dossiers par rapport au dossier d'exécution (ex: bin/Debug/net8.0)
    public static readonly string BaseDir = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, @"..\..\"));

    // Dossiers d'entrée/sortie
    public static readonly string InputDir = Path.Combine(BaseDir, "data", "input");
    public static readonly string OutputDir = Path.Combine(BaseDir, "data", "output");

    // --- DEFINITION DES FICHIERS CLES ---

    // A. Fichier source (Optionnel : liste des ID à dupliquer pour le script d'activation)
    public static readonly string SourceFile = Path.Combine(InputDir, "contrats_sources.xlsx");

    // B. Fichier pivot (Sortie de l'Activation -> Entrée du Comparateur)
    // CORRECTION MAJEURE : On pointe vers le fichier généré par run_activation.py
    public static readonly string ActivationOutputFile = Path.Combine(InputDir, "contrats_en_attente_activation.xlsx");

    // C. Variable utilisée par run_comparison.py (doit pointer sur le fichier pivot)
    public static readonly string InputFile = ActivationOutputFile;

    // -----------------------------------------------------------------------------
    // 2. CONFIGURATION BASES DE DONNÉES
    // -----------------------------------------------------------------------------

    // A. Configuration LISA (SQL Server) - Utilisée par src/database.py par défaut
    public static readonly ReadOnlyDictionary<string, string> DbConfig = new(new Dictionary<string, string>
    {
        { "DRIVER", "SQL Server" },       // Ou 'ODBC Driver 17 for SQL Server' si erreur
        { "SERVER", "SQLMFDBD01" },
        { "DATABASE", "FJ0AGDB_D000" },
        { "TRUSTED_CONNECTION", "yes" },  // 'yes' utilise l'auth Windows, sinon utilise UID/PWD
        { "UID", "XA3894" },
        // os.getenv() devient Environment.GetEnvironmentVariable()
        // L'opérateur ?? permet de définir la valeur par défaut si la variable d'environnement est null
        { "PWD", Environment.GetEnvironmentVariable("DB_PWD") ?? "*****************" } 
    });

    // B. Configuration ELIA (Pour l'injection/duplication - À ADAPTER)
    public static readonly ReadOnlyDictionary<string, string> DbConfigElia = new(new Dictionary<string, string>
    {
        { "DRIVER", "Oracle in OraClient19Home1" }, // Exemple courant pour ELIA
        { "SERVER", "ELIA_PROD_DB" },
        { "DATABASE", "ELIA_SCHEMA" },
        { "UID", "USER_ELIA" },
        { "PWD", "PASSWORD_ELIA" }
    });
}