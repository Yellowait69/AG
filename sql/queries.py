QUERIES = {
    # -------------------------------------------------------------------------
    # 1. ÉTAPE PRÉLIMINAIRE : RÉCUPÉRATION ID
    # -------------------------------------------------------------------------
    # Sert à trouver le NO_CNT (Interne) à partir du numéro utilisateur (Externe)
    "GET_INTERNAL_ID": """
                       SELECT NO_CNT
                       FROM LV.SCNTT0
                       WHERE NO_CNT_EXTENDED = '{contract_number}'
                       """,

    # -------------------------------------------------------------------------
    # 2. DONNÉES CONTRAT & AVENANTS
    # -------------------------------------------------------------------------
    # Table principale du contrat
    "LV.SCNTT0": """
                 SELECT * FROM LV.SCNTT0
                 WHERE NO_CNT = {internal_id}
                 """,

    # Table des avenants (Historique des modifications)
    # Trié par N° Avenant pour avoir la chronologie
    "LV.SAVTT0": """
                 SELECT * FROM LV.SAVTT0
                 WHERE NO_CNT = {internal_id}
                 ORDER BY NO_AVT ASC
                 """,

    # -------------------------------------------------------------------------
    # 3. DONNÉES PRODUITS / GARANTIES
    # -------------------------------------------------------------------------
    # Table des supports/garanties (Coverages)
    # Trié par Avenant puis par Code Proposition pour aligner les garanties
    "LV.SWBGT0": """
                 SELECT * FROM LV.SWBGT0
                 WHERE NO_CNT = {internal_id}
                 ORDER BY NO_AVT ASC, C_PROP ASC
                 """,

    # -------------------------------------------------------------------------
    # 4. DONNÉES BÉNÉFICIAIRES & CLAUSES
    # -------------------------------------------------------------------------
    # Table SCLS
    # Trié par Avenant et Ordre de clause
    "LV.SCLST0": """
                 SELECT * FROM LV.SCLST0
                 WHERE NO_CNT = {internal_id}
                 ORDER BY NO_AVT ASC, NO_ORD_CLS ASC
                 """,

    # Table SCLR
    # Trié par Avenant, Clause et Rang (ligne) pour reconstruire le texte dans l'ordre
    "LV.SCLRT0": """
                 SELECT * FROM LV.SCLRT0
                 WHERE NO_CNT = {internal_id}
                 ORDER BY NO_AVT ASC, NO_ORD_CLS ASC, NO_ORD_RNG ASC
                 """,

    # -------------------------------------------------------------------------
    # 5. DONNÉES FINANCIÈRES (RÉSERVES & MOUVEMENTS)
    # -------------------------------------------------------------------------
    # Table BSPD
    # Trié par date d'effet ou numéro de mouvement pour suivre l'évolution financière
    "LV.BSPDT0": """
                 SELECT * FROM LV.BSPDT0
                 WHERE NO_CNT = {internal_id}
                 ORDER BY NO_ORD_TRF_EPA ASC, NO_ORD_MVT_EPA ASC
                 """,

    # Table BSPG
    # Trié pour correspondre à BSPD
    "LV.BSPGT0": """
                 SELECT * FROM LV.BSPGT0
                 WHERE NO_CNT = {internal_id}
                 ORDER BY NO_ORD_TRF_EPA ASC
                 """
}