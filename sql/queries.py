QUERIES = {
    # -------------------------------------------------------------------------
    # 1. ÉTAPE PRÉLIMINAIRE : RÉCUPÉRATION ID
    # -------------------------------------------------------------------------
    # Ajout de TOP 1 pour sécuriser le retour scalaire
    "GET_INTERNAL_ID": """
                       SELECT TOP 1 NO_CNT
                       FROM LV.SCNTT0 WITH (NOLOCK)
                       WHERE NO_CNT_EXTENDED = '{contract_number}'
                       """,

    # -------------------------------------------------------------------------
    # 2. DONNÉES CONTRAT & AVENANTS
    # -------------------------------------------------------------------------
    "LV.SCNTT0": """
                 SELECT * FROM LV.SCNTT0 WITH (NOLOCK)
                 WHERE NO_CNT = {internal_id}
                 """,

    "LV.SAVTT0": """
                 SELECT * FROM LV.SAVTT0 WITH (NOLOCK)
                 WHERE NO_CNT = {internal_id}
                 ORDER BY NO_AVT ASC
                 """,

    # -------------------------------------------------------------------------
    # 3. DONNÉES PAIEMENTS / RECETTES (NOUVEAU & CRITIQUE)
    # -------------------------------------------------------------------------
    # Indispensable pour vérifier que l'activation (le paiement) a bien été prise en compte.
    # On trie par date de référence et timestamp pour comparer l'historique comptable.
    "LV.PRCTT0": """
                 SELECT * FROM LV.PRCTT0 WITH (NOLOCK)
                 WHERE NO_CNT = {internal_id}
                 ORDER BY D_REF_PRM ASC, TSTAMP_CRT_RCT ASC
                 """,

    # -------------------------------------------------------------------------
    # 4. DONNÉES PRODUITS / GARANTIES
    # -------------------------------------------------------------------------
    "LV.SWBGT0": """
                 SELECT * FROM LV.SWBGT0 WITH (NOLOCK)
                 WHERE NO_CNT = {internal_id}
                 ORDER BY NO_AVT ASC, C_PROP ASC
                 """,

    # -------------------------------------------------------------------------
    # 5. DONNÉES BÉNÉFICIAIRES & CLAUSES
    # -------------------------------------------------------------------------
    "LV.SCLST0": """
                 SELECT * FROM LV.SCLST0 WITH (NOLOCK)
                 WHERE NO_CNT = {internal_id}
                 ORDER BY NO_AVT ASC, NO_ORD_CLS ASC
                 """,

    "LV.SCLRT0": """
                 SELECT * FROM LV.SCLRT0 WITH (NOLOCK)
                 WHERE NO_CNT = {internal_id}
                 ORDER BY NO_AVT ASC, NO_ORD_CLS ASC, NO_ORD_RNG ASC
                 """,

    # -------------------------------------------------------------------------
    # 6. DONNÉES FINANCIÈRES (RÉSERVES & MOUVEMENTS)
    # -------------------------------------------------------------------------
    # Amélioration : Tri par Date d'abord, puis par Séquence.
    # Cela stabilise la comparaison si les séquences techniques changent mais pas la chronologie métier.
    "LV.BSPDT0": """
                 SELECT * FROM LV.BSPDT0 WITH (NOLOCK)
                 WHERE NO_CNT = {internal_id}
                 ORDER BY D_REF_MVT_EPA ASC, NO_ORD_TRF_EPA ASC, NO_ORD_MVT_EPA ASC
                 """,

    "LV.BSPGT0": """
                 SELECT * FROM LV.BSPGT0 WITH (NOLOCK)
                 WHERE NO_CNT = {internal_id}
                 ORDER BY D_REF_MVT_EPA ASC, NO_ORD_TRF_EPA ASC
                 """
}