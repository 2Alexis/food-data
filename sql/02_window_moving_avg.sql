-- ============================================================================
-- Requête 2 : Fonctions de fenêtrage temporelles (Moyenne mobile & LAG)
-- Fichier : sql/02_window_moving_avg.sql
-- Objectif : Analyser la tendance du taux de sucre au fil des créations de produits
-- Concept utilisé : AVG(...) OVER (ROWS BETWEEN), LAG(...) OVER (...), CTE
-- ============================================================================

WITH produits_chronologiques AS (
    SELECT
        p.code AS code_produit,
        p.product_name AS nom_produit,
        c.name AS categorie,
        p.created_at,
        n.sugars_100g,
        -- Moyenne mobile du sucre sur une fenêtre glissante de 5 produits (4 précédents + le courant)
        AVG(n.sugars_100g) OVER (
            PARTITION BY p.category_id
            ORDER BY p.created_at ASC, p.code ASC
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS moyenne_mobile_sucre_5p,
        -- Valeur de sucre du produit précédent dans la même catégorie
        LAG(n.sugars_100g, 1) OVER (
            PARTITION BY p.category_id
            ORDER BY p.created_at ASC, p.code ASC
        ) AS sucre_produit_precedent
    FROM products p
    JOIN categories c ON p.category_id = c.id
    JOIN product_nutrition n ON p.code = n.product_code
    WHERE p.created_at IS NOT NULL
      AND n.sugars_100g IS NOT NULL
)
SELECT
    code_produit,
    nom_produit,
    categorie,
    created_at,
    sugars_100g AS sucre_actuel,
    ROUND(moyenne_mobile_sucre_5p, 2) AS moyenne_glissante_sucre,
    sucre_produit_precedent,
    ROUND(sugars_100g - sucre_produit_precedent, 2) AS delta_vs_precedent
FROM produits_chronologiques
ORDER BY categorie ASC, created_at ASC
LIMIT 30;
