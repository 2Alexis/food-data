-- ============================================================================
-- Requête 1 : Fonctions de fenêtrage (Window Functions)
-- Fichier : sql/01_window_ranking.sql
-- Objectif : Identifier le Top 3 des produits les plus caloriques par catégorie
-- Concept utilisé : DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...), CTE
-- ============================================================================

WITH classement_calories AS (
    SELECT
        c.name AS categorie,
        p.code AS code_produit,
        p.product_name AS nom_produit,
        b.name AS marque,
        p.nutriscore_grade AS nutriscore,
        n.energy_kcal AS calories_100g,
        n.sugars_100g AS sucres_100g,
        n.fat_100g AS lipides_100g,
        DENSE_RANK() OVER (
            PARTITION BY c.id
            ORDER BY n.energy_kcal DESC NULLS LAST, p.code ASC
        ) AS rang_calorie,
        ROW_NUMBER() OVER (
            PARTITION BY c.id
            ORDER BY n.energy_kcal DESC NULLS LAST, p.code ASC
        ) AS numero_ligne
    FROM products p
    JOIN categories c ON p.category_id = c.id
    JOIN brands b ON p.brand_id = b.id
    JOIN product_nutrition n ON p.code = n.product_code
    WHERE n.energy_kcal IS NOT NULL
)
SELECT
    categorie,
    rang_calorie,
    nom_produit,
    marque,
    nutriscore,
    calories_100g,
    sucres_100g,
    lipides_100g
FROM classement_calories
WHERE rang_calorie <= 3
ORDER BY categorie ASC, rang_calorie ASC;
