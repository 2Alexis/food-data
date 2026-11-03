-- ============================================================================
-- Requête 4 : Multi-CTE & Détection des paradoxes alimentaires
-- Fichier : sql/04_ultra_processed_paradox_cte.sql
-- Objectif : Détecter les aliments ultra-transformés (NOVA 4) bénéficiant
--            pourtant d'un Nutri-Score "A" et mesurer leur écart à la catégorie.
-- Concept utilisé : Enchaînement de CTE, Window aggregation, Filtres avancés
-- ============================================================================

WITH stats_par_categorie AS (
    -- Calcul des repères nutritionnels médians/moyens par catégorie
    SELECT
        category_id,
        ROUND(AVG(n.energy_kcal), 2) AS cat_avg_energy,
        ROUND(AVG(n.sugars_100g), 2) AS cat_avg_sugars,
        ROUND(AVG(n.salt_100g), 2) AS cat_avg_salt,
        COUNT(p.code) AS total_produits_cat
    FROM products p
    JOIN product_nutrition n ON p.code = n.product_code
    GROUP BY category_id
    HAVING COUNT(p.code) >= 5
),
produits_paradoxes AS (
    -- Produits ultra-transformés (NOVA 4) paradoxalement notés Nutriscore A
    SELECT
        p.code,
        p.product_name,
        b.name AS marque,
        c.name AS categorie,
        p.nutriscore_grade,
        p.nova_group,
        n.energy_kcal,
        n.sugars_100g,
        n.salt_100g,
        s.cat_avg_energy,
        s.cat_avg_sugars,
        s.cat_avg_salt,
        ROUND(n.sugars_100g - s.cat_avg_sugars, 2) AS diff_sucre_vs_moyenne_cat,
        ROUND(n.salt_100g - s.cat_avg_salt, 2) AS diff_sel_vs_moyenne_cat
    FROM products p
    JOIN brands b ON p.brand_id = b.id
    JOIN categories c ON p.category_id = c.id
    JOIN product_nutrition n ON p.code = n.product_code
    JOIN stats_par_categorie s ON p.category_id = s.category_id
    WHERE p.nutriscore_grade = 'A'
      AND p.nova_group = 4
)
SELECT
    code,
    product_name,
    marque,
    categorie,
    nova_group,
    nutriscore_grade,
    energy_kcal,
    sugars_100g,
    diff_sucre_vs_moyenne_cat,
    salt_100g,
    diff_sel_vs_moyenne_cat
FROM produits_paradoxes
ORDER BY diff_sucre_vs_moyenne_cat ASC, diff_sel_vs_moyenne_cat ASC
LIMIT 20;
