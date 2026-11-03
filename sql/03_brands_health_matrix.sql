-- ============================================================================
-- Requête 3 : Jointures relationnelles & Agrégations GROUP BY / HAVING
-- Fichier : sql/03_brands_health_matrix.sql
-- Objectif : Évaluer la qualité nutritionnelle globale de l'offre par marque
-- Concept utilisé : Multi-JOIN, GROUP BY, HAVING, Agrégations conditionnelles (CASE WHEN)
-- ============================================================================

SELECT
    b.name AS marque,
    COUNT(p.code) AS total_produits,
    -- Pourcentage de produits Nutriscore favorable (A ou B)
    ROUND(
        100.0 * SUM(CASE WHEN p.nutriscore_grade IN ('A', 'B') THEN 1 ELSE 0 END) / COUNT(p.code),
        1
    ) AS pct_nutriscore_sain_ab,
    -- Pourcentage de produits Nutriscore défavorable (D ou E)
    ROUND(
        100.0 * SUM(CASE WHEN p.nutriscore_grade IN ('D', 'E') THEN 1 ELSE 0 END) / COUNT(p.code),
        1
    ) AS pct_nutriscore_defavorable_de,
    -- Pourcentage de produits ultra-transformés (NOVA 4)
    ROUND(
        100.0 * SUM(CASE WHEN p.nova_group = 4 THEN 1 ELSE 0 END) / NULLIF(COUNT(p.nova_group), 0),
        1
    ) AS pct_ultra_transformes_nova4,
    ROUND(AVG(n.energy_kcal), 1) AS calories_moyennes,
    ROUND(AVG(n.sugars_100g), 2) AS sucres_moyens_100g,
    ROUND(AVG(n.salt_100g), 2) AS sel_moyen_100g
FROM brands b
JOIN products p ON b.id = p.brand_id
LEFT JOIN product_nutrition n ON p.code = n.product_code
WHERE b.name <> 'Marque Inconnue'
GROUP BY b.id, b.name
HAVING COUNT(p.code) >= 3
ORDER BY total_produits DESC, pct_nutriscore_sain_ab DESC
LIMIT 25;
