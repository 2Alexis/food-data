-- ============================================================================
-- Requête 5 : Optimisation de performance & Justification d'Index
-- Fichier : sql/05_optimization_explain_analyze.sql
-- Objectif : Justifier un index par EXPLAIN ANALYZE, avant / après création.
-- Moteur : DuckDB (exécuter après scripts/load_sql.py, sur data/food_database.duckdb)
-- Version automatisée : scripts/load_sql.py (run_explain_analyze_benchmark)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Pourquoi cette requête et pas un filtre analytique ?
-- ----------------------------------------------------------------------------
-- Première version testée : index composites (nutriscore_grade, category_id) et
-- (sugars_100g, energy_kcal) pour une recherche "Nutri-Score A/B, peu sucré".
-- Mesure : plan identique (Table Scan) avant et après, 6,05 ms -> 5,75 ms en
-- médiane, soit aucun gain. Deux raisons :
--   1. DuckDB est colonnaire et saute déjà les blocs hors filtre (statistiques
--      min/max par groupe de lignes) ; ses index ART ne servent qu'aux
--      recherches sélectives sur une colonne (égalité, petite plage).
--   2. 1 447 lignes tiennent dans un seul groupe de lignes : rien à éviter.
-- On mesure donc l'index sur l'accès qu'il sert vraiment : retrouver un produit
-- par son code-barres (cas d'une appli de scan), sur un volume réaliste.

-- ----------------------------------------------------------------------------
-- ÉTAPE 0 : volume de test (1 447 000 lignes)
-- Chaque produit réel est répliqué 1 000 fois avec un code suffixé ; les lignes
-- sont mélangées, sinon l'ordre permettrait déjà à DuckDB d'élaguer les blocs.
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS products_volume;

CREATE TABLE products_volume AS
SELECT p.code || lpad(k::VARCHAR, 4, '0') AS code,
       p.product_name, p.brand_id, p.category_id, p.nutriscore_grade, p.nova_group
FROM products p, range(1000) AS t(k)
ORDER BY hash(p.code, k);

-- ----------------------------------------------------------------------------
-- ÉTAPE 1 : plan AVANT index
-- (code réel du 724e produit par ordre de code + suffixe '0500' ;
--  load_sql.py choisit le même automatiquement)
-- ----------------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT code, product_name, nutriscore_grade, nova_group
FROM products_volume
WHERE code = '37600497988450500';  -- « Pain de mie grandes tranches Avoine & Graines », copie n° 500

-- Mesuré le 6/11/2026 (3 relances par configuration) :
--   Type: Sequential Scan · Row Groups Scanned: 12 / 12
--   Total Time ≈ 8 à 9 ms sur la base fichier (load_sql.py),
--              ≈ 3,6 à 4,4 ms en mémoire (benchmark_pipeline.py)

-- ----------------------------------------------------------------------------
-- ÉTAPE 2 : création de l'index (ART, équivalent DuckDB d'un B-Tree)
-- ----------------------------------------------------------------------------
CREATE INDEX idx_products_volume_code ON products_volume (code);
-- Coût mesuré : ≈ 1,5 s de création sur la base fichier, ≈ 0,6 s en mémoire.

-- ----------------------------------------------------------------------------
-- ÉTAPE 3 : plan APRÈS index
-- ----------------------------------------------------------------------------
EXPLAIN ANALYZE
SELECT code, product_name, nutriscore_grade, nova_group
FROM products_volume
WHERE code = '37600497988450500';  -- « Pain de mie grandes tranches Avoine & Graines », copie n° 500

-- Mesuré :
--   Type: Index Scan · Row Groups Scanned: 0 / 12
--   Total Time ≈ 1 ms sur la base fichier (gain ×8 à ×9),
--              ≈ 1,1 à 1,7 ms en mémoire (gain ×2 à ×4).
--   La preuve la plus stable est le plan : 12 groupes de lignes lus sans
--   index, aucun avec. Le temps, lui, dépend du stockage et de la machine.
--
-- Lien avec le schéma réel : la clé primaire products(code) crée exactement cet
-- index. C'est lui qui rend rapides la recherche par code-barres et le
-- INSERT ... ON CONFLICT (code) du chargement idempotent.

DROP TABLE IF EXISTS products_volume;
