-- ============================================================================
-- PROJET PIPELINE DE DONNÉES : OPEN FOOD FACTS
-- Fichier : sql/00_schema.sql
-- Description : DDL de création des tables relationnelles normalisées
-- Compatibilité : PostgreSQL (Supabase) et DuckDB
-- ============================================================================

-- Nettoyage préalable si recréation complète
DROP TABLE IF EXISTS product_nutrition;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS categories;
DROP TABLE IF EXISTS brands;

-- ----------------------------------------------------------------------------
-- Table 1 : Marques (Dimension)
-- ----------------------------------------------------------------------------
CREATE TABLE brands (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- ----------------------------------------------------------------------------
-- Table 2 : Catégories alimentaires (Dimension)
-- ----------------------------------------------------------------------------
CREATE TABLE categories (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- ----------------------------------------------------------------------------
-- Table 3 : Produits (Entité centrale)
-- Contraintes de clés étrangères, unicité et validations CHECK sur les scores
-- ----------------------------------------------------------------------------
CREATE TABLE products (
    code VARCHAR(64) PRIMARY KEY,
    product_name VARCHAR(500) NOT NULL,
    brand_id INTEGER NOT NULL REFERENCES brands(id),
    category_id INTEGER NOT NULL REFERENCES categories(id),
    nutriscore_grade VARCHAR(10) NOT NULL CHECK (nutriscore_grade IN ('A', 'B', 'C', 'D', 'E', 'UNKNOWN')),
    nova_group INTEGER CHECK (nova_group IS NULL OR (nova_group BETWEEN 1 AND 4)),
    ecoscore_grade VARCHAR(10) NOT NULL CHECK (ecoscore_grade IN ('A', 'B', 'C', 'D', 'E', 'UNKNOWN')),
    created_at TIMESTAMP WITH TIME ZONE,
    modified_at TIMESTAMP WITH TIME ZONE
);

-- ----------------------------------------------------------------------------
-- Table 4 : Valeurs nutritionnelles (Faits / Métriques pour 100g)
-- Contraintes physiologiques CHECK (pourcentages <= 100g, énergie <= 1000 kcal)
-- ----------------------------------------------------------------------------
CREATE TABLE product_nutrition (
    product_code VARCHAR(64) PRIMARY KEY REFERENCES products(code),
    energy_kcal NUMERIC(7,2) CHECK (energy_kcal IS NULL OR (energy_kcal >= 0 AND energy_kcal <= 1000)),
    fat_100g NUMERIC(5,2) CHECK (fat_100g IS NULL OR (fat_100g >= 0 AND fat_100g <= 100)),
    sugars_100g NUMERIC(5,2) CHECK (sugars_100g IS NULL OR (sugars_100g >= 0 AND sugars_100g <= 100)),
    proteins_100g NUMERIC(5,2) CHECK (proteins_100g IS NULL OR (proteins_100g >= 0 AND proteins_100g <= 100)),
    salt_100g NUMERIC(5,2) CHECK (salt_100g IS NULL OR (salt_100g >= 0 AND salt_100g <= 100)),
    sodium_100g NUMERIC(5,2) CHECK (sodium_100g IS NULL OR (sodium_100g >= 0 AND sodium_100g <= 50))
);
