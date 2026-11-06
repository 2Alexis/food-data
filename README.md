# 🥗 Open Food Facts Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.13+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![License: ODbL](https://img.shields.io/badge/License-ODbL-blue.svg)](https://opendatacommons.org/licenses/odbl/)
[![Parquet](https://img.shields.io/badge/Format-Apache%20Parquet-teal.svg)](https://parquet.apache.org/)
[![Database](https://img.shields.io/badge/Database-DuckDB-336791.svg)](https://duckdb.org/)
[![Tests](https://img.shields.io/badge/Pytest-16%20Passed-brightgreen.svg)](https://pytest.org/)

Pipeline de données complet (ETL) développé dans le cadre du module **Données et pipelines SQL** (B3 Ynov Campus).
Le projet assure l'ingestion asynchrone, la transformation par lots vers Parquet, le stockage relationnel idempotent sous SQL avec indexation justifiée par `EXPLAIN ANALYZE`, et la publication sur le **Hugging Face Hub** avec versionnement et Dataset Card ODbL.

---

## 📐 Architecture du Pipeline

```
 ┌─────────────────────────┐
 │ API Open Food Facts v2  │
 └───────────┬─────────────┘
             │ aiohttp asynchrone (Semaphore, Timeouts, Exponential Backoff)
             ▼
 ┌─────────────────────────┐
 │ data/raw/ (*.jsonl)     │ ◄── Point de reprise (Checkpoint déterministe)
 └───────────┬─────────────┘
             │ Lecture ligne à ligne, écriture par lots de 500 (PyArrow)
             ▼
 ┌─────────────────────────┐
 │ data/processed/         │ ◄── Snappy + déduplication (-92.9% d'espace)
 │ (Parquet relationnel)   │
 └─────┬─────────────┬─────┘
       │             │
       ▼             ▼
 ┌──────────────┐  ┌─────────────────────────┐
 │ Base SQL     │  │ Hugging Face Hub        │
 │ (DuckDB)     │  │ (Dépôt privé, v1.0.0,   │
 │              │  │ Dataset Card ODbL)      │
 └──────────────┘  └─────────────────────────┘
```

---

## 📁 Structure du Dépôt

La structure respecte scrupuleusement les exigences du projet :

```
food-data-pipeline/
├── data/
│   ├── raw/                 # Données brutes JSON Lines (JAMAIS modifiées, ignorées par Git)
│   └── processed/           # Données nettoyées en Parquet (ignorées par Git)
├── scripts/
│   ├── ingest/              # Collecte asynchrone résiliente
│   │   └── fetch_raw_data.py
│   ├── transform/           # Nettoyage et typage en flux continu
│   │   └── clean_and_transform.py
│   ├── load_sql.py          # Chargement idempotent & analyses SQL
│   ├── publish_hub.py       # Publication Hugging Face Hub & Dataset Card
│   └── benchmark_pipeline.py# Mesures réelles de performance (RAM, disques, index)
├── sql/
│   ├── 00_schema.sql        # Schéma DDL avec contraintes, clés primaires et étrangères
│   ├── 01_window_ranking.sql# Window Function : DENSE_RANK() Top 3 calories par catégorie
│   ├── 02_window_moving_avg.sql # Window Function : Moyenne mobile et LAG sur les sucres
│   ├── 03_brands_health_matrix.sql # Multi-JOIN & GROUP BY / HAVING (Matrice santé marques)
│   ├── 04_ultra_processed_paradox_cte.sql # Multi-CTE : Paradoxe NOVA 4 & Nutriscore A
│   └── 05_optimization_explain_analyze.sql # Indexation justifiée par EXPLAIN ANALYZE
├── tests/
│   ├── test_ingest.py       # Tests d'ingestion et checkpoint
│   ├── test_transform.py    # Tests de nettoyage, typage et streaming
│   └── test_sql.py          # Tests du schéma, de l'idempotence et des requêtes
├── .env.example             # Exemple des variables d'environnement requises
├── .gitignore               # Exclusion stricte de data/ et des secrets .env
├── main.py                  # Orchestrateur CLI de bout en bout
├── PRESENTATION.md          # Guide & diapositives pour la soutenance finale (10 min)
└── README.md                # Documentation & Dataset Card
```

---

## ⚡ Caractéristiques et Attendus Validés

### 1. Ingestion Asynchrone (`scripts/ingest/fetch_raw_data.py`)
- **`aiohttp` & Concurrence** : Sessions asynchrones bridées par un sémaphore configurable (`asyncio.Semaphore(3)`).
- **Timeouts stricts** : Protection via `ClientTimeout(total=30, connect=10)`.
- **Backoff Exponentiel & Jitter** : En cas de coupure ou d'erreur `503 Service Unavailable`, application d'un délai exponentiel aléatoire ($base \times 2^{tentative-1} + jitter$).
- **Nommage Déterministe & Checkpoint** : Fichiers `products_{cat}_page_{XXXX}.jsonl`. En cas d'interruption du script, la relance reprend exactement là où elle s'est arrêtée grâce à `checkpoint.json`.

### 2. Transformation par Lots (`scripts/transform/clean_and_transform.py`)
- **Traitement par Lots** : les fichiers JSON Lines sont lus ligne à ligne (jamais chargés en entier) et la table unifiée est écrite par blocs de 500 enregistrements via `pyarrow.parquet.ParquetWriter`. **Pic mémoire mesuré : 25,9 Mo** (tracemalloc, transformation complète en ≈ 2 s). Limite connue : les tables `dim_products` / `fact_nutrition` et l'ensemble des codes déjà vus restent en mémoire jusqu'à la fin, donc la mémoire croît avec le volume ; à grande échelle, il faudrait les écrire elles aussi par lots.
- **Nettoyage et Typage** :
  - Déduplication stricte des codes-barres.
  - Normalisation du Nutri-Score (`A` à `E` ou `UNKNOWN`), de l'Éco-Score et du groupe NOVA (`1` à `4`).
  - Validation physique des nutriments (plages $0 \le x \le 100\text{g}$, énergie $\le 1000\text{ kcal}$).
  - Conversion des timestamps Unix en objets `timestamp[ms, tz=UTC]`.
- **Modélisation relationnelle** :
  - `products_cleaned.parquet` (Table unifiée dénormalisée)
  - `dim_brands.parquet` & `dim_categories.parquet` (Dimensions normalisées)
  - `dim_products.parquet` & `fact_nutrition.parquet` (Schéma en étoile)

### 3. Base SQL & Optimisation (`scripts/load_sql.py` & `sql/`)
- **Schéma relationnel strict** : Clés primaires, intégrité référentielle (`FOREIGN KEY`), contraintes `CHECK` et `NOT NULL`.
- **Idempotence garantie** : Les chargements peuvent être relancés indéfiniment sans créer de doublons ni d'erreurs de contrainte (`INSERT INTO ... ON CONFLICT DO NOTHING`).
- **5 Requêtes d'Analyse avancées** :
  1. `01_window_ranking.sql` : Podiums caloriques par catégorie avec `DENSE_RANK()`.
  2. `02_window_moving_avg.sql` : Moyenne mobile lissée sur 5 produits et calcul de $\Delta$ via `LAG()`.
  3. `03_brands_health_matrix.sql` : Matrice de qualité des marques avec jointure 3 tables, `GROUP BY`, `HAVING` et ratios conditionnels (`CASE WHEN`).
  4. `04_ultra_processed_paradox_cte.sql` : Chaînage de CTE repérant les aliments ultra-transformés (NOVA 4) paradoxalement notés Nutri-Score A.
  5. `05_optimization_explain_analyze.sql` : Index sur le code-barres justifié par `EXPLAIN ANALYZE` (voir ci-dessous).

#### Justification de l'index (requête 5)
- **Première tentative abandonnée** : des index composites sur `(nutriscore_grade, category_id)` et `(sugars_100g, energy_kcal)` ne changeaient pas le plan (`Table Scan` avant et après, 6,05 → 5,75 ms : du bruit). DuckDB est colonnaire, il saute déjà les blocs hors filtre, et ses index ART ne servent qu'aux recherches sélectives sur une colonne. Avec 1 447 lignes, tout tient dans un seul bloc.
- **Démonstration retenue** : recherche d'un produit par code-barres sur `products_volume`, une table de **1 447 000 lignes** obtenue en répliquant 1 000 fois les produits réels (codes suffixés, lignes mélangées). C'est un volume simulé, construit à partir des vraies données.
- **Résultat** : `Sequential Scan`, 12 groupes de lignes lus sur 12 → `Index Scan`, **0 sur 12**. Temps moteur ≈ 9 ms → ≈ 1 ms sur la base fichier (×8 à ×9), ≈ 4 ms → ≈ 1,5 ms en mémoire (×2 à ×4). Coût : 0,6 à 1,5 s pour créer l'index.
- **Lien avec le schéma** : la clé primaire `products(code)` crée exactement cet index ; c'est lui qui sert la recherche par code-barres et le `ON CONFLICT (code)` du chargement idempotent.
- Le test `test_explain_analyze_index_gain` vérifie automatiquement ce changement de plan.

### 4. Publication sur le Hugging Face Hub (`scripts/publish_hub.py`)
- Script automatisé utilisant `huggingface_hub`.
- Dépôt configuré comme **privé par défaut** (`private=True`) pendant le développement.
- Version taguée explicitement : **`v1.0.0`**.
- Dataset Card complète et conforme générée automatiquement avec métadonnées YAML et mention de la licence **ODbL**.

---

## 🚀 Installation & Utilisation

### 1. Prérequis
Python 3.10 ou supérieur avec les dépendances :
```bash
pip install aiohttp pyarrow duckdb pytest huggingface-hub
```

### 2. Configuration (`.env`)
Copiez le modèle et personnalisez si nécessaire :
```bash
cp .env.example .env
```
*(Optionnel : renseignez `HF_TOKEN=hf_...` pour la publication réelle sur Hugging Face. `DATABASE_URL` permet de se connecter à PostgreSQL, mais le chargement des Parquet n'est implémenté que pour DuckDB.)*

### 3. Exécution avec l'Orchestrateur Principal
Pour lancer l'ensemble des étapes de bout en bout :
```bash
python main.py --all
```

Ou étape par étape :
```bash
# 1. Tests automatisés
python -m pytest -v

# 2. Ingestion asynchrone Open Food Facts
python scripts/ingest/fetch_raw_data.py --preset-diverse --pages-per-cat 3

# 3. Transformation par lots vers Parquet
python scripts/transform/clean_and_transform.py --chunk-size 500

# 4. Chargement SQL, vérification de l'idempotence et requêtes d'analyse
python scripts/load_sql.py --idempotent-test --run-queries

# 5. Benchmark des métriques chiffrées (RAM, disques, index)
python scripts/benchmark_pipeline.py

# 6. Publication sur le Hub (mode simulation par défaut)
python scripts/publish_hub.py --dry-run
```

---

## 📈 Résultats et Métriques Mesurées (Benchmark)

Chiffres obtenus avec `python scripts/benchmark_pipeline.py` et `python scripts/load_sql.py` (mesures du 6 novembre 2026, 3 relances ; ils varient selon la machine) :

| Indicateur mesuré | Avant | Après | Gain / Commentaire |
| :--- | :--- | :--- | :--- |
| **Volumétrie disque** | 3,14 Mo (24 fichiers JSONL) | 228 Ko (5 fichiers Parquet) | **−92,9 %** (Snappy + déduplication + colonnes non conservées) |
| **Enregistrements** | 2 400 lignes brutes | 1 447 produits uniques | 953 doublons (39,7 %) entre pages globales et par catégorie |
| **Transformation** | – | ≈ 2 s (1,7 à 2,5 s), pic mémoire **25,9 Mo** | 3 lots de 500 |
| **Chargement SQL** | 1er chargement 76 à 102 ms | relance 46 à 54 ms | Comptages identiques (518 / 28 / 1 447 / 1 447) |
| **Index sur le code-barres** (1 447 000 lignes) | `Sequential Scan`, 12/12 blocs, ≈ 9 ms | `Index Scan`, 0/12 blocs, ≈ 1 ms | **×8 à ×9** sur la base fichier, ×2 à ×4 en mémoire |
| **Tests automatisés** | – | **16 tests** pytest | 100 % de réussite en ≈ 2 s |

---

## 📄 Licence et Mentions Légales

Données sources issues de la base collaborative mondiale **Open Food Facts** (https://world.openfoodfacts.org).
Ce projet et le jeu de données produit sont distribués sous licence libre **ODbL (Open Database License v1.0)**.
