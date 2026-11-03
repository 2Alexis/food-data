# 🥗 Open Food Facts Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.13+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![License: ODbL](https://img.shields.io/badge/License-ODbL-blue.svg)](https://opendatacommons.org/licenses/odbl/)
[![Parquet](https://img.shields.io/badge/Format-Apache%20Parquet-teal.svg)](https://parquet.apache.org/)
[![Database](https://img.shields.io/badge/Database-PostgreSQL%20%7C%20DuckDB-336791.svg)](https://duckdb.org/)
[![Tests](https://img.shields.io/badge/Pytest-16%20Passed-brightgreen.svg)](https://pytest.org/)

Pipeline de données complet (ETL) développé dans le cadre du module **Données et pipelines SQL** (B3 Ynov Campus).
Le projet assure l'ingestion asynchrone, la transformation par flux bornée en mémoire, le stockage relationnel idempotent sous SQL avec indexation justifiée par `EXPLAIN ANALYZE`, et la publication sur le **Hugging Face Hub** avec versionnement et Dataset Card ODbL.

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
             │ Streaming par lots (Chunking PyArrow, O(1) RAM)
             ▼
 ┌─────────────────────────┐
 │ data/processed/         │ ◄── Compression Snappy (-92.9% d'espace)
 │ (Parquet relationnel)   │
 └─────┬─────────────┬─────┘
       │             │
       ▼             ▼
 ┌──────────────┐  ┌─────────────────────────┐
 │ Base SQL     │  │ Hugging Face Hub        │
 │ (PostgreSQL/ │  │ (Dépôt privé, v1.0.0,   │
 │ DuckDB)      │  │ Dataset Card ODbL)      │
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
- **Traitement par Lots (Streaming Chunking)** : Aucun chargement complet en mémoire RAM. Traitement par blocs de 500 enregistrements directement injectés dans `pyarrow.parquet.ParquetWriter`. Empreinte mémoire bornée $O(1)$ (**pic RAM mesuré à 24.7 Mo**).
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
  5. `05_optimization_explain_analyze.sql` : Justification d'index B-Tree composite via `EXPLAIN ANALYZE` avec gain de performance mesuré.

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
*(Optionnel : renseignez `HF_TOKEN=hf_...` pour la publication réelle sur Hugging Face et `DATABASE_URL=...` pour PostgreSQL).*

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

Ces chiffres mesurés sont récapitulés dans `PRESENTATION.md` pour la soutenance finale :

| Indicateur mesuré | Valeur brute / Avant | Valeur propre / Après | Gain / Efficacité |
| :--- | :--- | :--- | :--- |
| **Volumétrie disque** | 3.14 Mo (24 fichiers JSONL) | 228 Ko (5 fichiers Parquet) | **-92.9% d'espace économisé** |
| **Nombre d'enregistrements** | 2 400 lignes brutes | 1 447 produits dédupliqués | Nettoyage & typage à 100% |
| **Pic de mémoire RAM** | - | **24.72 Mo** | Mémoire streaming constante $O(1)$ |
| **Temps requête filtrée (SQL)** | 32.38 ms (Scan séquentiel) | 11.90 ms (Scan indexé) | **Accélération mesurée (x2.7)** |
| **Tests automatisés** | - | **16 tests unitaires & d'intégration** | 100% succès en 2.3s |

---

## 📄 Licence et Mentions Légales

Données sources issues de la base collaborative mondiale **Open Food Facts** (https://world.openfoodfacts.org).
Ce projet et le jeu de données produit sont distribués sous licence libre **ODbL (Open Database License v1.0)**.
