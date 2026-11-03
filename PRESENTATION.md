# 🎤 Soutenance Finale (10 minutes) · Pipeline de Données Complet
**Thématique : Ingestion Asynchrone, Transformation par Flux et Analyse SQL des Données Alimentaires Mondiales (Open Food Facts)**

---

## ⏱️ Répartition du Temps de Parole (10 minutes au total)

| Diapositive / Thème | Durée estimée | Membre en charge | Contenu clé |
| :--- | :--- | :--- | :--- |
| **1. Introduction & Thématique** | 1 min 30 | Membre 1 | Source Open Food Facts, volumétrie, licence ODbL, cas d'usage |
| **2. Architecture Globale** | 1 min 30 | Membre 1 | Schéma du pipeline, découplage raw/processed, choix technologiques |
| **3. Ingestion Asynchrone** | 2 min 00 | Membre 2 | `aiohttp`, sémaphore, backoff exponentiel, checkpoint & reprise |
| **4. Transformation & Parquet** | 1 min 30 | Membre 2 | Streaming par lots (RAM O(1)), nettoyage, typage, 92.9% de compression |
| **5. Modélisation & Requêtes SQL** | 2 min 00 | Membre 3 | Schéma en étoile, idempotence, Window Functions, Multi-CTE |
| **6. Optimisation & EXPLAIN ANALYZE**| 1 min 00 | Membre 3 | Index composite B-Tree, plan d'accès, facteur d'accélération |
| **7. Publication Hub & Bilan Chiffré**| 0 min 30 | Tous | Version `v1.0.0`, Dataset Card, métriques mesurées, perspectives |

---

## 📊 Support de Présentation Diapositive par Diapositive

### Slide 1 : Thématique & Choix de la Source de Données (1 min 30)
* **Pourquoi Open Food Facts ?**
  - **Données réelles et massives** : Base collaborative mondiale de plus de 4,8 millions de produits alimentaires.
  - **Mises à jour permanentes** : Plusieurs milliers de scans et d'ajouts quotidiens justifiant un pipeline automatisé et répétable plutôt qu'un téléchargement statique.
  - **Licence claire et ouverte** : Distribuée sous licence **ODbL (Open Database License)**, 100% compatible avec une redistribution publique sur Hugging Face Hub.
  - **Richesse et variété des types** :
    - Textuel (`product_name`, `brand`, `category`)
    - Catégoriel (`nutriscore_grade`, `nova_group`, `ecoscore_grade`)
    - Métriques continues (`energy_kcal`, `sugars_100g`, `fat_100g`, `salt_100g`)
    - Temporel (`created_at`, `modified_at` avec fuseaux horaires UTC)

---

### Slide 2 : Architecture Technique du Pipeline (1 min 30)
```
┌─────────────────────────┐
│ API Open Food Facts v2  │
└───────────┬─────────────┘
            │ aiohttp asynchrone (Semaphore=3, Timeouts, Backoff exponentiel)
            ▼
┌─────────────────────────┐
│ data/raw/ (*.jsonl)     │ ◄── Point de reprise (Checkpoint déterministe)
└───────────┬─────────────┘
            │ Streaming par lots de 500 (PyArrow, RAM constante O(1))
            ▼
┌─────────────────────────┐
│ data/processed/         │ ◄── Compression Snappy (-92.9% d'espace disque)
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
* **Principe clé d'isolation** : Les données brutes dans `data/raw/` ne sont jamais modifiées. `data/` et les secrets `.env` sont strictement exclus de Git via `.gitignore`.

---

### Slide 3 : Ingestion Asynchrone & Résilience (2 min 00)
* **Parallélisme maîtrisé** : Limitation par `asyncio.Semaphore(3)` évitant de surcharger le serveur ou d'être banni par rate-limiting.
* **Gestion des erreurs et Backoff Exponentiel** :
  - Sur erreur HTTP `503 Service Unavailable` ou `429 Too Many Requests`, application d'un délai exponentiel aléatoire :
    $$\text{délai} = \text{base} \times 2^{\text{tentative}-1} + \text{jitter}$$
  - Jusqu'à 4 tentatives avant abandon.
* **Tolérance aux pannes & Checkpointing** :
  - Chaque lot écrit génère un fichier déterministe : `products_{category}_page_{XXXX}.jsonl`.
  - Écriture atomique (fichier temporaire `.tmp` puis renommage direct).
  - Enregistrement de l'état dans `data/raw/checkpoint.json` : en cas d'interruption (Ctrl+C ou coupure réseau), la relance saute instantanément les pages déjà acquises.

---

### Slide 4 : Transformation par Lots & Optimisation Parquet (1 min 30)
* **Traitement par Lots (Streaming Chunking)** :
  - Aucun chargement global en mémoire RAM ($O(1)$ au lieu de $O(N)$).
  - Générateur Python par lots de 500 lignes, alimentant directement un `pyarrow.parquet.ParquetWriter`.
* **Nettoyage et Règles Métier** :
  - Déduplication stricte des codes-barres (EAN-13).
  - Validation physique des nutriments ($0 \le \text{nutriments} \le 100\text{g}$, $\text{énergie} \le 1000\text{ kcal}$).
  - Normalisation des labels (Nutriscore en majuscules `'A'` à `'E'`, NOVA `'1'` à `'4'`).
  - Conversion des timestamps Unix en objets `timestamp[ms, tz=UTC]`.
* **Modélisation relationnelle (Schéma en étoile)** :
  - `products_cleaned.parquet` (Table unifiée)
  - `dim_brands.parquet` & `dim_categories.parquet` (Dimensions normalisées)
  - `dim_products.parquet` & `fact_nutrition.parquet` (Faits nutritionnels)

---

### Slide 5 : Base SQL & Requêtes Analytiques (2 min 00)
* **Schéma relationnel robuste** : Clés primaires (`PRIMARY KEY`), intégrité référentielle (`FOREIGN KEY`), contraintes `CHECK` et `NOT NULL`.
* **Chargement Idempotent** :
  - Syntaxe `INSERT INTO ... ON CONFLICT (code) DO NOTHING`.
  - Possibilité de recharger indéfiniment la base sans risquer de créer de doublons ni d'altérer l'état.
* **4 Requêtes d'Analyse complexes développées** :
  1. **Window Ranking** : `DENSE_RANK() OVER (PARTITION BY c.id ORDER BY n.energy_kcal DESC)` pour dégager le podium calorique de chaque univers alimentaire.
  2. **Window Moving Average & LAG** : Moyenne mobile lissée sur 5 produits consécutifs (`ROWS BETWEEN 4 PRECEDING AND CURRENT ROW`) et variation $\Delta$ vs produit précédent pour suivre l'évolution temporelle des sucres.
  3. **Jointures & GROUP BY / HAVING** : Matrice qualité des industriels calculant le pourcentage d'aliments sains (Nutriscore A/B) vs défavorables (D/E) et ultra-transformés (NOVA 4) pour les marques ayant $\ge 3$ références.
  4. **Multi-CTE Paradoxes** : Détection des aliments ultra-transformés (NOVA 4) pourtant gratifiés d'un Nutriscore A (ex: chips de lentilles industrielles, substituts végétaux ultra-transformés) avec écart à la moyenne de la catégorie.

---

### Slide 6 : Optimisation SQL & EXPLAIN ANALYZE (1 min 00)
* **Requête cible** : Filtrage multi-critères à forte sélectivité (Nutriscore 'A'/'B', sucres $\le 5\text{g}$, énergie entre 50 et 400 kcal, jointure 4 tables et tri par protéines décroissantes).
* **Analyse comparative mesurée** :
  - **SANS index** : Sequential Scan complet de la table des produits et de la table nutrition. Parcours de tous les blocs de données en mémoire.
  - **Création des index ciblés** :
    ```sql
    CREATE INDEX idx_products_nutriscore_cat ON products(nutriscore_grade, category_id);
    CREATE INDEX idx_nutrition_sugars_energy ON product_nutrition(sugars_100g, energy_kcal);
    ```
  - **AVEC index** : Remplacement par un Index Scan ciblé. Élimination immédiate des tuples non pertinents sans surcoût d'I/O.
  - **Résultat mesuré** : Accélération notable du temps de réponse confirmée par le plan d'exécution `EXPLAIN ANALYZE`.

---

### Slide 7 : Publication Hugging Face & Bilan Chiffré (0 min 30)
* **Publication Hugging Face Hub** :
  - Script automatisé `scripts/publish_hub.py` via `huggingface_hub`.
  - Dépôt configuré comme privé par défaut, tagué `v1.0.0`.
  - Dataset Card conforme avec métadonnées YAML, dictionnaire de données et licence légale ODbL.

| Métrique mesurée | Valeur brute | Valeur traitée | Gain / Performance |
| :--- | :--- | :--- | :--- |
| **Volume de données** | 3.14 Mo (24 JSONL) | 228 Ko (5 Parquets) | **-92.9% de réduction d'espace** |
| **Enregistrements** | 2 400 produits bruts | 1 447 produits uniques | Déduplication & assainissement 100% |
| **Empreinte RAM pic** | - | **24.72 Mo** | Mémoire streaming bornée $O(1)$ |
| **Couverture Tests** | - | **16 tests pytest** | 100% succès en 2.3 secondes |
| **Gain Index SQL** | 32.38 ms | 11.90 ms | **Accélération mesurée (x2.7)** |

---

## 💡 Conseils pour l'Oral
1. **Fluidité des transitions** : Utilisez les métriques du tableau pour passer la parole à votre coéquipier (ex: *"Après cette collecte asynchrone résiliente, voyons comment nous avons compressé ce volume de 92.9%..."*).
2. **Démonstration en direct (facultatif si demandé)** :
   ```bash
   python main.py --all
   ```
   Exécute la validation complète en direct en moins de 10 secondes !
