# 🎤 Soutenance Finale (10 minutes) · Pipeline de Données Complet
**Thématique : Ingestion Asynchrone, Transformation par Flux et Analyse SQL des Données Alimentaires Mondiales (Open Food Facts)**

---

## ⏱️ Répartition du Temps de Parole (10 minutes au total)

| Diapositive / Thème | Durée estimée | Membre en charge | Contenu clé |
| :--- | :--- | :--- | :--- |
| **1. Introduction & Thématique** | 1 min 30 | Membre 1 | Source Open Food Facts, volumétrie, licence ODbL, cas d'usage |
| **2. Architecture Globale** | 1 min 30 | Membre 1 | Schéma du pipeline, découplage raw/processed, choix technologiques |
| **3. Ingestion Asynchrone** | 2 min 00 | Membre 2 | `aiohttp`, sémaphore, backoff exponentiel, checkpoint & reprise |
| **4. Transformation & Parquet** | 1 min 30 | Membre 2 | Lots de 500 (pic 25,9 Mo), nettoyage, typage, −92,9 % de volume |
| **5. Modélisation & Requêtes SQL** | 2 min 00 | Membre 3 | Schéma en étoile, idempotence, Window Functions, Multi-CTE |
| **6. Optimisation & EXPLAIN ANALYZE**| 1 min 00 | Membre 3 | Index sur le code-barres : Sequential Scan → Index Scan, ×8 à ×9 |
| **7. Publication Hub & Bilan Chiffré**| 0 min 30 | Tous | Version `v1.0.0`, Dataset Card, métriques mesurées, perspectives |

---

## 📊 Support de Présentation Diapositive par Diapositive

### Slide 1 : Thématique & Choix de la Source de Données (1 min 30)
* **Pourquoi Open Food Facts ?**
  - **Données réelles et massives** : Base collaborative mondiale de plusieurs millions de produits alimentaires, accessible par une API paginée.
  - **Mises à jour permanentes** : nos fiches vont de 2012 à des modifications du 3 novembre 2026, la veille de la collecte, ce qui justifie un pipeline répétable plutôt qu'un téléchargement statique.
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
            │ Lecture ligne à ligne, écriture par lots de 500 (PyArrow)
            ▼
┌─────────────────────────┐
│ data/processed/         │ ◄── Snappy + déduplication (-92.9% d'espace disque)
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
* **Traitement par Lots** :
  - Les JSON Lines sont lus ligne à ligne, jamais chargés en entier.
  - Générateur Python par lots de 500 lignes, alimentant directement un `pyarrow.parquet.ParquetWriter`.
  - Mesuré : ≈ 2 s pour toute la transformation, pic mémoire 25,9 Mo, 953 doublons retirés sur 2 400 lignes.
  - Limite assumée : les tables produits / nutrition restent en mémoire jusqu'à la fin (croissance linéaire avec le volume).
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
* **Ce qui n'a pas marché d'abord** : des index composites pour un filtre « Nutri-Score A/B, peu sucré ». Plan identique avant/après (`Table Scan`), 6,05 → 5,75 ms : aucun gain. DuckDB est colonnaire et saute déjà les blocs hors filtre ; ses index ART ne servent qu'aux recherches sélectives sur une colonne, et 1 447 lignes tiennent dans un seul bloc.
* **Requête retenue** : recherche d'un produit par code-barres, sur `products_volume` (1 447 000 lignes : les produits réels répliqués 1 000 fois, codes suffixés, lignes mélangées).
  ```sql
  CREATE INDEX idx_products_volume_code ON products_volume (code);
  ```
* **Résultat mesuré (`EXPLAIN ANALYZE`)** :
  - SANS index : `Sequential Scan`, 12 groupes de lignes lus sur 12, ≈ 9 ms.
  - AVEC index : `Index Scan`, 0 groupe lu sur 12, ≈ 1 ms → **×8 à ×9** sur la base fichier (×2 à ×4 en mémoire).
  - Coût : 0,6 à 1,5 s pour construire l'index.
* **Lien avec le schéma** : la clé primaire `products(code)` crée exactement cet index ; il sert la recherche par code-barres et le `ON CONFLICT (code)` du chargement.

---

### Slide 7 : Publication Hugging Face & Bilan Chiffré (0 min 30)
* **Publication Hugging Face Hub** :
  - Script automatisé `scripts/publish_hub.py` via `huggingface_hub`.
  - Dépôt configuré comme privé par défaut, tagué `v1.0.0`.
  - Dataset Card conforme avec métadonnées YAML, dictionnaire de données et licence légale ODbL.

| Métrique mesurée | Valeur brute | Valeur traitée | Gain / Performance |
| :--- | :--- | :--- | :--- |
| **Volume de données** | 3,14 Mo (24 JSONL) | 228 Ko (5 Parquets) | **−92,9 %** (Snappy + déduplication) |
| **Enregistrements** | 2 400 produits bruts | 1 447 produits uniques | 953 doublons retirés (39,7 %) |
| **Transformation** | – | ≈ 2 s, pic **25,9 Mo** | 3 lots de 500 |
| **Couverture Tests** | – | **16 tests pytest** | 100 % de réussite en ≈ 2 s |
| **Index code-barres** (1 447 000 lignes) | Sequential Scan, 12/12 blocs, ≈ 9 ms | Index Scan, 0/12 blocs, ≈ 1 ms | **×8 à ×9** (base fichier) |

---

## 💡 Conseils pour l'Oral
1. **Fluidité des transitions** : Utilisez les métriques du tableau pour passer la parole à votre coéquipier (ex: *"Après cette collecte asynchrone résiliente, voyons comment nous avons compressé ce volume de 92.9%..."*).
2. **Démonstration en direct (facultatif si demandé)** :
   ```bash
   python main.py --all
   ```
   Exécute tout le pipeline en direct en ≈ 12 s (mesuré le 6/11), car les pages déjà présentes dans le checkpoint ne sont pas retéléchargées.
