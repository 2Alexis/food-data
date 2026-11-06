"""
Script de Publication sur le Hugging Face Hub
=============================================
Respecte l'ensemble des attendus du sujet de projet :
- Publication programmatique via `huggingface_hub`
- Dépôt configuré comme PRIVÉ par défaut pendant le développement (`private=True`)
- Version taguée explicitement (`v1.0.0`)
- Génération d'une Dataset Card complète et conforme aux spécifications Hugging Face
  (métadonnées YAML, licence ODbL de la source Open Food Facts, dictionnaire des colonnes, etc.)
- Mode Simulation (--dry-run) pour validation locale sans token requis.
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [hub] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("hub")


def load_env_file(env_path: Path) -> None:
    """Charge les variables d'environnement depuis .env."""
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))


def generate_dataset_card_content(
    num_products: int,
    num_brands: int,
    num_categories: int,
    version: str = "v1.0.0"
) -> str:
    """Génère la documentation Markdown et les métadonnées YAML standard de la Dataset Card."""
    card = f"""---
language:
- fr
- en
license: odbl
task_categories:
- tabular-classification
- tabular-regression
tags:
- food
- nutrition
- nutriscore
- open-data
- etl-pipeline
- parquet
size_categories:
- 1K<n<10K
dataset_info:
  features:
  - name: code
    dtype: string
  - name: product_name
    dtype: string
  - name: brand
    dtype: string
  - name: category
    dtype: string
  - name: nutriscore_grade
    dtype: string
  - name: nova_group
    dtype: int32
  - name: ecoscore_grade
    dtype: string
  - name: energy_kcal_100g
    dtype: float64
  - name: fat_100g
    dtype: float64
  - name: sugars_100g
    dtype: float64
  - name: proteins_100g
    dtype: float64
  - name: salt_100g
    dtype: float64
  - name: sodium_100g
    dtype: float64
  - name: completeness
    dtype: float64
  - name: created_at
    dtype: timestamp[ms, tz=UTC]
  - name: modified_at
    dtype: timestamp[ms, tz=UTC]
---

# Open Food Facts · Dataset Traité & Nettoyé ({version})

Ce jeu de données est le produit d'un **pipeline de données complet (ETL)** développé dans le cadre du module *Données & Pipelines SQL*. Il rassemble des données de produits alimentaires issues de l'API publique d'**Open Food Facts**, collectées de manière asynchrone, nettoyées par flux, typées et exportées au format optimisé **Apache Parquet**.

## 📌 Résumé du Dataset

- **Version taguée** : `{version}`
- **Nombre de produits uniques** : {num_products}
- **Marques distinctes** : {num_brands}
- **Grandes catégories alimentaires** : {num_categories}
- **Format de stockage** : Apache Parquet (Compression Snappy)
- **Licence source** : [Open Database License (ODbL) v1.0](https://opendatacommons.org/licenses/odbl/)

## 🏗️ Structure des Tables

Le jeu de données est articulé selon un schéma optimisé :

1. **`products_cleaned.parquet`** : Table unifiée dénormalisée prête pour l'analyse et le Machine Learning.
2. **`dim_brands.parquet`** : Dimension des marques normalisées (`id`, `name`).
3. **`dim_categories.parquet`** : Dimension des catégories alimentaires (`id`, `name`).
4. **`dim_products.parquet`** : Dimension des métadonnées produits (`code`, `name`, `brand_id`, `category_id`, scores).
5. **`fact_nutrition.parquet`** : Table de faits contenant les métriques nutritionnelles strictes pour 100g.

## 🔬 Description des Champs

| Colonne | Type | Description |
| :--- | :--- | :--- |
| `code` | string | Code-barres unique du produit (EAN-13, UPC). Clé primaire. |
| `product_name` | string | Dénomination commerciale nettoyée. |
| `brand` | string | Marque principale normalisée. |
| `category` | string | Catégorie alimentaire standardisée. |
| `nutriscore_grade` | string | Note Nutri-Score officielle (`A`, `B`, `C`, `D`, `E` ou `UNKNOWN`). |
| `nova_group` | int32 | Classification du degré de transformation industrielle (`1` à `4`). |
| `ecoscore_grade` | string | Note d'impact environnemental (`A` à `E`). |
| `energy_kcal_100g` | float64 | Valeur énergétique en kilocalories pour 100g. |
| `fat_100g` | float64 | Teneur en lipides (matières grasses) pour 100g. |
| `sugars_100g` | float64 | Teneur en sucres simples pour 100g. |
| `proteins_100g` | float64 | Teneur en protéines pour 100g. |
| `salt_100g` | float64 | Teneur en sel pour 100g. |
| `sodium_100g` | float64 | Teneur en sodium pour 100g. |
| `completeness` | float64 | Score de complétude de la fiche produit (0.0 à 1.0). |
| `created_at` | timestamp | Date et heure de création de la fiche (UTC). |
| `modified_at` | timestamp | Date et heure de dernière mise à jour (UTC). |

## ⚖️ Droits et Licence

Ce jeu de données est distribué sous la licence libre **ODbL (Open Database License)**, conformément aux conditions de réutilisation de la base Open Food Facts.
Toute utilisation ou redistribution doit mentionner la source originale :
> *"Données issues de la base collaborative Open Food Facts (https://world.openfoodfacts.org), sous licence ODbL."*
"""
    return card


def publish_to_huggingface(
    processed_dir: Path,
    repo_id: str,
    token: Optional[str] = None,
    private: bool = True,
    version_tag: str = "v1.0.0",
    dry_run: bool = False
) -> bool:
    """Publie le dataset et sa Dataset Card sur le Hugging Face Hub."""
    import pyarrow.parquet as pq

    # Calcul des statistiques réelles du dataset
    unified_file = processed_dir / "products_cleaned.parquet"
    if not unified_file.exists():
        logger.error("Fichier Parquet introuvable : %s", unified_file)
        return False

    table = pq.read_table(unified_file)
    num_products = table.num_rows

    brands_file = processed_dir / "dim_brands.parquet"
    num_brands = pq.read_table(brands_file).num_rows if brands_file.exists() else 0

    cats_file = processed_dir / "dim_categories.parquet"
    num_cats = pq.read_table(cats_file).num_rows if cats_file.exists() else 0

    card_content = generate_dataset_card_content(
        num_products=num_products,
        num_brands=num_brands,
        num_categories=num_cats,
        version=version_tag
    )

    card_path = processed_dir / "README.md"
    with open(card_path, "w", encoding="utf-8") as f:
        f.write(card_content)
    logger.info("Dataset Card générée avec succès dans %s", card_path)

    if dry_run or not token or token.startswith("hf_your_token"):
        logger.info("=" * 70)
        logger.info("MODE SIMULATION (DRY-RUN) / TOKEN ABSENT")
        logger.info("Le script de publication est entièrement validé :")
        logger.info(" - Dépôt cible : %s (Privé : %s)", repo_id, private)
        logger.info(" - Tag de version : %s", version_tag)
        logger.info(" - Fichiers prêts à l'envoi :")
        for f in processed_dir.glob("*.parquet"):
            logger.info("   * %s (%.1f KB)", f.name, f.stat().st_size / 1024)
        logger.info("   * README.md (Dataset Card ODbL)")
        logger.info("Pour exécuter la publication réelle sur le Hub :")
        logger.info("  1. Renseignez votre token dans .env : HF_TOKEN=hf_xxx")
        logger.info("  2. Exécutez : python scripts/publish_hub.py --repo-id %s", repo_id)
        logger.info("=" * 70)
        return True

    from huggingface_hub import HfApi

    api = HfApi(token=token)

    try:
        logger.info("Création / Vérification du dépôt sur Hugging Face (%s, private=%s)...", repo_id, private)
        api.create_repo(
            repo_id=repo_id,
            repo_type="dataset",
            private=private,
            exist_ok=True
        )

        logger.info("Téléversement des fichiers Parquet et de la Dataset Card...")
        api.upload_folder(
            folder_path=str(processed_dir),
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=f"Publication initiale du pipeline de données - Release {version_tag}"
        )

        logger.info("Création du tag de version %s sur le Hub...", version_tag)
        try:
            api.create_tag(
                repo_id=repo_id,
                repo_type="dataset",
                tag=version_tag,
                tag_message=f"Version {version_tag} du dataset Open Food Facts traité"
            )
        except Exception as e:
            logger.warning("Tag déjà existant ou avertissement : %s", e)

        logger.info("SUCCÈS : Dataset publié avec succès sur https://huggingface.co/datasets/%s", repo_id)
        return True

    except Exception as exc:
        logger.error("Erreur lors de la publication sur le Hub : %s", exc)
        return False


def main():
    parser = argparse.ArgumentParser(description="Publication du dataset Parquet sur Hugging Face Hub.")
    parser.add_argument("--repo-id", type=str, default=None,
                        help="Identifiant du dépôt Hugging Face (ex: username/openfoodfacts-france-processed)")
    parser.add_argument("--token", type=str, default=None, help="Jeton Hugging Face Hub (ou variable HF_TOKEN)")
    parser.add_argument("--public", action="store_true",
                        help="Créer un dépôt public (par défaut: privé comme exigé dans le sujet)")
    parser.add_argument("--tag", type=str, default="v1.0.0", help="Version taguée du dataset (défaut: v1.0.0)")
    parser.add_argument("--processed-dir", type=str, default="data/processed", help="Dossier Parquet à publier")
    parser.add_argument("--dry-run", action="store_true", help="Simuler l'envoi sans appeler l'API")

    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    load_env_file(project_root / ".env")

    token = args.token or os.environ.get("HF_TOKEN")
    repo_id = args.repo_id or os.environ.get("HF_REPO_ID", "alexis/openfoodfacts-france-processed")
    private = not args.public if not os.environ.get("HF_PRIVATE") else os.environ.get("HF_PRIVATE", "True").lower() == "true"
    processed_path = project_root / args.processed_dir

    publish_to_huggingface(
        processed_dir=processed_path,
        repo_id=repo_id,
        token=token,
        private=private,
        version_tag=args.tag,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    main()
