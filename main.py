"""
Orchestrateur Principal du Pipeline de Données Open Food Facts
==============================================================
Point d'entrée unique permettant de piloter l'ensemble du cycle de vie des données :
  1. Ingestion asynchrone (aiohttp, sémaphore, backoff exponentiel, checkpoint)
  2. Transformation et streaming par lots (nettoyage, typage strict, Parquet)
  3. Chargement et requêtes SQL (PostgreSQL / DuckDB, modélisation relationnelle, idempotence, EXPLAIN ANALYZE)
  4. Publication sur le Hugging Face Hub (dépôt privé, version v1.0.0, Dataset Card ODbL)
  5. Benchmark et mesures réelles (durées, RAM, compression, gain index)
  6. Suite de tests automatisés pytest
"""

import os
import sys
import argparse
import subprocess
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [main] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("main")
PROJECT_ROOT = Path(__file__).resolve().parent


def run_command(cmd_args: list, desc: str) -> bool:
    logger.info(">>> Début de l'étape : %s", desc)
    logger.info("Commande : %s", " ".join(cmd_args))
    res = subprocess.run(cmd_args, cwd=PROJECT_ROOT)
    if res.returncode != 0:
        logger.error("Échec de l'étape '%s' (code de sortie %d)", desc, res.returncode)
        return False
    logger.info("Étape '%s' terminée avec succès.\n", desc)
    return True


def main():
    parser = argparse.ArgumentParser(description="Orchestrateur du Pipeline de Données Open Food Facts.")
    parser.add_argument("--all", action="store_true", help="Exécuter l'ensemble du pipeline de bout en bout")
    parser.add_argument("--ingest", action="store_true", help="Lancer l'ingestion asynchrone")
    parser.add_argument("--transform", action="store_true", help="Lancer la transformation par lots vers Parquet")
    parser.add_argument("--load-sql", action="store_true", help="Charger les données en SQL et exécuter les analyses")
    parser.add_argument("--publish", action="store_true", help="Générer la Dataset Card et préparer la publication Hub")
    parser.add_argument("--benchmark", action="store_true", help="Calculer les métriques chiffrées du pipeline")
    parser.add_argument("--test", action="store_true", help="Lancer la suite de tests pytest")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Mode simulation pour la publication Hub")

    args = parser.parse_args()

    # Si aucun argument spécifié, afficher l'aide
    if not any([args.all, args.ingest, args.transform, args.load_sql, args.publish, args.benchmark, args.test]):
        parser.print_help()
        sys.exit(0)

    py_exe = sys.executable

    # 1. Tests
    if args.all or args.test:
        ok = run_command([py_exe, "-m", "pytest", "-v"], "Exécution de la suite de tests pytest")
        if not ok and not args.all:
            sys.exit(1)

    # 2. Ingestion
    if args.all or args.ingest:
        ok = run_command([py_exe, "scripts/ingest/fetch_raw_data.py", "--pages", "5", "--page-size", "100"], "Ingestion asynchrone Open Food Facts")
        if not ok and not args.all:
            sys.exit(1)

    # 3. Transformation
    if args.all or args.transform:
        ok = run_command([py_exe, "scripts/transform/clean_and_transform.py", "--chunk-size", "500"], "Transformation par lots (Parquet)")
        if not ok and not args.all:
            sys.exit(1)

    # 4. SQL
    if args.all or args.load_sql:
        ok = run_command([py_exe, "scripts/load_sql.py", "--idempotent-test", "--run-queries"], "Chargement idempotent et analyses SQL")
        if not ok and not args.all:
            sys.exit(1)

    # 5. Benchmark
    if args.all or args.benchmark:
        ok = run_command([py_exe, "scripts/benchmark_pipeline.py"], "Benchmark des performances et mesures réelles")
        if not ok and not args.all:
            sys.exit(1)

    # 6. Publication
    if args.all or args.publish:
        pub_cmd = [py_exe, "scripts/publish_hub.py"]
        if args.dry_run:
            pub_cmd.append("--dry-run")
        ok = run_command(pub_cmd, "Génération de la Dataset Card et Publication Hub")
        if not ok and not args.all:
            sys.exit(1)

    logger.info("=" * 70)
    logger.info(" PIPELINE EXÉCUTÉ AVEC SUCCÈS !")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
