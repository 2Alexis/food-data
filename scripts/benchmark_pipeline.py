"""
Script de Mesure et Benchmark du Pipeline de Données
=====================================================
Génère les métriques chiffrées demandées pour la soutenance finale (10 min) :
- Volumes disques (JSON Lines brut vs Parquet compressé Snappy) et taux de réduction
- Durée et pic mémoire de la transformation réelle (tracemalloc, sortie dans un dossier temporaire)
- Durée du chargement SQL et de sa relance (idempotence)
- Gain d'un index validé par EXPLAIN ANALYZE (plan, blocs lus, temps moteur)
"""

import sys
import time
import tempfile
import tracemalloc
from pathlib import Path

# Inclusion du chemin racine
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from scripts.load_sql import (  # noqa: E402
    SQLDatabaseManager,
    create_schema,
    load_parquet_idempotent,
    run_explain_analyze_benchmark,
)
from scripts.transform.clean_and_transform import transform_pipeline  # noqa: E402


def get_dir_size(path: Path, pattern: str = "*") -> int:
    return sum(f.stat().st_size for f in path.glob(pattern) if f.is_file())


def fmt_int(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def run_benchmark():
    raw_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "processed"
    schema_path = project_root / "sql" / "00_schema.sql"

    print("\n" + "=" * 80)
    print(" MESURES ET BENCHMARK DU PIPELINE (CHIFFRES POUR LA PRÉSENTATION)")
    print("=" * 80)

    # 1. Volumes
    raw_size = get_dir_size(raw_dir, "*.jsonl")
    raw_count = len(list(raw_dir.glob("*.jsonl")))
    raw_lines = sum(1 for f in raw_dir.glob("*.jsonl") for _ in open(f, encoding="utf-8"))
    parquet_size = get_dir_size(processed_dir, "*.parquet")
    parquet_count = len(list(processed_dir.glob("*.parquet")))
    unified_size = (processed_dir / "products_cleaned.parquet").stat().st_size
    reduction = (1.0 - parquet_size / max(raw_size, 1)) * 100.0

    print("\n1. VOLUMÉTRIE :")
    print(f"   - JSON Lines bruts : {raw_count} fichiers, {fmt_int(raw_lines)} lignes, {raw_size / 1024**2:.2f} Mo")
    print(f"   - Parquet (5 tables) : {parquet_count} fichiers, {parquet_size / 1024:.0f} Ko "
          f"(table unifiée seule : {unified_size / 1024:.0f} Ko)")
    print(f"   - Réduction : {reduction:.1f} % (Snappy + déduplication + colonnes non conservées)")

    # 2. Transformation réelle, écrite dans un dossier temporaire
    print("\n2. TRANSFORMATION (exécution réelle, sortie temporaire) :")
    with tempfile.TemporaryDirectory() as tmp:
        tracemalloc.start()
        t0 = time.perf_counter()
        stats = transform_pipeline(raw_dir, Path(tmp), chunk_size=500)
        duration_s = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    dup = raw_lines - stats["unique_products"]
    print(f"   - Durée : {duration_s:.2f} s, {stats['total_chunks_streamed']} lots de 500")
    print(f"   - Pic mémoire Python (tracemalloc) : {peak / 1024**2:.1f} Mo")
    print(f"   - {fmt_int(raw_lines)} lignes -> {fmt_int(stats['unique_products'])} produits uniques "
          f"({fmt_int(dup)} doublons, {100 * dup / max(raw_lines, 1):.1f} %), "
          f"{stats['unique_brands']} marques, {stats['unique_categories']} catégories")
    print("   - Note : la table unifiée est écrite lot par lot ; les tables produits/nutrition")
    print("     et l'ensemble des codes vus restent en mémoire jusqu'à la fin (croissance linéaire).")

    # 3. Chargement SQL
    print("\n3. CHARGEMENT SQL (DuckDB en mémoire) :")
    db = SQLDatabaseManager(db_type="duckdb", db_path=":memory:")
    create_schema(db, schema_path)
    t0 = time.perf_counter()
    counts_1 = load_parquet_idempotent(db, processed_dir)
    load_ms = (time.perf_counter() - t0) * 1000.0
    t0 = time.perf_counter()
    counts_2 = load_parquet_idempotent(db, processed_dir)
    reload_ms = (time.perf_counter() - t0) * 1000.0
    print(f"   - Premier chargement : {load_ms:.0f} ms | relance : {reload_ms:.0f} ms | "
          f"comptages identiques : {counts_1 == counts_2} {counts_1}")

    # 4. Index
    print("\n4. GAIN D'INDEX (EXPLAIN ANALYZE, recherche par code-barres) :")
    r = run_explain_analyze_benchmark(db, verbose=False)
    b, a = r["before"], r["after"]
    print(f"   - Volume de test : {fmt_int(r['rows'])} lignes ({r['factor']} copies mélangées des produits réels)")
    print(f"   - SANS index : {', '.join(b['scan_types'])}, blocs lus {', '.join(b['row_groups'])}, "
          f"moteur {b['engine_ms']:.1f} ms, médiane {b['median_ms']:.2f} ms")
    print(f"   - AVEC index : {', '.join(a['scan_types'])}, blocs lus {', '.join(a['row_groups'])}, "
          f"moteur {a['engine_ms']:.1f} ms, médiane {a['median_ms']:.2f} ms")
    print(f"   - Création de l'index : {r['index_ms']:.0f} ms")
    print(f"   - Gain : x{r['gain_engine']:.1f} (moteur), x{r['gain_median']:.1f} (de bout en bout)")
    db.close()

    print("\n" + "=" * 80)
    print(" Tous les chiffres mesurés sont prêts pour les diapositives de la soutenance.")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    run_benchmark()
