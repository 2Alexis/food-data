"""
Chargement et Analyse SQL du Pipeline de Données
=================================================
Respecte l'ensemble des attendus du sujet :
- Schéma relationnel strict avec clés primaires, clés étrangères, contraintes CHECK et NOT NULL.
- Chargement idempotent pouvant être relancé indéfiniment sans provoquer de doublons (ON CONFLICT / UPSERT).
- Chargement DuckDB (in-memory ou fichier local). La connexion PostgreSQL / Supabase via DATABASE_URL
  est prévue, mais le chargement des Parquet n'est implémenté que pour DuckDB.
- Exécution automatisée des 5 requêtes d'analyse avancées (Jointures, GROUP BY, CTE, Window Functions).
- Analyse comparative avec EXPLAIN ANALYZE avant / après création d'un index sur le code-barres,
  mesurée sur une table agrandie par réplication (products_volume).
"""

import os
import re
import sys
import time
import statistics
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [sql] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("sql")


def load_env_file(env_path: Path) -> None:
    """Charge manuellement le fichier .env sans dépendance externe obligatoire."""
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))


class SQLDatabaseManager:
    """Interface unifiée pour exécuter les opérations SQL sur DuckDB ou PostgreSQL."""

    def __init__(self, db_type: str = "duckdb", db_path: Optional[str] = None, database_url: Optional[str] = None):
        self.db_type = db_type.lower()
        self.db_path = db_path
        self.database_url = database_url
        self.conn = None
        self._init_connection()

    def _init_connection(self):
        if self.db_type == "duckdb":
            import duckdb
            target = self.db_path if self.db_path else ":memory:"
            self.conn = duckdb.connect(database=target)
            logger.info("Connexion DuckDB établie sur '%s'.", target)
        elif self.db_type == "postgres":
            import psycopg2
            if not self.database_url:
                raise ValueError("DATABASE_URL est requise pour le mode PostgreSQL.")
            self.conn = psycopg2.connect(self.database_url)
            self.conn.autocommit = True
            logger.info("Connexion PostgreSQL établie avec succès.")
        else:
            raise ValueError(f"Type de base inconnu : {self.db_type}")

    def execute(self, query: str, params: Optional[tuple] = None):
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor

    def execute_script(self, script_path: Path):
        with open(script_path, "r", encoding="utf-8") as f:
            content = f.read()
        if self.db_type == "duckdb":
            self.conn.execute(content)
        elif self.db_type == "postgres":
            with self.conn.cursor() as cur:
                cur.execute(content)

    def close(self):
        if self.conn:
            self.conn.close()


def create_schema(db: SQLDatabaseManager, schema_file: Path) -> None:
    """Initialise le schéma de base de données à partir du fichier DDL."""
    logger.info("Initialisation du schéma SQL depuis %s...", schema_file.name)
    db.execute_script(schema_file)
    logger.info("Schéma initialisé avec succès.")


def load_parquet_idempotent(db: SQLDatabaseManager, processed_dir: Path) -> Dict[str, int]:
    """
    Charge les données propres depuis les Parquets de façon idempotente.
    Peut être relancé plusieurs fois sans créer de doublons.
    """
    logger.info("Chargement des tables dimensionnelles et de faits depuis %s...", processed_dir)

    brands_path = processed_dir / "dim_brands.parquet"
    categories_path = processed_dir / "dim_categories.parquet"
    products_path = processed_dir / "dim_products.parquet"
    nutrition_path = processed_dir / "fact_nutrition.parquet"

    counts = {}

    if db.db_type == "duckdb":
        # DuckDB permet un chargement ultra-rapide direct depuis les fichiers Parquet
        # avec déduplication et clauses ON CONFLICT
        db.conn.execute(f"""
            INSERT INTO brands (id, name)
            SELECT id, name FROM read_parquet('{brands_path.as_posix()}')
            ON CONFLICT (id) DO NOTHING;
        """)
        counts["brands"] = db.conn.execute("SELECT COUNT(*) FROM brands").fetchone()[0]

        db.conn.execute(f"""
            INSERT INTO categories (id, name)
            SELECT id, name FROM read_parquet('{categories_path.as_posix()}')
            ON CONFLICT (id) DO NOTHING;
        """)
        counts["categories"] = db.conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]

        db.conn.execute(f"""
            INSERT INTO products (code, product_name, brand_id, category_id, nutriscore_grade, nova_group, ecoscore_grade, created_at, modified_at)
            SELECT code, product_name, brand_id, category_id, nutriscore_grade, nova_group, ecoscore_grade, created_at, modified_at
            FROM read_parquet('{products_path.as_posix()}')
            ON CONFLICT (code) DO NOTHING;
        """)
        counts["products"] = db.conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]

        db.conn.execute(f"""
            INSERT INTO product_nutrition (product_code, energy_kcal, fat_100g, sugars_100g, proteins_100g, salt_100g, sodium_100g)
            SELECT product_code, energy_kcal_100g, fat_100g, sugars_100g, proteins_100g, salt_100g, sodium_100g
            FROM read_parquet('{nutrition_path.as_posix()}')
            ON CONFLICT (product_code) DO NOTHING;
        """)
        counts["product_nutrition"] = db.conn.execute("SELECT COUNT(*) FROM product_nutrition").fetchone()[0]

    logger.info("Chargement idempotent terminé : %s", counts)
    return counts


def run_analytical_queries(db: SQLDatabaseManager, sql_dir: Path, index_volume_factor: int = 1000) -> None:
    """Exécute les 5 requêtes d'analyse et affiche les résultats formatés."""
    query_files = [
        ("Requête 1 - Window Ranking (Top 3 kcal / catégorie)", sql_dir / "01_window_ranking.sql"),
        ("Requête 2 - Fenêtrage & Moyenne mobile (Tendance sucres)", sql_dir / "02_window_moving_avg.sql"),
        ("Requête 3 - Jointures & GROUP BY (Matrice santé des marques)", sql_dir / "03_brands_health_matrix.sql"),
        ("Requête 4 - Multi-CTE (Paradoxe NOVA 4 & Nutriscore A)", sql_dir / "04_ultra_processed_paradox_cte.sql"),
    ]

    print("\n" + "=" * 80)
    print(" EXÉCUTION DES REQUÊTES D'ANALYSE SQL (ATTENDUS DU PROJET)")
    print("=" * 80)

    for title, q_path in query_files:
        print(f"\n>>> {title} ({q_path.name})")
        print("-" * 80)
        with open(q_path, "r", encoding="utf-8") as f:
            sql_text = f.read()

        start = time.perf_counter()
        cursor = db.execute(sql_text)
        duration_ms = (time.perf_counter() - start) * 1000.0
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        print(f"Durée d'exécution : {duration_ms:.2f} ms | Lignes retournées : {len(rows)}")
        # Afficher les 5 premières lignes
        header = " | ".join(col_names[:6])
        print(header)
        print("-" * len(header))
        for r in rows[:5]:
            print(" | ".join(str(val) if val is not None else "NULL" for val in r[:6]))
        if len(rows) > 5:
            print(f"... ({len(rows) - 5} lignes supplémentaires)")

    # Exécution de la Requête 5 avec EXPLAIN ANALYZE (Optimisation & Index)
    print("\n" + "=" * 80)
    print(" REQUÊTE 5 : OPTIMISATION ET JUSTIFICATION D'INDEX (EXPLAIN ANALYZE)")
    print("=" * 80)
    if db.db_type != "duckdb":
        print("Démonstration d'index prévue pour DuckDB : lancer sql/05_optimization_explain_analyze.sql à la main.")
        return
    run_explain_analyze_benchmark(db, factor=index_volume_factor)


# ----------------------------------------------------------------------------
# Démonstration d'index (requête 5)
# ----------------------------------------------------------------------------
# Constat de départ : avec 1 447 produits, DuckDB lit toute la table en moins d'une
# milliseconde et ses index ART ne servent pas aux filtres IN ou par plage. Les
# anciens index composites (nutriscore_grade, sugars_100g...) ne changeaient pas
# le plan. On mesure donc l'index sur le cas qu'il sert vraiment : la recherche
# d'un produit par code-barres, sur une table agrandie par réplication.

INDEX_DEMO_FACTOR = 1000


def build_volume_table(db: SQLDatabaseManager, factor: int = INDEX_DEMO_FACTOR) -> int:
    """Crée products_volume : chaque produit réel répliqué `factor` fois avec un code
    suffixé, lignes mélangées (sinon DuckDB élague déjà les blocs grâce à l'ordre)."""
    if not 1 <= factor <= 9999:
        raise ValueError("factor doit être compris entre 1 et 9999")
    db.execute("DROP TABLE IF EXISTS products_volume;")
    db.execute(f"""
        CREATE TABLE products_volume AS
        SELECT p.code || lpad(k::VARCHAR, 4, '0') AS code,
               p.product_name, p.brand_id, p.category_id, p.nutriscore_grade, p.nova_group
        FROM products p, range({int(factor)}) AS t(k)
        ORDER BY hash(p.code, k);
    """)
    return db.execute("SELECT COUNT(*) FROM products_volume").fetchone()[0]


def index_demo_query(db: SQLDatabaseManager, factor: int) -> str:
    """Recherche d'un produit par code-barres, sur un code réel choisi de façon déterministe."""
    n = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    base = db.execute(f"SELECT code FROM products ORDER BY code LIMIT 1 OFFSET {n // 2}").fetchone()[0]
    target = base + str(factor // 2).zfill(4)
    return f"""
    SELECT code, product_name, nutriscore_grade, nova_group
    FROM products_volume
    WHERE code = '{target}';
    """


def explain_profile(db: SQLDatabaseManager, query: str) -> Dict[str, Any]:
    """Lance EXPLAIN ANALYZE (DuckDB) et extrait le type de scan, les blocs lus et le temps moteur."""
    text = db.execute("EXPLAIN ANALYZE " + query).fetchall()[0][1]
    total = re.search(r"Total Time: ([\d.]+)s", text)
    return {
        "plan": text,
        "scan_types": re.findall(r"Type: (Sequential Scan|Index Scan)", text),
        "row_groups": re.findall(r"Row Groups Scanned: (\d+ / \d+)", text),
        "engine_ms": float(total.group(1)) * 1000.0 if total else None,
    }


def median_ms(db: SQLDatabaseManager, query: str, runs: int = 30) -> float:
    """Temps médian de bout en bout (appel Python compris) sur `runs` exécutions, après un essai à blanc."""
    db.execute(query).fetchall()
    samples = []
    for _ in range(runs):
        t0 = time.perf_counter()
        db.execute(query).fetchall()
        samples.append((time.perf_counter() - t0) * 1000.0)
    return statistics.median(samples)


def run_explain_analyze_benchmark(
    db: SQLDatabaseManager,
    factor: int = INDEX_DEMO_FACTOR,
    runs: int = 30,
    verbose: bool = True,
) -> Dict[str, Any]:
    """Mesure le gain d'un index sur products_volume(code) avant / après, via EXPLAIN ANALYZE."""
    t0 = time.perf_counter()
    rows = build_volume_table(db, factor)
    build_ms = (time.perf_counter() - t0) * 1000.0
    query = index_demo_query(db, factor)

    # Médiane d'abord (elle fait un essai à blanc), puis EXPLAIN ANALYZE à chaud
    before_median = median_ms(db, query, runs)
    before = explain_profile(db, query)
    before["median_ms"] = before_median

    t0 = time.perf_counter()
    db.execute("CREATE INDEX idx_products_volume_code ON products_volume (code);")
    index_ms = (time.perf_counter() - t0) * 1000.0

    after_median = median_ms(db, query, runs)
    after = explain_profile(db, query)
    after["median_ms"] = after_median
    result_rows = db.execute(query).fetchall()

    db.execute("DROP TABLE IF EXISTS products_volume;")

    summary = {
        "rows": rows,
        "factor": factor,
        "build_ms": build_ms,
        "index_ms": index_ms,
        "before": before,
        "after": after,
        "result_rows": len(result_rows),
        "gain_engine": (before["engine_ms"] / after["engine_ms"]) if before["engine_ms"] and after["engine_ms"] else None,
        "gain_median": before["median_ms"] / max(after["median_ms"], 1e-6),
    }

    if verbose:
        rows_txt = f"{rows:,}".replace(",", " ")
        print(f"\nTable products_volume : {rows_txt} lignes ({factor} copies mélangées des produits réels), "
              f"créée en {build_ms:.0f} ms")
        print("Requête : recherche d'un produit par code-barres (WHERE code = ...)")
        for label, prof in (("SANS index", before), ("AVEC index", after)):
            print(f"\n[{label}] Scan : {', '.join(prof['scan_types']) or '?'} | "
                  f"blocs lus : {', '.join(prof['row_groups']) or '?'} | "
                  f"temps moteur (EXPLAIN ANALYZE) : {prof['engine_ms']:.1f} ms | "
                  f"médiane sur {runs} exécutions : {prof['median_ms']:.2f} ms")
        print(f"\nCréation de idx_products_volume_code : {index_ms:.0f} ms")
        print(f"[SYNTHÈSE] Gain moteur x{summary['gain_engine']:.1f} | gain de bout en bout x{summary['gain_median']:.1f} "
              f"| {len(result_rows)} ligne(s) retournée(s)")
        print("=" * 80 + "\n")
    return summary


def main():
    parser = argparse.ArgumentParser(description="Chargement et exécution SQL pour le projet Open Food Facts.")
    parser.add_argument("--db-type", choices=["duckdb", "postgres"], default="duckdb",
                        help="Moteur SQL : duckdb (défaut, zéro config) ou postgres")
    parser.add_argument("--db-path", type=str, default="data/food_database.duckdb",
                        help="Chemin du fichier DuckDB")
    parser.add_argument("--processed-dir", type=str, default="data/processed",
                        help="Dossier contenant les fichiers Parquet propres")
    parser.add_argument("--run-queries", action="store_true", default=True,
                        help="Exécuter automatiquement les requêtes d'analyse")
    parser.add_argument("--idempotent-test", action="store_true",
                        help="Tester le rechargement consécutif pour prouver l'idempotence")
    parser.add_argument("--index-volume-factor", type=int, default=INDEX_DEMO_FACTOR,
                        help="Nombre de copies des produits pour la démonstration d'index (défaut : 1000)")

    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    load_env_file(project_root / ".env")

    database_url = os.environ.get("DATABASE_URL")
    processed_path = project_root / args.processed_dir
    schema_path = project_root / "sql" / "00_schema.sql"
    sql_path = project_root / "sql"

    db_path = str(project_root / args.db_path) if args.db_type == "duckdb" else None
    db = SQLDatabaseManager(db_type=args.db_type, db_path=db_path, database_url=database_url)

    try:
        # 1. Création schéma
        create_schema(db, schema_path)

        # 2. Chargement initial
        counts_1 = load_parquet_idempotent(db, processed_path)

        # 3. Test d'idempotence si demandé
        if args.idempotent_test:
            logger.info("Test d'idempotence : deuxième chargement sans doublons...")
            counts_2 = load_parquet_idempotent(db, processed_path)
            assert counts_1 == counts_2, f"Échec d'idempotence : {counts_1} != {counts_2}"
            logger.info("Validation réussie : exactitude des comptages confirmée !")

        # 4. Requêtes d'analyse & optimisation
        if args.run_queries:
            run_analytical_queries(db, sql_path, index_volume_factor=args.index_volume_factor)

    finally:
        db.close()


if __name__ == "__main__":
    main()
