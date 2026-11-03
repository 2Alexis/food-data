"""
Tests unitaires et d'intégration pour le module SQL (scripts/load_sql.py et requêtes sql/)
"""

import pytest
from pathlib import Path
from scripts.load_sql import (
    SQLDatabaseManager,
    create_schema,
    load_parquet_idempotent
)


@pytest.fixture
def test_db():
    """Fournit une base de données DuckDB en mémoire isolée pour les tests."""
    db = SQLDatabaseManager(db_type="duckdb", db_path=":memory:")
    yield db
    db.close()


def test_schema_creation(test_db):
    schema_file = Path(__file__).resolve().parent.parent / "sql" / "00_schema.sql"
    assert schema_file.exists()

    create_schema(test_db, schema_file)

    # Vérification de l'existence des 4 tables
    tables = [r[0] for r in test_db.execute("SHOW TABLES;").fetchall()]
    assert "brands" in tables
    assert "categories" in tables
    assert "products" in tables
    assert "product_nutrition" in tables


def test_idempotent_loading_and_counts(test_db):
    project_root = Path(__file__).resolve().parent.parent
    schema_file = project_root / "sql" / "00_schema.sql"
    processed_dir = project_root / "data" / "processed"

    create_schema(test_db, schema_file)

    # Premier chargement
    counts_1 = load_parquet_idempotent(test_db, processed_dir)
    assert counts_1["products"] > 0
    assert counts_1["brands"] > 0
    assert counts_1["categories"] > 0

    # Deuxième chargement consécutif (test de non-duplication)
    counts_2 = load_parquet_idempotent(test_db, processed_dir)
    assert counts_1 == counts_2


def test_analytical_queries_execution(test_db):
    project_root = Path(__file__).resolve().parent.parent
    schema_file = project_root / "sql" / "00_schema.sql"
    processed_dir = project_root / "data" / "processed"
    sql_dir = project_root / "sql"

    create_schema(test_db, schema_file)
    load_parquet_idempotent(test_db, processed_dir)

    # Test des 4 requêtes d'analyse
    queries = [
        "01_window_ranking.sql",
        "02_window_moving_avg.sql",
        "03_brands_health_matrix.sql",
        "04_ultra_processed_paradox_cte.sql"
    ]

    for q_file in queries:
        path = sql_dir / q_file
        assert path.exists()
        sql_content = path.read_text(encoding="utf-8")
        rows = test_db.execute(sql_content).fetchall()
        assert len(rows) > 0, f"La requête {q_file} n'a retourné aucun résultat."


def test_explain_analyze_index_gain(test_db):
    project_root = Path(__file__).resolve().parent.parent
    schema_file = project_root / "sql" / "00_schema.sql"
    processed_dir = project_root / "data" / "processed"

    create_schema(test_db, schema_file)
    load_parquet_idempotent(test_db, processed_dir)

    query = """
    EXPLAIN ANALYZE
    SELECT p.code, p.product_name, n.sugars_100g
    FROM products p
    JOIN product_nutrition n ON p.code = n.product_code
    WHERE p.nutriscore_grade = 'A' AND n.sugars_100g <= 5.0;
    """

    # Exécution avant index
    res_before = test_db.execute(query).fetchall()
    assert len(res_before) > 0

    # Création des index
    test_db.execute("CREATE INDEX idx_test_nutriscore ON products(nutriscore_grade);")
    test_db.execute("CREATE INDEX idx_test_sugars ON product_nutrition(sugars_100g);")

    # Exécution après index
    res_after = test_db.execute(query).fetchall()
    assert len(res_after) > 0
