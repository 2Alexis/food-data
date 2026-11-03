"""
Tests unitaires pour le module de transformation et streaming (scripts/transform/clean_and_transform.py)
"""

import json
import pytest
from pathlib import Path
import pyarrow.parquet as pq

from scripts.transform.clean_and_transform import (
    clean_text,
    extract_primary_brand,
    extract_primary_category,
    normalize_grade,
    normalize_nova,
    clean_numeric_bounded,
    timestamp_to_datetime,
    clean_record,
    stream_json_lines_chunks,
    transform_pipeline
)


def test_clean_text():
    assert clean_text("  Eau Minérale , ") == "Eau Minérale"
    assert clean_text("", default="Inconnu") == "Inconnu"
    assert clean_text(None, default="Def") == "Def"


def test_extract_primary_brand():
    assert extract_primary_brand("Danone, Lu, Nestlé") == "Danone"
    assert extract_primary_brand("coca-cola") == "Coca-Cola"
    assert extract_primary_brand(None) == "Marque Inconnue"


def test_extract_primary_category():
    assert extract_primary_category("Boissons, Sodas", None) == "Boissons"
    assert extract_primary_category(None, ["en:beverages", "en:sodas"]) == "Beverages"
    assert extract_primary_category(None, None) == "Non Classifié"


def test_normalize_grade():
    assert normalize_grade("a") == "A"
    assert normalize_grade("E") == "E"
    assert normalize_grade("f") == "UNKNOWN"
    assert normalize_grade(None) == "UNKNOWN"


def test_normalize_nova():
    assert normalize_nova("1") == 1
    assert normalize_nova(4.0) == 4
    assert normalize_nova(5) is None
    assert normalize_nova("invalide") is None


def test_clean_numeric_bounded():
    assert clean_numeric_bounded("12.546", 0.0, 100.0) == 12.55
    assert clean_numeric_bounded("-5.0", 0.0, 100.0) is None
    assert clean_numeric_bounded("150.0", 0.0, 100.0) is None
    assert clean_numeric_bounded("abc", 0.0, 100.0) is None


def test_clean_record_valid():
    raw = {
        "code": "3017620422003",
        "product_name": "Nutella",
        "brands": "Ferrero",
        "categories": "Pâtes à tartiner",
        "nutriscore_grade": "e",
        "nova_group": 4,
        "energy-kcal_100g": 539.0,
        "sugars_100g": 56.3,
        "fat_100g": 30.9,
        "proteins_100g": 6.3,
        "salt_100g": 0.1,
        "created_t": 1600000000
    }
    cleaned = clean_record(raw)
    assert cleaned is not None
    assert cleaned["code"] == "3017620422003"
    assert cleaned["product_name"] == "Nutella"
    assert cleaned["brand"] == "Ferrero"
    assert cleaned["nutriscore_grade"] == "E"
    assert cleaned["nova_group"] == 4
    assert cleaned["energy_kcal_100g"] == 539.0
    assert cleaned["sugars_100g"] == 56.3
    assert cleaned["created_at"] is not None


def test_clean_record_invalid_barcode():
    # Code-barres vide ou non alphanumérique
    assert clean_record({"code": "", "product_name": "Test"}) is None
    assert clean_record({"code": "!!!---", "product_name": "Test"}) is None


def test_stream_chunks_and_deduplication(tmp_path):
    f1 = tmp_path / "raw_1.jsonl"
    f2 = tmp_path / "raw_2.jsonl"

    f1.write_text('{"code": "A100", "product_name": "Prod 1"}\n{"code": "A200", "product_name": "Prod 2"}\n', encoding="utf-8")
    # Doublon de A100 dans f2
    f2.write_text('{"code": "A100", "product_name": "Prod 1 Bis"}\n{"code": "A300", "product_name": "Prod 3"}\n', encoding="utf-8")

    chunks = list(stream_json_lines_chunks([f1, f2], chunk_size=2))
    all_codes = [item["code"] for ch in chunks for item in ch]

    # Seulement 3 produits uniques
    assert len(all_codes) == 3
    assert set(all_codes) == {"A100", "A200", "A300"}
