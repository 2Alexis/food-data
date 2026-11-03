"""
Tests unitaires pour le module d'ingestion asynchrone (scripts/ingest/fetch_raw_data.py)
"""

import json
import pytest
from pathlib import Path
from scripts.ingest.fetch_raw_data import (
    CheckpointManager,
    write_json_lines,
    API_FIELDS
)


def test_api_fields_not_empty():
    assert len(API_FIELDS) >= 10
    assert "code" in API_FIELDS
    assert "product_name" in API_FIELDS
    assert "nutriscore_grade" in API_FIELDS


def test_checkpoint_lifecycle(tmp_path):
    cp_file = tmp_path / "test_checkpoint.json"
    mgr = CheckpointManager(cp_file)

    # Initialement vide
    assert len(mgr._completed_tasks) == 0

    # Création d'un faux fichier cible
    target_file = tmp_path / "products_page_0001.jsonl"
    target_file.write_text('{"code": "123"}\n', encoding="utf-8")

    assert not mgr.is_completed("task_1", target_file)

    # Marquer comme complété
    mgr.mark_completed("task_1")
    assert mgr.is_completed("task_1", target_file)

    # Recharger un nouveau gestionnaire sur le même fichier
    mgr2 = CheckpointManager(cp_file)
    assert mgr2.is_completed("task_1", target_file)

    # Réinitialisation
    mgr2.reset()
    assert not mgr2.is_completed("task_1", target_file)
    assert not cp_file.exists()


def test_write_json_lines(tmp_path):
    out_file = tmp_path / "test_output.jsonl"
    sample_data = [
        {"code": "111", "name": "Pomme"},
        {"code": "222", "name": "Poire"}
    ]

    count = write_json_lines(out_file, sample_data)
    assert count == 2
    assert out_file.exists()

    lines = out_file.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    obj1 = json.loads(lines[0])
    assert obj1["code"] == "111"
    assert obj1["name"] == "Pomme"
