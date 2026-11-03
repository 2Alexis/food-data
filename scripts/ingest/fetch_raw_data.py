"""
Pipeline d'ingestion asynchrone Open Food Facts
================================================
Respecte l'ensemble des attendus du sujet de projet :
- Collecte asynchrone via `aiohttp`
- Concurrence limitée par un `asyncio.Semaphore`
- Gestion des timeouts stricts (`aiohttp.ClientTimeout`)
- Nouvelles tentatives avec attente exponentielle (exponential backoff + jitter)
- Écriture au format JSON Lines avec nommage déterministe
- Mécanisme de reprise après interruption (checkpoint persistant par tâche / fichier)
- Support de la pagination multi-catégories pour contourner la limite de profondeur d'offset de l'API publique
"""

import os
import sys
import json
import random
import asyncio
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [ingest] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ingest")

API_SEARCH_URL = "https://world.openfoodfacts.org/api/v2/search"
DEFAULT_USER_AGENT = "DataPipelineYnov/1.0 (contact: student@ynov.com; school: Ynov Campus)"

API_FIELDS = [
    "code",
    "product_name",
    "generic_name",
    "brands",
    "categories",
    "categories_hierarchy",
    "countries",
    "nutriscore_grade",
    "nova_group",
    "ecoscore_grade",
    "energy-kcal_100g",
    "fat_100g",
    "sugars_100g",
    "proteins_100g",
    "salt_100g",
    "sodium_100g",
    "completeness",
    "created_t",
    "last_modified_t"
]

DEFAULT_CATEGORIES = [
    "beverages",
    "dairies",
    "snacks",
    "groceries",
    "meats",
    "desserts",
    "plant-based-foods-and-beverages",
    "cereals-and-potatoes"
]


class CheckpointManager:
    """Gère l'état d'avancement pour permettre une reprise déterministe après coupure réseau ou arrêt du script."""

    def __init__(self, checkpoint_path: Path):
        self.checkpoint_path = checkpoint_path
        self._completed_tasks = set()
        self.load()

    def load(self) -> None:
        if self.checkpoint_path.exists():
            try:
                with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._completed_tasks = set(data.get("completed_tasks", []))
                    # Rétrocompatibilité avec les versions à pages simples
                    for p in data.get("completed_pages", []):
                        self._completed_tasks.add(f"page_{p:04d}")
                    logger.info("Checkpoint chargé : %d tâche(s) déjà terminée(s).", len(self._completed_tasks))
            except Exception as e:
                logger.warning("Erreur lors de la lecture du checkpoint (%s), réinitialisation.", e)
                self._completed_tasks = set()
        else:
            self._completed_tasks = set()

    def is_completed(self, task_key: str, target_file: Path) -> bool:
        return task_key in self._completed_tasks and target_file.exists() and target_file.stat().st_size > 0

    def mark_completed(self, task_key: str) -> None:
        self._completed_tasks.add(task_key)
        self.save()

    def save(self) -> None:
        temp_file = self.checkpoint_path.with_suffix(".tmp")
        data = {
            "completed_tasks": sorted(list(self._completed_tasks)),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "total_completed": len(self._completed_tasks)
        }
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        temp_file.replace(self.checkpoint_path)

    def reset(self) -> None:
        self._completed_tasks.clear()
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()
        logger.info("Checkpoint réinitialisé.")


async def fetch_page_with_backoff(
    session: aiohttp.ClientSession,
    page: int,
    page_size: int,
    category: Optional[str],
    semaphore: asyncio.Semaphore,
    max_retries: int = 4,
    base_backoff: float = 1.2
) -> Optional[Dict[str, Any]]:
    """
    Récupère une page de l'API avec limitation de concurrence (semaphore) et backoff exponentiel.
    """
    params = {
        "fields": ",".join(API_FIELDS),
        "page_size": str(page_size),
        "page": str(page),
        "json": "1"
    }
    if category:
        params["categories_tags_en"] = category

    async with semaphore:
        for attempt in range(1, max_retries + 1):
            try:
                # Ajout d'un petit délai de courtoisie pour respecter les quotas de l'API publique
                await asyncio.sleep(0.3)
                async with session.get(API_SEARCH_URL, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data

                    if response.status in (429, 500, 502, 503, 504):
                        delay = base_backoff * (2 ** (attempt - 1)) + random.uniform(0.1, 0.6)
                        cat_str = f" [cat={category}]" if category else ""
                        logger.warning(
                            "Page %d%s : HTTP %d. Tentative %d/%d -> nouvel essai dans %.2fs...",
                            page, cat_str, response.status, attempt, max_retries, delay
                        )
                        await asyncio.sleep(delay)
                    else:
                        cat_str = f" [cat={category}]" if category else ""
                        logger.error("Page %d%s : Statut HTTP inattendu %d", page, cat_str, response.status)
                        return None

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                delay = base_backoff * (2 ** (attempt - 1)) + random.uniform(0.1, 0.6)
                cat_str = f" [cat={category}]" if category else ""
                logger.warning(
                    "Page %d%s : Erreur réseau %s. Tentative %d/%d -> nouvel essai dans %.2fs...",
                    page, cat_str, type(exc).__name__, attempt, max_retries, delay
                )
                await asyncio.sleep(delay)
            except Exception as e:
                logger.error("Page %d : Erreur critique non gérée : %s", page, e)
                return None

        logger.error("Page %d : Abandon définitif après %d tentatives infructueuses.", page, max_retries)
        return None


def write_json_lines(file_path: Path, items: List[Dict[str, Any]]) -> int:
    """Écrit les produits au format JSON Lines de manière atomique (fichier temporaire puis renommage)."""
    temp_path = file_path.with_suffix(".tmp")
    count = 0
    with open(temp_path, "w", encoding="utf-8") as f:
        for item in items:
            line = json.dumps(item, ensure_ascii=False)
            f.write(line + "\n")
            count += 1
    temp_path.replace(file_path)
    return count


async def ingest_pipeline(
    raw_dir: Path,
    categories: Optional[List[str]] = None,
    pages_per_category: int = 3,
    start_page: int = 1,
    page_size: int = 100,
    concurrency: int = 3,
    timeout_sec: int = 30,
    max_retries: int = 4,
    reset_checkpoint: bool = False
) -> Dict[str, Any]:
    """Orchestre la collecte globale asynchrone et résiliente."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_file = raw_dir / "checkpoint.json"
    checkpoint_mgr = CheckpointManager(checkpoint_file)

    if reset_checkpoint:
        checkpoint_mgr.reset()

    timeout = aiohttp.ClientTimeout(total=timeout_sec, connect=10.0)
    semaphore = asyncio.Semaphore(concurrency)
    headers = {"User-Agent": DEFAULT_USER_AGENT}

    # Préparation des tâches planifiées
    tasks_to_run = []
    if categories:
        for cat in categories:
            for p in range(start_page, start_page + pages_per_category):
                task_key = f"{cat}_page_{p:04d}"
                target_file = raw_dir / f"products_{cat}_page_{p:04d}.jsonl"
                if checkpoint_mgr.is_completed(task_key, target_file):
                    logger.info("Tâche %s déjà complétée, ignorée.", task_key)
                else:
                    tasks_to_run.append((cat, p, task_key, target_file))
    else:
        for p in range(start_page, start_page + pages_per_category):
            task_key = f"page_{p:04d}"
            target_file = raw_dir / f"products_page_{p:04d}.jsonl"
            if checkpoint_mgr.is_completed(task_key, target_file):
                logger.info("Tâche %s déjà complétée, ignorée.", task_key)
            else:
                tasks_to_run.append((None, p, task_key, target_file))

    logger.info("Ingestion asynchrone démarrée : %d tâche(s) planifiée(s).", len(tasks_to_run))

    total_records = 0
    succeeded = 0
    failed = 0

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        async def execute_task(cat: Optional[str], page_num: int, task_key: str, target_file: Path):
            nonlocal total_records, succeeded, failed
            data = await fetch_page_with_backoff(
                session=session,
                page=page_num,
                page_size=page_size,
                category=cat,
                semaphore=semaphore,
                max_retries=max_retries
            )

            if data and "products" in data:
                prods = data["products"]
                nb_written = write_json_lines(target_file, prods)
                checkpoint_mgr.mark_completed(task_key)
                succeeded += 1
                total_records += nb_written
                logger.info(
                    "[%s] Succès : %d produits stockés dans %s",
                    task_key, nb_written, target_file.name
                )
            else:
                failed += 1
                logger.error("[%s] Échec persistant", task_key)

        tasks = [execute_task(cat, p, key, f) for cat, p, key, f in tasks_to_run]
        if tasks:
            await asyncio.gather(*tasks)

    summary = {
        "tasks_planned": len(tasks_to_run),
        "tasks_succeeded": succeeded,
        "tasks_failed": failed,
        "total_records_ingested": total_records,
        "raw_dir": str(raw_dir)
    }
    logger.info("Collecte terminée : %d nouveaux enregistrements enregistrés.", total_records)
    return summary


def main():
    parser = argparse.ArgumentParser(description="Ingestion asynchrone de données réelles Open Food Facts.")
    parser.add_argument("--categories", nargs="*", default=None,
                        help="Liste des catégories en anglais (ex: snacks beverages dairies). Par défaut: pagination globale.")
    parser.add_argument("--pages-per-cat", type=int, default=3, help="Pages à récupérer par catégorie (défaut: 3)")
    parser.add_argument("--start-page", type=int, default=1, help="Page de départ (défaut: 1)")
    parser.add_argument("--page-size", type=int, default=100, help="Produits par page (défaut: 100)")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrence max (défaut: 3)")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout en secondes (défaut: 30)")
    parser.add_argument("--retries", type=int, default=4, help="Nombre max de retries (défaut: 4)")
    parser.add_argument("--raw-dir", type=str, default="data/raw", help="Dossier de stockage JSON Lines")
    parser.add_argument("--reset-checkpoint", action="store_true", help="Réinitialiser l'état du checkpoint")
    parser.add_argument("--preset-diverse", action="store_true",
                        help="Ingérer automatiquement un jeu diversifié de 8 grandes catégories alimentaires")

    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent.parent
    raw_path = project_root / args.raw_dir

    cats = None
    if args.preset_diverse:
        cats = DEFAULT_CATEGORIES
    elif args.categories:
        cats = args.categories

    asyncio.run(
        ingest_pipeline(
            raw_dir=raw_path,
            categories=cats,
            pages_per_category=args.pages_per_cat,
            start_page=args.start_page,
            page_size=args.page_size,
            concurrency=args.concurrency,
            timeout_sec=args.timeout,
            max_retries=args.retries,
            reset_checkpoint=args.reset_checkpoint
        )
    )


if __name__ == "__main__":
    main()
