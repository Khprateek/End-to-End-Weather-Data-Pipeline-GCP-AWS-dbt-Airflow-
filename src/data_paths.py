"""
Shared data lake path constants.

Layout (local, S3, and GCS use the same key prefix):

    {LOCAL_OUTPUT_DIR}/raw/owm/{endpoint}/city={city}/year=.../month=.../day=.../hour=.../

LOCAL_OUTPUT_DIR is the data root (repo: ``data``, Docker: ``/opt/airflow/data``).
Do not set it to ``data/raw`` — that creates a duplicate ``raw/raw/owm`` tree.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

RAW_OWM_PREFIX = "raw/owm"

# Repo root ``data/`` folder; mounted as ``/opt/airflow/data`` in Docker.
DATA_ROOT = Path(os.getenv("LOCAL_OUTPUT_DIR", "data"))


def raw_owm_dir() -> Path:
    """Filesystem path to the raw OWM zone: ``data/raw/owm/``."""
    return DATA_ROOT / "raw" / "owm"


def raw_owm_search_roots() -> list[Path]:
    """
    Directories that may contain raw OWM JSON.

    Canonical path is checked first; legacy misconfigured paths are included
    so existing files are still discoverable after the path fix.
    """
    candidates = [
        raw_owm_dir(),
        DATA_ROOT / "raw" / "raw" / "owm",  # LOCAL_OUTPUT_DIR was wrongly set to data/raw
        DATA_ROOT / "owm",                  # LOCAL_OUTPUT_DIR was data/raw without raw/ prefix
    ]
    seen: set[Path] = set()
    roots: list[Path] = []
    for path in candidates:
        resolved = path.resolve()
        if resolved not in seen and path.exists():
            seen.add(resolved)
            roots.append(path)
    return roots


def make_partition_path(endpoint: str, city: str, now: datetime) -> str:
    """
    Hive-style partition path relative to DATA_ROOT (also used as S3/GCS object key prefix).

    Example: raw/owm/current/city=delhi/year=2026/month=06/day=04/hour=10/
    """
    city_slug = city.lower().replace(" ", "_")
    return (
        f"{RAW_OWM_PREFIX}/{endpoint}/"
        f"city={city_slug}/"
        f"year={now.year:04d}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"hour={now.hour:02d}/"
    )


def make_filename(city: str, now: datetime) -> str:
    city_slug = city.lower().replace(" ", "_")
    return f"{city_slug}_{now.strftime('%Y%m%dT%H%M%SZ')}.json"


def make_storage_key(endpoint: str, city: str, now: datetime) -> str:
    """Full relative key / object name for one JSON payload."""
    return make_partition_path(endpoint, city, now) + make_filename(city, now)


def local_file_path(relative_key: str) -> Path:
    """Resolve a storage key to a local filesystem path."""
    return DATA_ROOT / relative_key
