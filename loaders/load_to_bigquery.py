"""
load_to_bigquery.py
-------------------
Loads raw weather JSON files into BigQuery using BATCH loading
(load_table_from_json) — works on the free tier.

Streaming inserts (insert_rows_json) are NOT allowed on the free tier.
This loader writes rows via the Jobs API instead, which is always free.

Usage:
    python loaders/load_to_bigquery.py                    # load all files
    python loaders/load_to_bigquery.py --date 2026-06-06  # specific date
    python loaders/load_to_bigquery.py --dry-run          # print, no upload
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from data_paths import DATA_ROOT, raw_owm_search_roots  # noqa: E402

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("bq_loader")

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ID       = os.getenv("GCP_PROJECT_ID", "")
DATASET_RAW      = os.getenv("BIGQUERY_DATASET_RAW", "raw_weather")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp-credentials.json")
LOCAL_OUTPUT_DIR = str(DATA_ROOT)

TABLE_CURRENT  = f"{PROJECT_ID}.{DATASET_RAW}.current_weather"
TABLE_FORECAST = f"{PROJECT_ID}.{DATASET_RAW}.forecast_weather"

# ── BigQuery Schemas ──────────────────────────────────────────────────────────

SCHEMA_CURRENT = [
    bigquery.SchemaField("city_name",           "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("city_id",             "INTEGER"),
    bigquery.SchemaField("lat",                 "FLOAT"),
    bigquery.SchemaField("lon",                 "FLOAT"),
    bigquery.SchemaField("country",             "STRING"),
    bigquery.SchemaField("dt",                  "INTEGER",   mode="REQUIRED"),
    bigquery.SchemaField("dt_utc",              "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("temp",                "FLOAT"),
    bigquery.SchemaField("feels_like",          "FLOAT"),
    bigquery.SchemaField("temp_min",            "FLOAT"),
    bigquery.SchemaField("temp_max",            "FLOAT"),
    bigquery.SchemaField("pressure",            "INTEGER"),
    bigquery.SchemaField("humidity",            "INTEGER"),
    bigquery.SchemaField("wind_speed",          "FLOAT"),
    bigquery.SchemaField("wind_deg",            "INTEGER"),
    bigquery.SchemaField("wind_gust",           "FLOAT"),
    bigquery.SchemaField("visibility",          "INTEGER"),
    bigquery.SchemaField("cloudiness",          "INTEGER"),
    bigquery.SchemaField("weather_id",          "INTEGER"),
    bigquery.SchemaField("weather_main",        "STRING"),
    bigquery.SchemaField("weather_desc",        "STRING"),
    bigquery.SchemaField("weather_icon",        "STRING"),
    bigquery.SchemaField("sunrise",             "INTEGER"),
    bigquery.SchemaField("sunset",              "INTEGER"),
    bigquery.SchemaField("ingested_at",         "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("pipeline_city_query", "STRING"),
]

SCHEMA_FORECAST = [
    bigquery.SchemaField("city_name",           "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("city_id",             "INTEGER"),
    bigquery.SchemaField("lat",                 "FLOAT"),
    bigquery.SchemaField("lon",                 "FLOAT"),
    bigquery.SchemaField("country",             "STRING"),
    bigquery.SchemaField("dt",                  "INTEGER",   mode="REQUIRED"),
    bigquery.SchemaField("dt_utc",              "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("dt_txt",              "STRING"),
    bigquery.SchemaField("temp",                "FLOAT"),
    bigquery.SchemaField("feels_like",          "FLOAT"),
    bigquery.SchemaField("temp_min",            "FLOAT"),
    bigquery.SchemaField("temp_max",            "FLOAT"),
    bigquery.SchemaField("pressure",            "INTEGER"),
    bigquery.SchemaField("humidity",            "INTEGER"),
    bigquery.SchemaField("wind_speed",          "FLOAT"),
    bigquery.SchemaField("wind_deg",            "INTEGER"),
    bigquery.SchemaField("wind_gust",           "FLOAT"),
    bigquery.SchemaField("cloudiness",          "INTEGER"),
    bigquery.SchemaField("weather_main",        "STRING"),
    bigquery.SchemaField("weather_desc",        "STRING"),
    bigquery.SchemaField("pop",                 "FLOAT"),
    bigquery.SchemaField("rain_3h",             "FLOAT"),
    bigquery.SchemaField("snow_3h",             "FLOAT"),
    bigquery.SchemaField("ingested_at",         "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("pipeline_city_query", "STRING"),
]

# ── BQ Client ─────────────────────────────────────────────────────────────────

def get_bq_client() -> bigquery.Client:
    creds_path = Path(CREDENTIALS_PATH)
    if not creds_path.exists():
        logger.error("Credentials file not found: %s", creds_path)
        sys.exit(1)
    credentials = service_account.Credentials.from_service_account_file(
        str(creds_path),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def ensure_dataset(client: bigquery.Client) -> None:
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_RAW}")
    dataset_ref.location = "US"
    dataset_ref.description = "Raw weather data from OpenWeatherMap API"
    client.create_dataset(dataset_ref, exists_ok=True)
    logger.info("Dataset ready: %s.%s", PROJECT_ID, DATASET_RAW)


def ensure_table(
    client: bigquery.Client,
    table_id: str,
    schema: list[bigquery.SchemaField],
) -> None:
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="dt_utc",
    )
    table.clustering_fields = ["city_name"]
    client.create_table(table, exists_ok=True)
    logger.info("Table ready: %s", table_id)

# ── Row parsers ───────────────────────────────────────────────────────────────

def parse_current(payload: dict) -> dict[str, Any]:
    main    = payload.get("main", {})
    wind    = payload.get("wind", {})
    weather = payload.get("weather", [{}])[0]
    clouds  = payload.get("clouds", {})
    sys_    = payload.get("sys", {})
    coord   = payload.get("coord", {})
    return {
        "city_name":           payload.get("name"),
        "city_id":             payload.get("id"),
        "lat":                 coord.get("lat"),
        "lon":                 coord.get("lon"),
        "country":             sys_.get("country"),
        "dt":                  payload.get("dt"),
        "dt_utc":              datetime.fromtimestamp(
                                   payload["dt"], tz=timezone.utc
                               ).isoformat(),
        "temp":                main.get("temp"),
        "feels_like":          main.get("feels_like"),
        "temp_min":            main.get("temp_min"),
        "temp_max":            main.get("temp_max"),
        "pressure":            main.get("pressure"),
        "humidity":            main.get("humidity"),
        "wind_speed":          wind.get("speed"),
        "wind_deg":            wind.get("deg"),
        "wind_gust":           wind.get("gust"),
        "visibility":          payload.get("visibility"),
        "cloudiness":          clouds.get("all"),
        "weather_id":          weather.get("id"),
        "weather_main":        weather.get("main"),
        "weather_desc":        weather.get("description"),
        "weather_icon":        weather.get("icon"),
        "sunrise":             sys_.get("sunrise"),
        "sunset":              sys_.get("sunset"),
        "ingested_at":         payload.get("ingested_at"),
        "pipeline_city_query": payload.get("pipeline_city_query"),
    }


def parse_forecast(payload: dict) -> list[dict[str, Any]]:
    city  = payload.get("city", {})
    coord = city.get("coord", {})
    rows  = []
    for interval in payload.get("list", []):
        main    = interval.get("main", {})
        wind    = interval.get("wind", {})
        weather = interval.get("weather", [{}])[0]
        clouds  = interval.get("clouds", {})
        rain    = interval.get("rain", {})
        snow    = interval.get("snow", {})
        rows.append({
            "city_name":           city.get("name"),
            "city_id":             city.get("id"),
            "lat":                 coord.get("lat"),
            "lon":                 coord.get("lon"),
            "country":             city.get("country"),
            "dt":                  interval.get("dt"),
            "dt_utc":              datetime.fromtimestamp(
                                       interval["dt"], tz=timezone.utc
                                   ).isoformat(),
            "dt_txt":              interval.get("dt_txt"),
            "temp":                main.get("temp"),
            "feels_like":          main.get("feels_like"),
            "temp_min":            main.get("temp_min"),
            "temp_max":            main.get("temp_max"),
            "pressure":            main.get("pressure"),
            "humidity":            main.get("humidity"),
            "wind_speed":          wind.get("speed"),
            "wind_deg":            wind.get("deg"),
            "wind_gust":           wind.get("gust"),
            "cloudiness":          clouds.get("all"),
            "weather_main":        weather.get("main"),
            "weather_desc":        weather.get("description"),
            "pop":                 interval.get("pop"),
            "rain_3h":             rain.get("3h"),
            "snow_3h":             snow.get("3h"),
            "ingested_at":         payload.get("ingested_at"),
            "pipeline_city_query": payload.get("pipeline_city_query"),
        })
    return rows

# ── File discovery ────────────────────────────────────────────────────────────

def _path_matches_date(path: Path, date_filter: str | None) -> bool:
    """Match Hive partitions like year=2026/month=06/day=08."""
    if not date_filter:
        return True
    try:
        year, month, day = date_filter.split("-")
    except ValueError:
        return date_filter in str(path)
    needle = f"year={year}/month={month}/day={day}"
    return needle in str(path).replace("\\", "/")


def find_json_files(
    date_filter: str | None = None,
) -> dict[str, list[Path]]:
    files: dict[str, list[Path]] = {"current": [], "forecast": []}
    seen: dict[str, set[str]] = {"current": set(), "forecast": set()}
    roots = raw_owm_search_roots()

    if not roots:
        logger.warning(
            "No raw OWM directory found under %s (expected %s/raw/owm/)",
            DATA_ROOT,
            DATA_ROOT,
        )
        return files

    for endpoint in ("current", "forecast"):
        for root in roots:
            search_root = root / endpoint
            if not search_root.exists():
                continue
            for path in sorted(search_root.glob("**/*.json")):
                if not _path_matches_date(path, date_filter):
                    continue
                if path.name in seen[endpoint]:
                    continue
                seen[endpoint].add(path.name)
                files[endpoint].append(path)

    logger.info(
        "Found %d current + %d forecast files under %s",
        len(files["current"]),
        len(files["forecast"]),
        ", ".join(str(r) for r in roots),
    )
    return files

# ── Batch loader (free-tier safe) ─────────────────────────────────────────────

def batch_load(
    client: bigquery.Client,
    table_id: str,
    rows: list[dict],
    dry_run: bool = False,
) -> int:
    """
    Load rows using load_table_from_json (BigQuery Jobs API).
    This is FREE — unlike streaming inserts which require the paid tier.
    Uses WRITE_APPEND so existing rows are preserved across runs.
    """
    if not rows:
        return 0

    if dry_run:
        logger.info("[dry-run] Would batch-load %d rows into %s", len(rows), table_id)
        for row in rows[:2]:
            print(json.dumps(row, indent=2, default=str))
        return len(rows)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=True,
    )

    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()  # waits for job to complete

    if job.errors:
        logger.error("Load job errors for %s: %s", table_id, job.errors)
        return 0

    logger.info("Batch loaded %d rows → %s", len(rows), table_id)
    return len(rows)

# ── Main ──────────────────────────────────────────────────────────────────────

def load_files(
    client: bigquery.Client,
    files: dict[str, list[Path]],
    dry_run: bool = False,
) -> dict[str, int]:
    totals = {"current": 0, "forecast": 0}

    current_rows = []
    for path in files["current"]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            current_rows.append(parse_current(payload))
        except Exception as exc:
            logger.warning("Skipping %s — parse error: %s", path.name, exc)

    totals["current"] = batch_load(client, TABLE_CURRENT, current_rows, dry_run)

    forecast_rows = []
    for path in files["forecast"]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            forecast_rows.extend(parse_forecast(payload))
        except Exception as exc:
            logger.warning("Skipping %s — parse error: %s", path.name, exc)

    totals["forecast"] = batch_load(client, TABLE_FORECAST, forecast_rows, dry_run)

    return totals


def run(date_filter: str | None = None, dry_run: bool = False) -> None:
    if not PROJECT_ID:
        logger.error("GCP_PROJECT_ID is not set in .env")
        sys.exit(1)

    client = get_bq_client()
    ensure_dataset(client)
    ensure_table(client, TABLE_CURRENT,  SCHEMA_CURRENT)
    ensure_table(client, TABLE_FORECAST, SCHEMA_FORECAST)

    files = find_json_files(date_filter)

    if not files["current"] and not files["forecast"]:
        logger.warning(
            "No JSON files found in %s. "
            "Run extract_weather.py first.", LOCAL_OUTPUT_DIR
        )
        return

    totals = load_files(files=files, client=client, dry_run=dry_run)
    logger.info(
        "Load complete | current=%d rows | forecast=%d rows",
        totals["current"], totals["forecast"]
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load raw weather JSON into BigQuery (batch, free-tier safe)"
    )
    parser.add_argument("--date",    type=str, default=None,
                        help="Only load files for this date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print rows, skip BigQuery insert")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(date_filter=args.date, dry_run=args.dry_run)