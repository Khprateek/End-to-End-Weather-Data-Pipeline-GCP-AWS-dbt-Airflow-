"""
load_to_bigquery.py
-------------------
Loads raw weather JSON files from local storage / GCS into BigQuery.

Two tables are created / appended to in the `raw_weather` dataset:
  - raw_weather.current_weather   (one row per city per hour)
  - raw_weather.forecast_weather  (one row per forecast interval per city per hour)

Usage:
    python loaders/load_to_bigquery.py                  # load all files in data/raw/
    python loaders/load_to_bigquery.py --date 2026-06-04  # load a specific date
    python loaders/load_to_bigquery.py --dry-run          # print rows, skip upload
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

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("bq_loader")

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ID      = os.getenv("GCP_PROJECT_ID", "")
DATASET_RAW     = os.getenv("BIGQUERY_DATASET_RAW", "raw_weather")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp-credentials.json")
LOCAL_OUTPUT_DIR = os.getenv("LOCAL_OUTPUT_DIR", "data/raw")

TABLE_CURRENT  = f"{PROJECT_ID}.{DATASET_RAW}.current_weather"
TABLE_FORECAST = f"{PROJECT_ID}.{DATASET_RAW}.forecast_weather"

# ── BigQuery Schemas ──────────────────────────────────────────────────────────

SCHEMA_CURRENT = [
    bigquery.SchemaField("city_name",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("city_id",          "INTEGER"),
    bigquery.SchemaField("lat",              "FLOAT"),
    bigquery.SchemaField("lon",              "FLOAT"),
    bigquery.SchemaField("country",          "STRING"),
    bigquery.SchemaField("dt",               "INTEGER",   mode="REQUIRED"),
    bigquery.SchemaField("dt_utc",           "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("temp",             "FLOAT"),
    bigquery.SchemaField("feels_like",       "FLOAT"),
    bigquery.SchemaField("temp_min",         "FLOAT"),
    bigquery.SchemaField("temp_max",         "FLOAT"),
    bigquery.SchemaField("pressure",         "INTEGER"),
    bigquery.SchemaField("humidity",         "INTEGER"),
    bigquery.SchemaField("wind_speed",       "FLOAT"),
    bigquery.SchemaField("wind_deg",         "INTEGER"),
    bigquery.SchemaField("wind_gust",        "FLOAT"),
    bigquery.SchemaField("visibility",       "INTEGER"),
    bigquery.SchemaField("cloudiness",       "INTEGER"),
    bigquery.SchemaField("weather_id",       "INTEGER"),
    bigquery.SchemaField("weather_main",     "STRING"),
    bigquery.SchemaField("weather_desc",     "STRING"),
    bigquery.SchemaField("weather_icon",     "STRING"),
    bigquery.SchemaField("sunrise",          "INTEGER"),
    bigquery.SchemaField("sunset",           "INTEGER"),
    bigquery.SchemaField("ingested_at",      "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("pipeline_city_query", "STRING"),
]

SCHEMA_FORECAST = [
    bigquery.SchemaField("city_name",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("city_id",          "INTEGER"),
    bigquery.SchemaField("lat",              "FLOAT"),
    bigquery.SchemaField("lon",              "FLOAT"),
    bigquery.SchemaField("country",          "STRING"),
    bigquery.SchemaField("dt",               "INTEGER",   mode="REQUIRED"),
    bigquery.SchemaField("dt_utc",           "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("dt_txt",           "STRING"),
    bigquery.SchemaField("temp",             "FLOAT"),
    bigquery.SchemaField("feels_like",       "FLOAT"),
    bigquery.SchemaField("temp_min",         "FLOAT"),
    bigquery.SchemaField("temp_max",         "FLOAT"),
    bigquery.SchemaField("pressure",         "INTEGER"),
    bigquery.SchemaField("humidity",         "INTEGER"),
    bigquery.SchemaField("wind_speed",       "FLOAT"),
    bigquery.SchemaField("wind_deg",         "INTEGER"),
    bigquery.SchemaField("wind_gust",        "FLOAT"),
    bigquery.SchemaField("cloudiness",       "INTEGER"),
    bigquery.SchemaField("weather_main",     "STRING"),
    bigquery.SchemaField("weather_desc",     "STRING"),
    bigquery.SchemaField("pop",              "FLOAT"),
    bigquery.SchemaField("rain_3h",          "FLOAT"),
    bigquery.SchemaField("snow_3h",          "FLOAT"),
    bigquery.SchemaField("ingested_at",      "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("pipeline_city_query", "STRING"),
]

# ── BQ Client ─────────────────────────────────────────────────────────────────

def get_bq_client() -> bigquery.Client:
    """Return an authenticated BigQuery client."""
    creds_path = Path(CREDENTIALS_PATH)
    if not creds_path.exists():
        logger.error(
            "Credentials file not found: %s\n"
            "Set GOOGLE_APPLICATION_CREDENTIALS in your .env", creds_path
        )
        sys.exit(1)

    credentials = service_account.Credentials.from_service_account_file(
        str(creds_path),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def ensure_dataset(client: bigquery.Client) -> None:
    """Create the raw_weather dataset if it doesn't exist."""
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
    """Create the table if it doesn't exist, with the given schema."""
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
    """Flatten a raw /weather JSON payload into a single BQ row."""
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
    """
    Flatten a raw /forecast JSON payload into one BQ row per interval.
    A single forecast file contains up to 40 intervals (5 days × 8 per day).
    """
    city   = payload.get("city", {})
    coord  = city.get("coord", {})
    rows   = []

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

def find_json_files(base_dir: str, date_filter: str | None = None) -> dict[str, list[Path]]:
    """
    Walk the raw data directory and return:
      { "current": [Path, ...], "forecast": [Path, ...] }
    Optionally filter to a specific date (YYYY-MM-DD).
    """
    base = Path(base_dir)
    files: dict[str, list[Path]] = {"current": [], "forecast": []}

    for endpoint in ("current", "forecast"):
        pattern = f"**/*.json"
        for path in sorted((base / "owm" / endpoint).glob(pattern)):
            if date_filter:
                if date_filter.replace("-", "/") not in str(path):
                    continue
            files[endpoint].append(path)

    logger.info(
        "Found %d current + %d forecast files",
        len(files["current"]), len(files["forecast"])
    )
    return files

# ── Loader ────────────────────────────────────────────────────────────────────

def load_files(
    client: bigquery.Client,
    files: dict[str, list[Path]],
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Parse all JSON files and stream-insert rows into BigQuery.
    Returns { "current": rows_loaded, "forecast": rows_loaded }.
    """
    totals = {"current": 0, "forecast": 0}

    # ── Current weather ───────────────────────────────────────────────────────
    current_rows = []
    for path in files["current"]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            current_rows.append(parse_current(payload))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping %s — parse error: %s", path.name, exc)

    if current_rows:
        if dry_run:
            logger.info("[dry-run] Would insert %d rows into %s", len(current_rows), TABLE_CURRENT)
            for row in current_rows[:2]:
                print(json.dumps(row, indent=2, default=str))
        else:
            errors = client.insert_rows_json(TABLE_CURRENT, current_rows)
            if errors:
                logger.error("BQ insert errors (current): %s", errors)
            else:
                logger.info("Inserted %d rows → %s", len(current_rows), TABLE_CURRENT)
        totals["current"] = len(current_rows)

    # ── Forecast ──────────────────────────────────────────────────────────────
    forecast_rows = []
    for path in files["forecast"]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            forecast_rows.extend(parse_forecast(payload))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping %s — parse error: %s", path.name, exc)

    if forecast_rows:
        # BigQuery streaming has a 10 MB request limit — batch in chunks of 500
        chunk_size = 500
        chunks = [forecast_rows[i:i+chunk_size] for i in range(0, len(forecast_rows), chunk_size)]
        for chunk in chunks:
            if dry_run:
                logger.info("[dry-run] Would insert %d rows into %s", len(chunk), TABLE_FORECAST)
            else:
                errors = client.insert_rows_json(TABLE_FORECAST, chunk)
                if errors:
                    logger.error("BQ insert errors (forecast): %s", errors)
        if not dry_run:
            logger.info("Inserted %d rows → %s", len(forecast_rows), TABLE_FORECAST)
        totals["forecast"] = len(forecast_rows)

    return totals

# ── Main ──────────────────────────────────────────────────────────────────────

def run(date_filter: str | None = None, dry_run: bool = False) -> None:
    if not PROJECT_ID:
        logger.error("GCP_PROJECT_ID is not set in .env")
        sys.exit(1)

    client = get_bq_client()
    ensure_dataset(client)
    ensure_table(client, TABLE_CURRENT, SCHEMA_CURRENT)
    ensure_table(client, TABLE_FORECAST, SCHEMA_FORECAST)

    files = find_json_files(LOCAL_OUTPUT_DIR, date_filter)

    if not files["current"] and not files["forecast"]:
        logger.warning(
            "No JSON files found in %s. "
            "Run extract_weather.py first to generate raw data.", LOCAL_OUTPUT_DIR
        )
        return

    totals = load_files(client, files, dry_run=dry_run)
    logger.info(
        "Load complete | current=%d rows | forecast=%d rows",
        totals["current"], totals["forecast"]
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load raw weather JSON into BigQuery")
    parser.add_argument("--date", type=str, default=None,
                        help="Only load files for this date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print rows, skip BigQuery insert")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(date_filter=args.date, dry_run=args.dry_run)
