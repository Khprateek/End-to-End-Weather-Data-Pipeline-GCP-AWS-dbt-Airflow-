"""
extract_weather.py
------------------
Extracts current weather and 5-day forecast data from OpenWeatherMap API
for a list of Indian cities and uploads raw JSON to a cloud data lake
(AWS S3 or GCS). Designed to be called by an Airflow DAG.

Usage:
    python extract_weather.py                  # runs for all cities
    python extract_weather.py --city Delhi     # single city
    python extract_weather.py --dry-run        # skip upload, print to stdout
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from data_paths import DATA_ROOT, local_file_path, make_storage_key

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("weather_extractor")

# ── Configuration ─────────────────────────────────────────────────────────────

OWM_API_KEY: str = os.getenv("OWM_API_KEY", "")
STORAGE_BACKEND: str = os.getenv("STORAGE_BACKEND", "local")   # "s3" | "gcs" | "local"
S3_BUCKET: str = os.getenv("S3_BUCKET", "")
GCS_BUCKET: str = os.getenv("GCS_BUCKET", "")
LOCAL_OUTPUT_DIR: str = str(DATA_ROOT)

BASE_URL = "https://api.openweathermap.org/data/2.5"

CITIES: list[dict[str, str]] = [
    {"name": "Delhi",       "country": "IN"},
    {"name": "Mumbai",      "country": "IN"},
    {"name": "Bengaluru",   "country": "IN"},
    {"name": "Prayagraj",   "country": "IN"},
    {"name": "Chennai",     "country": "IN"},
    {"name": "Kolkata",     "country": "IN"},
    {"name": "Hyderabad",   "country": "IN"},
    {"name": "Pune",        "country": "IN"},
    {"name": "Jaipur",      "country": "IN"},
    {"name": "Ahmedabad",   "country": "IN"},
]

# Required keys we validate exist in the API response
REQUIRED_CURRENT_KEYS = {"main", "weather", "wind", "dt", "name", "coord"}
REQUIRED_FORECAST_KEYS = {"list", "city"}

# ── HTTP Client ───────────────────────────────────────────────────────────────

def _build_session() -> requests.Session:
    """Build a requests Session with automatic retry on transient failures."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,          # waits 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = _build_session()

# ── API Calls ─────────────────────────────────────────────────────────────────

def fetch_current_weather(city: str, country: str = "IN") -> dict[str, Any]:
    """
    Fetch current weather for a city from OWM /weather endpoint.
    Returns the raw JSON payload with an added `ingested_at` timestamp.
    Raises requests.HTTPError on non-2xx responses.
    """
    params = {
        "q": f"{city},{country}",
        "appid": OWM_API_KEY,
        "units": "metric",
    }
    logger.info("Fetching current weather | city=%s", city)
    response = SESSION.get(f"{BASE_URL}/weather", params=params, timeout=10)
    response.raise_for_status()

    data: dict = response.json()
    data["ingested_at"] = datetime.now(timezone.utc).isoformat()
    data["pipeline_city_query"] = city
    return data


def fetch_forecast(city: str, country: str = "IN") -> dict[str, Any]:
    """
    Fetch 5-day / 3-hour forecast for a city from OWM /forecast endpoint.
    Returns the raw JSON payload with an added `ingested_at` timestamp.
    Raises requests.HTTPError on non-2xx responses.
    """
    params = {
        "q": f"{city},{country}",
        "appid": OWM_API_KEY,
        "units": "metric",
        "cnt": 40,   # 40 × 3h = 5 days
    }
    logger.info("Fetching forecast | city=%s", city)
    response = SESSION.get(f"{BASE_URL}/forecast", params=params, timeout=10)
    response.raise_for_status()

    data: dict = response.json()
    data["ingested_at"] = datetime.now(timezone.utc).isoformat()
    data["pipeline_city_query"] = city
    return data

# ── Validation ────────────────────────────────────────────────────────────────

def validate_current(payload: dict) -> None:
    """
    Basic schema validation for /weather response.
    Raises ValueError with a descriptive message on any failure.
    """
    missing = REQUIRED_CURRENT_KEYS - payload.keys()
    if missing:
        raise ValueError(f"Missing keys in current weather response: {missing}")

    if not isinstance(payload.get("weather"), list) or len(payload["weather"]) == 0:
        raise ValueError("'weather' field is empty or not a list")

    temp = payload.get("main", {}).get("temp")
    if temp is None or not (-90 <= temp <= 60):
        raise ValueError(f"Temperature out of plausible range: {temp}°C")

    humidity = payload.get("main", {}).get("humidity")
    if humidity is None or not (0 <= humidity <= 100):
        raise ValueError(f"Humidity out of range: {humidity}%")

    logger.info(
        "Validation passed | city=%s temp=%.1f°C humidity=%d%%",
        payload.get("name"), temp, humidity,
    )


def validate_forecast(payload: dict) -> None:
    """
    Basic schema validation for /forecast response.
    Raises ValueError with a descriptive message on any failure.
    """
    missing = REQUIRED_FORECAST_KEYS - payload.keys()
    if missing:
        raise ValueError(f"Missing keys in forecast response: {missing}")

    forecast_list = payload.get("list", [])
    if len(forecast_list) == 0:
        raise ValueError("Forecast list is empty")

    # Check the first interval has the fields we care about
    first = forecast_list[0]
    if "main" not in first or "dt_txt" not in first:
        raise ValueError("Forecast interval missing 'main' or 'dt_txt'")

    logger.info(
        "Validation passed | city=%s intervals=%d",
        payload.get("city", {}).get("name"), len(forecast_list),
    )

# ── Storage ───────────────────────────────────────────────────────────────────

def upload_to_s3(data: dict, key: str) -> None:
    """Upload JSON payload to AWS S3."""
    import boto3  # imported lazily — only needed for S3 backend
    s3 = boto3.client("s3")
    body = json.dumps(data, ensure_ascii=False, indent=2)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Uploaded to S3 | s3://%s/%s", S3_BUCKET, key)


def upload_to_gcs(data: dict, blob_name: str) -> None:
    """Upload JSON payload to Google Cloud Storage."""
    from google.cloud import storage  # imported lazily — only needed for GCS backend
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_name)
    body = json.dumps(data, ensure_ascii=False, indent=2)
    blob.upload_from_string(body, content_type="application/json")
    logger.info("Uploaded to GCS | gs://%s/%s", GCS_BUCKET, blob_name)


def save_locally(data: dict, relative_key: str) -> None:
    """Save JSON payload to local filesystem (useful for dev/testing)."""
    full_path = local_file_path(relative_key)
    full_path.parent.mkdir(parents=True, exist_ok=True)
    full_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Saved locally | %s", full_path)


def store(data: dict, endpoint: str, city: str, dry_run: bool = False) -> str:
    """
    Route the payload to the configured storage backend.
    Returns the storage key/path used.
    """
    now = datetime.now(timezone.utc)
    key = make_storage_key(endpoint, city, now)

    if dry_run:
        print(json.dumps(data, indent=2, ensure_ascii=False))
        logger.info("[dry-run] Would write to key: %s", key)
        return key

    if STORAGE_BACKEND == "s3":
        upload_to_s3(data, key)
    elif STORAGE_BACKEND == "gcs":
        upload_to_gcs(data, key)
    else:
        save_locally(data, key)

    return key

# ── Run metadata ──────────────────────────────────────────────────────────────

def log_run_metadata(results: list[dict], run_id: str) -> None:
    """
    Print a structured run summary. In production, write this to a
    metadata table in your warehouse or a runs log file.
    """
    successes = [r for r in results if r["status"] == "success"]
    failures  = [r for r in results if r["status"] == "error"]

    summary = {
        "run_id": run_id,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "total": len(results),
        "success": len(successes),
        "failure": len(failures),
        "results": results,
    }
    logger.info("Run summary:\n%s", json.dumps(summary, indent=2))

    if failures:
        logger.warning(
            "%d cities failed: %s",
            len(failures),
            [f["city"] for f in failures],
        )

# ── Orchestration ─────────────────────────────────────────────────────────────

def extract_city(city_cfg: dict[str, str], dry_run: bool = False) -> dict:
    """
    Extract and store both endpoints for a single city.
    Returns a result dict with status, keys stored, and error info.
    """
    city = city_cfg["name"]
    country = city_cfg["country"]
    result = {"city": city, "status": "success", "keys": [], "error": None}

    try:
        # --- Current weather ---
        current = fetch_current_weather(city, country)
        validate_current(current)
        key = store(current, "current", city, dry_run)
        result["keys"].append(key)

        # Small delay to stay comfortably within free-tier rate limits
        time.sleep(0.5)

        # --- 5-day forecast ---
        forecast = fetch_forecast(city, country)
        validate_forecast(forecast)
        key = store(forecast, "forecast", city, dry_run)
        result["keys"].append(key)

    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "N/A"
        result["status"] = "error"
        result["error"] = f"HTTP {status_code}: {exc}"
        logger.error("HTTP error | city=%s | %s", city, exc)

    except ValueError as exc:
        result["status"] = "error"
        result["error"] = f"Validation failed: {exc}"
        logger.error("Validation error | city=%s | %s", city, exc)

    except Exception as exc:  # noqa: BLE001
        result["status"] = "error"
        result["error"] = str(exc)
        logger.exception("Unexpected error | city=%s", city)

    return result


def run(cities: list[dict], dry_run: bool = False) -> list[dict]:
    """
    Main entry point. Iterates over cities and extracts weather data.
    Returns a list of result dicts (one per city).
    Raises SystemExit(1) if ALL cities fail.
    """
    if not OWM_API_KEY:
        logger.error("OWM_API_KEY is not set. Add it to your .env file.")
        sys.exit(1)

    run_id = f"weather_run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    logger.info("Starting extraction | run_id=%s | cities=%d | backend=%s",
                run_id, len(cities), STORAGE_BACKEND)

    results = [extract_city(c, dry_run) for c in cities]
    log_run_metadata(results, run_id)

    if all(r["status"] == "error" for r in results):
        logger.error("All cities failed. Marking run as failed.")
        sys.exit(1)

    return results

# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract weather data from OpenWeatherMap")
    parser.add_argument("--city", type=str, default=None,
                        help="Extract a single city by name (default: all cities)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print JSON to stdout instead of uploading")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.city:
        target = [c for c in CITIES if c["name"].lower() == args.city.lower()]
        if not target:
            logger.error("City '%s' not found in CITIES list.", args.city)
            sys.exit(1)
    else:
        target = CITIES

    run(target, dry_run=args.dry_run)