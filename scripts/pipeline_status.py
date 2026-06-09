#!/usr/bin/env python3
"""
pipeline_status.py
------------------
Report where weather data exists across each pipeline layer:

  1. Local data lake  (data/raw/owm/)
  2. GCS bucket       (optional, when STORAGE_BACKEND=gcs)
  3. BigQuery raw     (raw_weather.*)
  4. BigQuery staging (stg_weather.*)
  5. BigQuery mart    (mart_weather.*)  ← Looker Studio reads from here

Usage:
    python scripts/pipeline_status.py
    python scripts/pipeline_status.py --days 7
    python scripts/pipeline_status.py --json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from data_paths import DATA_ROOT, raw_owm_search_roots  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

EXPECTED_CITIES = {
    "delhi", "mumbai", "bengaluru", "prayagraj", "chennai",
    "kolkata", "hyderabad", "pune", "jaipur", "ahmedabad",
}
ENDPOINTS = ("current", "forecast")


@dataclass
class DayStatus:
    date: str
    local_current: set[str] = field(default_factory=set)
    local_forecast: set[str] = field(default_factory=set)
    gcs_current: set[str] = field(default_factory=set)
    gcs_forecast: set[str] = field(default_factory=set)
    bq_raw_rows: int = 0
    bq_raw_cities: set[str] = field(default_factory=set)
    bq_mart_cities: set[str] = field(default_factory=set)

    @property
    def local_ok(self) -> bool:
        return (
            self.local_current >= EXPECTED_CITIES
            and self.local_forecast >= EXPECTED_CITIES
        )

    @property
    def bq_raw_ok(self) -> bool:
        return self.bq_raw_rows > 0

    @property
    def mart_ok(self) -> bool:
        return len(self.bq_mart_cities) >= len(EXPECTED_CITIES)


def _parse_partition(path: Path) -> tuple[str | None, str | None]:
    text = str(path).replace("\\", "/")
    city_m = re.search(r"city=([^/]+)", text)
    day_m = re.search(r"day=(\d{2})", text)
    if not city_m or not day_m:
        return None, None
    month_m = re.search(r"month=(\d{2})", text)
    year_m = re.search(r"year=(\d{4})", text)
    if not month_m or not year_m:
        return None, None
    obs_date = f"{year_m.group(1)}-{month_m.group(1)}-{day_m.group(1)}"
    return obs_date, city_m.group(1)


def scan_local_lake(days: list[str]) -> dict[str, DayStatus]:
    statuses = {d: DayStatus(date=d) for d in days}
    for endpoint in ENDPOINTS:
        for root in raw_owm_search_roots():
            ep_dir = root / endpoint
            if not ep_dir.exists():
                continue
            for path in ep_dir.rglob("*.json"):
                obs_date, city = _parse_partition(path)
                if not obs_date or not city or obs_date not in statuses:
                    continue
                bucket = (
                    statuses[obs_date].local_current
                    if endpoint == "current"
                    else statuses[obs_date].local_forecast
                )
                bucket.add(city)
    return statuses


def scan_gcs(days: list[str]) -> dict[str, DayStatus]:
    statuses = {d: DayStatus(date=d) for d in days}
    bucket_name = os.getenv("GCS_BUCKET", "")
    backend = os.getenv("STORAGE_BACKEND", "local")
    if backend != "gcs" or not bucket_name:
        return statuses

    try:
        from google.cloud import storage
    except ImportError:
        return statuses

    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    client = (
        storage.Client.from_service_account_json(creds_path)
        if creds_path and Path(creds_path).exists()
        else storage.Client()
    )
    bucket = client.bucket(bucket_name)
    for blob in bucket.list_blobs(prefix="raw/owm/"):
        if not blob.name.endswith(".json"):
            continue
        obs_date, city = _parse_partition(Path(blob.name))
        if not obs_date or not city or obs_date not in statuses:
            continue
        endpoint = "current" if "/current/" in blob.name else "forecast"
        target = (
            statuses[obs_date].gcs_current
            if endpoint == "current"
            else statuses[obs_date].gcs_forecast
        )
        target.add(city)
    return statuses


def query_bigquery(days: list[str]) -> dict[str, DayStatus]:
    statuses = {d: DayStatus(date=d) for d in days}
    project = os.getenv("GCP_PROJECT_ID", "")
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not project or not creds_path or not Path(creds_path).exists():
        return statuses

    from google.cloud import bigquery
    from google.oauth2 import service_account

    credentials = service_account.Credentials.from_service_account_file(creds_path)
    client = bigquery.Client(project=project, credentials=credentials)

    def city_slug_expr(column: str) -> str:
        return f"LOWER(REPLACE({column}, ' ', '_'))"

    raw_sql = f"""
        SELECT
            FORMAT_DATE('%Y-%m-%d', DATE(dt_utc)) AS obs_date,
            {city_slug_expr('COALESCE(pipeline_city_query, city_name)')} AS city_slug,
            COUNT(*) AS row_count
        FROM `{project}.raw_weather.current_weather`
        WHERE DATE(dt_utc) BETWEEN @start_date AND @end_date
        GROUP BY 1, 2
    """

    mart_sql = f"""
        SELECT
            FORMAT_DATE('%Y-%m-%d', weather_date) AS obs_date,
            {city_slug_expr('city_name')} AS city_slug
        FROM `{project}.mart_weather.mart_city_weather_daily`
        WHERE weather_date BETWEEN @start_date AND @end_date
        GROUP BY 1, 2
    """

    # Fallback: legacy misnamed dataset from older dbt runs
    legacy_mart_sql = """
        SELECT
            FORMAT_DATE('%Y-%m-%d', weather_date) AS obs_date,
            LOWER(REPLACE(city_name, ' ', '_')) AS city_slug
        FROM `{project}.stg_weather_mart_weather.mart_city_weather_daily`
        WHERE weather_date BETWEEN @start_date AND @end_date
        GROUP BY 1, 2
    """.format(project=project)

    start = min(days)
    end = max(days)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start),
            bigquery.ScalarQueryParameter("end_date", "DATE", end),
        ]
    )

    for row in client.query(raw_sql, job_config=job_config).result():
        day = statuses.get(row.obs_date)
        if day:
            day.bq_raw_rows += row.row_count
            day.bq_raw_cities.add(row.city_slug)

    mart_rows: list[Any] = []
    try:
        mart_rows = list(client.query(mart_sql, job_config=job_config).result())
    except Exception:
        mart_rows = list(client.query(legacy_mart_sql, job_config=job_config).result())

    for row in mart_rows:
        day = statuses.get(row.obs_date)
        if day:
            day.bq_mart_cities.add(row.city_slug)

    return statuses


def merge_status(*layers: dict[str, DayStatus]) -> dict[str, DayStatus]:
    merged: dict[str, DayStatus] = {}
    all_days = set()
    for layer in layers:
        all_days.update(layer.keys())
    for day in sorted(all_days):
        base = DayStatus(date=day)
        for layer in layers:
            if day not in layer:
                continue
            src = layer[day]
            base.local_current |= src.local_current
            base.local_forecast |= src.local_forecast
            base.gcs_current |= src.gcs_current
            base.gcs_forecast |= src.gcs_forecast
            base.bq_raw_rows += src.bq_raw_rows
            base.bq_raw_cities |= src.bq_raw_cities
            base.bq_mart_cities |= src.bq_mart_cities
        merged[day] = base
    return merged


def _icon(ok: bool) -> str:
    return "OK" if ok else "MISSING"


def _missing_cities(found: set[str]) -> str:
    missing = sorted(EXPECTED_CITIES - found)
    return ", ".join(missing) if missing else "-"


def print_report(statuses: dict[str, DayStatus], backend: str) -> None:
    project = os.getenv("GCP_PROJECT_ID", "(not set)")
    print()
    print("=" * 72)
    print("  WEATHER PIPELINE STATUS")
    print(f"  Checked at : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Local root : {DATA_ROOT.resolve()}")
    print(f"  Storage    : {backend}")
    print(f"  GCP project: {project}")
    print(f"  Looker table: {project}.mart_weather.mart_city_weather_daily")
    print("=" * 72)

    header = (
        f"{'Date':<12} {'Local':<8} {'GCS':<8} {'BQ Raw':<8} "
        f"{'Mart':<8} {'Cities in Mart':<16} Missing"
    )
    print(header)
    print("-" * len(header))

    for day, s in statuses.items():
        local_ok = len(s.local_current) > 0 or len(s.local_forecast) > 0
        gcs_ok = (
            True if backend != "gcs"
            else bool(s.gcs_current or s.gcs_forecast)
        )
        mart_count = len(s.bq_mart_cities)
        print(
            f"{day:<12} "
            f"{_icon(local_ok):<8} "
            f"{('n/a' if backend != 'gcs' else _icon(gcs_ok)):<8} "
            f"{_icon(s.bq_raw_ok):<8} "
            f"{_icon(s.mart_ok):<8} "
            f"{mart_count:>2}/10{'':<12} "
            f"{_missing_cities(s.bq_mart_cities)}"
        )

    print()
    print("Layer guide:")
    print("  Local  = JSON files under data/raw/owm/")
    print("  GCS    = Cloud Storage bucket (only when STORAGE_BACKEND=gcs)")
    print("  BQ Raw = raw_weather.current_weather in BigQuery")
    print("  Mart   = mart_weather.mart_city_weather_daily (Looker Studio source)")
    print()
    latest = max(statuses) if statuses else None
    if latest and not statuses[latest].mart_ok:
        print("Action needed:")
        if not statuses[latest].local_ok:
            print("  - Run extraction (Airflow DAG or: python src/extract_weather.py)")
        if statuses[latest].local_ok and not statuses[latest].bq_raw_ok:
            print(f"  - Load to BigQuery: python loaders/load_to_bigquery.py --date {latest}")
        if statuses[latest].bq_raw_ok and not statuses[latest].mart_ok:
            print("  - Run transforms: cd dbt && dbt run")
        if statuses[latest].mart_ok:
            print("  - Refresh your Looker Studio dashboard (data source cache)")
    print()


def build_day_list(num_days: int) -> list[str]:
    today = datetime.now(timezone.utc).date()
    return [
        (today - timedelta(days=i)).isoformat()
        for i in reversed(range(num_days))
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Report weather pipeline layer status")
    parser.add_argument("--days", type=int, default=7, help="How many recent days to check")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    args = parser.parse_args()

    days = build_day_list(args.days)
    backend = os.getenv("STORAGE_BACKEND", "local")

    local = scan_local_lake(days)
    gcs = scan_gcs(days)
    bq = query_bigquery(days)
    statuses = merge_status(local, gcs, bq)

    if args.json:
        payload = {
            day: {
                "local_current_cities": sorted(s.local_current),
                "local_forecast_cities": sorted(s.local_forecast),
                "gcs_current_cities": sorted(s.gcs_current),
                "gcs_forecast_cities": sorted(s.gcs_forecast),
                "bq_raw_rows": s.bq_raw_rows,
                "bq_raw_cities": sorted(s.bq_raw_cities),
                "mart_cities": sorted(s.bq_mart_cities),
                "local_ok": s.local_ok,
                "bq_raw_ok": s.bq_raw_ok,
                "mart_ok": s.mart_ok,
            }
            for day, s in statuses.items()
        }
        print(json.dumps(payload, indent=2))
        return

    print_report(statuses, backend)


if __name__ == "__main__":
    main()
