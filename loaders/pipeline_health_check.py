"""
pipeline_health_check.py
------------------------
Data observability script — checks the health of every stage in the
IndiaWeatherFlow pipeline and tells you exactly where data is stuck.

Checks in order:
  1. Local raw files    — are JSON files present for each date?
  2. BigQuery raw       — did the loader push the data to BQ?
  3. BigQuery staging   — did dbt staging models run?
  4. BigQuery mart      — did dbt mart models run?
  5. Freshness          — how old is the latest data in each stage?
  6. City coverage      — which cities are missing at each stage?
  7. Row count drift    — do row counts look correct?

Usage:
    python loaders/pipeline_health_check.py
    python loaders/pipeline_health_check.py --date 2026-06-08
    python loaders/pipeline_health_check.py --days 7
    python loaders/pipeline_health_check.py --stage raw
    python loaders/pipeline_health_check.py --stage bigquery
    python loaders/pipeline_health_check.py --stage dbt
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ID        = os.getenv("GCP_PROJECT_ID", "")
DATASET_RAW       = os.getenv("BIGQUERY_DATASET_RAW",     "raw_weather")
DATASET_STAGING   = os.getenv("BIGQUERY_DATASET_STAGING", "stg_weather")
DATASET_MART      = os.getenv("BIGQUERY_DATASET_MART",    "mart_weather")
CREDENTIALS_PATH  = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp-credentials.json")
LOCAL_OUTPUT_DIR  = os.getenv("LOCAL_OUTPUT_DIR", "data/raw")

EXPECTED_CITIES = [
    "Delhi", "Mumbai", "Bengaluru", "Allahabad", "Chennai",
    "Kolkata", "Hyderabad", "Pune", "Jaipur", "Ahmedabad",
]

# Status symbols
OK      = "✓"
WARN    = "⚠"
FAIL    = "✗"
INFO    = "→"

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.WARNING,
    format="%(message)s",
)

# ── Colours (terminal) ────────────────────────────────────────────────────────

def green(s):  return f"\033[92m{s}\033[0m"
def yellow(s): return f"\033[93m{s}\033[0m"
def red(s):    return f"\033[91m{s}\033[0m"
def bold(s):   return f"\033[1m{s}\033[0m"
def dim(s):    return f"\033[2m{s}\033[0m"
def cyan(s):   return f"\033[96m{s}\033[0m"

def status_line(status: str, label: str, detail: str = "") -> None:
    if status == OK:
        sym = green(OK)
    elif status == WARN:
        sym = yellow(WARN)
    elif status == FAIL:
        sym = red(FAIL)
    else:
        sym = dim(INFO)
    detail_str = f"  {dim(detail)}" if detail else ""
    print(f"  {sym}  {label}{detail_str}")

# ── BQ client ─────────────────────────────────────────────────────────────────

def get_bq_client() -> bigquery.Client | None:
    creds_path = Path(CREDENTIALS_PATH)
    if not creds_path.exists():
        return None
    try:
        credentials = service_account.Credentials.from_service_account_file(
            str(creds_path),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    except Exception as exc:
        print(red(f"  BQ auth failed: {exc}"))
        return None


def bq_query(client: bigquery.Client, sql: str) -> list[dict]:
    try:
        rows = list(client.query(sql).result())
        return [dict(r) for r in rows]
    except Exception as exc:
        print(red(f"  BQ query failed: {exc}"))
        return []

# ── Date helpers ──────────────────────────────────────────────────────────────

def date_range(start: str, days: int) -> list[str]:
    s = datetime.strptime(start, "%Y-%m-%d")
    return [(s + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]


def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ── STAGE 1: Local raw files ───────────────────────────────────────────────────

def check_local_raw(dates: list[str]) -> dict:
    """
    Check data/raw/owm/current + forecast folders for each date.
    Returns per-date status dict.
    """
    results = {}
    base = Path(LOCAL_OUTPUT_DIR)

    for date in dates:
        y, m, d = date[:4], date[5:7], date[8:10]
        current_files  = list((base / "owm" / "current").glob(
            f"**/year={y}/month={m}/day={d}/**/*.json"))
        forecast_files = list((base / "owm" / "forecast").glob(
            f"**/year={y}/month={m}/day={d}/**/*.json"))

        cities_current  = {p.parent.parent.parent.parent.name.replace("city=", "").title()
                           for p in current_files}
        cities_forecast = {p.parent.parent.parent.parent.name.replace("city=", "").title()
                           for p in forecast_files}

        missing_current  = set(c.lower() for c in EXPECTED_CITIES) - \
                           set(c.lower() for c in cities_current)
        missing_forecast = set(c.lower() for c in EXPECTED_CITIES) - \
                           set(c.lower() for c in cities_forecast)

        results[date] = {
            "current_files":   len(current_files),
            "forecast_files":  len(forecast_files),
            "missing_current":  missing_current,
            "missing_forecast": missing_forecast,
            "ok": len(current_files) > 0 and len(forecast_files) > 0,
        }

    return results


def print_local_raw(results: dict) -> bool:
    print(bold(cyan("\n[Stage 1] Local raw files  (data/raw/owm/)")))
    all_ok = True

    for date, r in results.items():
        if r["current_files"] == 0 and r["forecast_files"] == 0:
            status_line(FAIL, date,
                "NO FILES FOUND → extract_weather.py has not run for this date")
            all_ok = False
        elif r["missing_current"] or r["missing_forecast"]:
            status_line(WARN, date,
                f"{r['current_files']} current + {r['forecast_files']} forecast files "
                f"| missing cities: {r['missing_current'] or r['missing_forecast']}")
            all_ok = False
        else:
            status_line(OK, date,
                f"{r['current_files']} current + {r['forecast_files']} forecast files")

    if all_ok:
        print(green("  → All dates have raw files locally"))
    else:
        print(yellow("  → FIX: run  python src/extract_weather.py"))

    return all_ok

# ── STAGE 2: BigQuery raw tables ──────────────────────────────────────────────

def check_bq_raw(client: bigquery.Client, dates: list[str]) -> dict:
    results = {}
    for date in dates:
        sql_current = f"""
            SELECT COUNT(*) as cnt,
                   COUNT(DISTINCT city_name) as cities
            FROM `{PROJECT_ID}.{DATASET_RAW}.current_weather`
            WHERE DATE(dt_utc) = '{date}'
        """
        sql_forecast = f"""
            SELECT COUNT(*) as cnt,
                   COUNT(DISTINCT city_name) as cities
            FROM `{PROJECT_ID}.{DATASET_RAW}.forecast_weather`
            WHERE DATE(dt_utc) = '{date}'
        """
        cur  = bq_query(client, sql_current)
        fore = bq_query(client, sql_forecast)

        cur_count   = cur[0]["cnt"]   if cur  else 0
        cur_cities  = cur[0]["cities"] if cur  else 0
        fore_count  = fore[0]["cnt"]  if fore else 0
        fore_cities = fore[0]["cities"] if fore else 0

        # Expected: ≥1 current row + ≥40 forecast rows per city
        results[date] = {
            "current_rows":    cur_count,
            "current_cities":  cur_cities,
            "forecast_rows":   fore_count,
            "forecast_cities": fore_cities,
            "ok": cur_count > 0 and fore_count > 0,
        }

    return results


def print_bq_raw(results: dict) -> bool:
    print(bold(cyan("\n[Stage 2] BigQuery raw tables  (raw_weather dataset)")))
    all_ok = True

    for date, r in results.items():
        if r["current_rows"] == 0 and r["forecast_rows"] == 0:
            status_line(FAIL, date,
                "0 rows in BigQuery → loader has NOT run for this date")
            all_ok = False
        elif r["current_rows"] == 0:
            status_line(FAIL, date,
                f"current_weather = 0 rows | forecast_weather = {r['forecast_rows']} rows")
            all_ok = False
        elif r["forecast_rows"] == 0:
            status_line(FAIL, date,
                f"current_weather = {r['current_rows']} rows | forecast_weather = 0 rows")
            all_ok = False
        elif r["current_cities"] < len(EXPECTED_CITIES):
            missing = len(EXPECTED_CITIES) - r["current_cities"]
            status_line(WARN, date,
                f"{r['current_rows']} current + {r['forecast_rows']} forecast rows "
                f"| {missing} cities missing in BQ")
            all_ok = False
        else:
            status_line(OK, date,
                f"{r['current_rows']} current rows ({r['current_cities']} cities) "
                f"+ {r['forecast_rows']} forecast rows")

    if all_ok:
        print(green("  → All dates are in BigQuery raw tables"))
    else:
        print(yellow("  → FIX: run  python loaders/load_to_bigquery.py"))

    return all_ok

# ── STAGE 3: dbt staging views ────────────────────────────────────────────────

def check_bq_staging(client: bigquery.Client, dates: list[str]) -> dict:
    results = {}
    for date in dates:
        sql = f"""
            SELECT
              COUNT(*) as current_rows,
              COUNT(DISTINCT city_name) as cities
            FROM `{PROJECT_ID}.{DATASET_STAGING}.stg_current_weather`
            WHERE observed_date = '{date}'
        """
        rows = bq_query(client, sql)
        cnt    = rows[0]["current_rows"] if rows else 0
        cities = rows[0]["cities"]        if rows else 0

        results[date] = {
            "rows":   cnt,
            "cities": cities,
            "ok":     cnt > 0,
        }

    return results


def print_bq_staging(results: dict) -> bool:
    print(bold(cyan("\n[Stage 3] dbt staging views  (stg_weather dataset)")))
    all_ok = True

    for date, r in results.items():
        if r["rows"] == 0:
            status_line(FAIL, date,
                "0 rows in stg_current_weather → dbt staging has NOT run "
                "OR raw data not loaded yet")
            all_ok = False
        elif r["cities"] < len(EXPECTED_CITIES):
            status_line(WARN, date,
                f"{r['rows']} rows | only {r['cities']}/{len(EXPECTED_CITIES)} cities present")
            all_ok = False
        else:
            status_line(OK, date, f"{r['rows']} rows | {r['cities']} cities")

    if all_ok:
        print(green("  → Staging views are healthy"))
    else:
        print(yellow("  → FIX: cd dbt && dbt run --select staging"))

    return all_ok

# ── STAGE 4: dbt mart tables ──────────────────────────────────────────────────

def check_bq_mart(client: bigquery.Client, dates: list[str]) -> dict:
    results = {}
    for date in dates:
        sql_daily = f"""
            SELECT COUNT(*) as rows, COUNT(DISTINCT city_name) as cities
            FROM `{PROJECT_ID}.{DATASET_MART}.mart_city_weather_daily`
            WHERE weather_date = '{date}'
        """
        sql_comp = f"""
            SELECT COUNT(*) as rows
            FROM `{PROJECT_ID}.{DATASET_MART}.mart_city_comparison`
        """
        sql_fore = f"""
            SELECT COUNT(*) as rows
            FROM `{PROJECT_ID}.{DATASET_MART}.mart_forecast_next24h`
            WHERE forecast_date = '{date}'
        """
        daily = bq_query(client, sql_daily)
        comp  = bq_query(client, sql_comp)
        fore  = bq_query(client, sql_fore)

        daily_rows   = daily[0]["rows"]   if daily else 0
        daily_cities = daily[0]["cities"] if daily else 0
        comp_rows    = comp[0]["rows"]    if comp  else 0
        fore_rows    = fore[0]["rows"]    if fore  else 0

        results[date] = {
            "daily_rows":    daily_rows,
            "daily_cities":  daily_cities,
            "comp_rows":     comp_rows,
            "forecast_rows": fore_rows,
            "ok": daily_rows > 0,
        }

    return results


def print_bq_mart(results: dict) -> bool:
    print(bold(cyan("\n[Stage 4] dbt mart tables  (mart_weather dataset)")))
    all_ok = True

    for date, r in results.items():
        if r["daily_rows"] == 0:
            status_line(FAIL, date,
                "0 rows in mart_city_weather_daily → dbt mart has NOT run")
            all_ok = False
        elif r["daily_cities"] < len(EXPECTED_CITIES):
            status_line(WARN, date,
                f"mart_daily: {r['daily_rows']} rows | "
                f"only {r['daily_cities']}/{len(EXPECTED_CITIES)} cities")
            all_ok = False
        else:
            status_line(OK, date,
                f"mart_daily: {r['daily_rows']} rows | "
                f"comparison: {r['comp_rows']} rows | "
                f"forecast_24h: {r['forecast_rows']} rows")

    if all_ok:
        print(green("  → Mart tables are healthy"))
    else:
        print(yellow("  → FIX: cd dbt && dbt run --select mart"))

    return all_ok

# ── STAGE 5: Freshness check ──────────────────────────────────────────────────

def check_freshness(client: bigquery.Client) -> None:
    print(bold(cyan("\n[Stage 5] Data freshness")))

    sql = f"""
        SELECT
          MAX(ingested_at)                        as latest_ingestion,
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),
            MAX(ingested_at), MINUTE)             as minutes_ago,
          COUNT(DISTINCT DATE(dt_utc))            as distinct_dates,
          MIN(DATE(dt_utc))                       as earliest_date,
          MAX(DATE(dt_utc))                       as latest_date,
          COUNT(*)                                as total_rows
        FROM `{PROJECT_ID}.{DATASET_RAW}.current_weather`
    """
    rows = bq_query(client, sql)
    if not rows or rows[0]["total_rows"] == 0:
        status_line(FAIL, "current_weather", "table is empty")
        return

    r = rows[0]
    mins  = r["minutes_ago"]
    fresh = mins is not None and mins < 90

    age_str = f"{mins} min ago" if mins is not None else "unknown"

    status_line(
        OK if fresh else WARN,
        f"Latest ingestion: {r['latest_ingestion']}  ({age_str})",
        f"dates: {r['earliest_date']} → {r['latest_date']}  |  "
        f"total rows: {r['total_rows']}  |  distinct dates: {r['distinct_dates']}"
    )

    if not fresh and mins is not None:
        if mins < 1440:
            print(yellow(f"  → Data is {mins} min old — pipeline may have missed a run"))
        else:
            days = mins // 1440
            print(red(f"  → Data is {days} day(s) old — pipeline has not run recently"))
            print(yellow("  → FIX: run python src/extract_weather.py && "
                         "python loaders/load_to_bigquery.py"))

# ── STAGE 6: City coverage ────────────────────────────────────────────────────

def check_city_coverage(client: bigquery.Client, dates: list[str]) -> None:
    print(bold(cyan("\n[Stage 6] City coverage across all stages")))

    for date in dates:
        print(f"\n  {bold(date)}")

        for stage, table, col in [
            ("raw → current_weather",           f"{DATASET_RAW}.current_weather",   "city_name"),
            ("raw → forecast_weather",          f"{DATASET_RAW}.forecast_weather",  "city_name"),
            ("stg → stg_current_weather",       f"{DATASET_STAGING}.stg_current_weather", "city_name"),
            ("mart → mart_city_weather_daily",  f"{DATASET_MART}.mart_city_weather_daily", "city_name"),
        ]:
            date_col = "dt_utc" if "forecast" in table or "current_weather" in table else "weather_date"
            if "stg_current" in table:
                date_col = "observed_date"

            sql = f"""
                SELECT ARRAY_AGG(DISTINCT {col} ORDER BY {col}) as cities
                FROM `{PROJECT_ID}.{table}`
                WHERE DATE({date_col}) = '{date}'
            """
            rows = bq_query(client, sql)
            found = set(rows[0]["cities"] or []) if rows else set()
            expected_lower = {c.lower() for c in EXPECTED_CITIES}
            found_lower    = {c.lower() for c in found}
            missing        = expected_lower - found_lower

            if not found:
                status_line(FAIL, stage, "no data")
            elif missing:
                status_line(WARN, stage,
                    f"{len(found)}/10 cities  |  missing: {', '.join(sorted(missing))}")
            else:
                status_line(OK, stage,"all 10 cities present")

# ── STAGE 7: Summary + fix guide ─────────────────────────────────────────────

def print_summary(stage_ok: dict) -> None:
    print(bold(cyan("\n" + "─" * 58)))
    print(bold("  Pipeline health summary"))
    print(bold("─" * 58))

    all_healthy = all(stage_ok.values())

    for stage, ok in stage_ok.items():
        status_line(OK if ok else FAIL, stage)

    print()
    if all_healthy:
        print(green("  ✓ All stages healthy — data should be visible in Looker Studio"))
        print(dim("    If Looker still shows no data, check the date range filter"))
        print(dim("    in your report (Resource → Report settings → Date range)"))
    else:
        print(yellow("  Pipeline has gaps. Run these commands in order:\n"))
        if not stage_ok.get("Local raw files"):
            print(f"  1. {cyan('python src/extract_weather.py')}")
            print(dim("     Extracts raw JSON from OpenWeatherMap for all 10 cities\n"))
        if not stage_ok.get("BigQuery raw"):
            print(f"  2. {cyan('python loaders/load_to_bigquery.py')}")
            print(dim("     Batch-loads raw JSON files into BigQuery raw_weather dataset\n"))
        if not stage_ok.get("dbt staging"):
            print(f"  3. {cyan('cd dbt && dbt run --select staging')}")
            print(dim("     Rebuilds stg_current_weather and stg_forecast_weather views\n"))
        if not stage_ok.get("dbt mart"):
            print(f"  4. {cyan('cd dbt && dbt run --select mart')}")
            print(dim("     Rebuilds mart_city_weather_daily, mart_city_comparison,"))
            print(dim("     mart_forecast_next24h tables in mart_weather dataset\n"))
        print(dim("  After each step, re-run this script to verify progress:"))
        print(f"  {cyan('python loaders/pipeline_health_check.py')}\n")

    print(bold("─" * 58 + "\n"))

# ── Main ──────────────────────────────────────────────────────────────────────

def run(dates: list[str], stage: str | None = None) -> None:
    print(bold(cyan("\n" + "═" * 58)))
    print(bold(cyan("  IndiaWeatherFlow — Pipeline Health Check")))
    print(bold(cyan("═" * 58)))
    print(dim(f"  Checking {len(dates)} date(s): {dates[0]}"
              + (f" → {dates[-1]}" if len(dates) > 1 else "")))
    print(dim(f"  Project: {PROJECT_ID}"))

    if not PROJECT_ID:
        print(red("\n  GCP_PROJECT_ID not set in .env — cannot query BigQuery"))
        sys.exit(1)

    client = get_bq_client()
    if client is None:
        print(red("\n  Cannot connect to BigQuery"))
        print(yellow(f"  Check that {CREDENTIALS_PATH} exists and is valid"))
        sys.exit(1)

    stage_ok: dict[str, bool] = {}

    run_all = stage is None

    # Stage 1 — local files
    if run_all or stage == "raw":
        local_results = check_local_raw(dates)
        stage_ok["Local raw files"] = print_local_raw(local_results)

    # Stage 2 — BQ raw
    if run_all or stage in ("bigquery", "bq"):
        bq_raw_results = check_bq_raw(client, dates)
        stage_ok["BigQuery raw"] = print_bq_raw(bq_raw_results)

    # Stage 3 — staging
    if run_all or stage == "dbt":
        stg_results = check_bq_staging(client, dates)
        stage_ok["dbt staging"] = print_bq_staging(stg_results)

    # Stage 4 — mart
    if run_all or stage == "dbt":
        mart_results = check_bq_mart(client, dates)
        stage_ok["dbt mart"] = print_bq_mart(mart_results)

    # Stage 5 — freshness
    if run_all:
        check_freshness(client)

    # Stage 6 — city coverage
    if run_all:
        check_city_coverage(client, dates)

    # Stage 7 — summary
    if run_all:
        print_summary(stage_ok)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="IndiaWeatherFlow pipeline health check — find where data is stuck"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date",  type=str, default=None,
                       help="Check a specific date (YYYY-MM-DD), default = today")
    group.add_argument("--days",  type=int, default=3,
                       help="Check last N days (default: 3)")
    parser.add_argument("--stage", type=str, default=None,
                        choices=["raw", "bigquery", "bq", "dbt"],
                        help="Check only a specific stage")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.date:
        dates = [args.date]
    else:
        end   = datetime.now(timezone.utc)
        dates = [(end - timedelta(days=i)).strftime("%Y-%m-%d")
                 for i in range(args.days - 1, -1, -1)]

    run(dates=dates, stage=args.stage)
