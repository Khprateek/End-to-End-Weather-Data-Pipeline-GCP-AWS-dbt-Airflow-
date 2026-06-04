"""
weather_pipeline_dag.py
-----------------------
Airflow DAG for the end-to-end weather pipeline.

Schedule : every hour
Tasks    : 1. validate_api_key  (quick pre-flight check)
           2. extract_current   (current weather, all cities)
           3. extract_forecast  (5-day forecast, all cities)
           4. run_quality_checks (row count + schema assertions)
           5. notify_on_failure  (triggers only if any task fails)

Task 2 and 3 run in parallel after task 1 passes.
Task 4 runs after both 2 and 3 succeed.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Path setup ────────────────────────────────────────────────────────────────
# Allows the DAG to import from src/ regardless of how Airflow is launched
PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

from extract_weather import (   # noqa: E402
    CITIES,
    fetch_current_weather,
    fetch_forecast,
    store,
    validate_current,
    validate_forecast,
)

logger = logging.getLogger(__name__)

# ── Default DAG arguments ─────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,       # set to True + add email once SMTP is configured
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}

# ── Task functions ─────────────────────────────────────────────────────────────

def validate_api_key(**context) -> None:
    """
    Pre-flight check: verify OWM_API_KEY is present and the API is reachable.
    Fails fast before spending quota on all cities.
    """
    import requests

    api_key = os.getenv("OWM_API_KEY", "")
    if not api_key:
        raise ValueError(
            "OWM_API_KEY is not set. "
            "Add it to your .env or Airflow Variables/Connections."
        )

    # Test with a single cheap call (London — guaranteed to exist)
    url = "https://api.openweathermap.org/data/2.5/weather"
    resp = requests.get(url, params={"q": "London", "appid": api_key}, timeout=8)

    if resp.status_code == 401:
        raise PermissionError(
            "OWM API key is invalid or not yet activated. "
            "Keys take up to 2 hours to activate after signup."
        )
    resp.raise_for_status()
    logger.info("API key validated successfully. Status: %s", resp.status_code)


def extract_all_current(**context) -> dict:
    """
    Extract current weather for all cities and land to storage.
    Pushes a summary dict to XCom so downstream tasks can inspect it.
    """
    results = []
    dry_run: bool = context["params"].get("dry_run", False)

    for city_cfg in CITIES:
        city = city_cfg["name"]
        country = city_cfg["country"]
        try:
            payload = fetch_current_weather(city, country)
            validate_current(payload)
            key = store(payload, "current", city, dry_run=dry_run)
            results.append({"city": city, "status": "success", "key": key})
            logger.info("current ✓ %s → %s", city, key)
        except Exception as exc:  # noqa: BLE001
            results.append({"city": city, "status": "error", "error": str(exc)})
            logger.error("current ✗ %s | %s", city, exc)

    failures = [r for r in results if r["status"] == "error"]
    if failures:
        raise RuntimeError(
            f"{len(failures)} cities failed during current extraction: "
            + ", ".join(f["city"] for f in failures)
        )

    # Push summary to XCom so quality checks can read it
    context["ti"].xcom_push(key="current_results", value=results)
    return results


def extract_all_forecast(**context) -> dict:
    """
    Extract 5-day forecast for all cities and land to storage.
    Runs in parallel with extract_all_current.
    """
    results = []
    dry_run: bool = context["params"].get("dry_run", False)

    for city_cfg in CITIES:
        city = city_cfg["name"]
        country = city_cfg["country"]
        try:
            payload = fetch_forecast(city, country)
            validate_forecast(payload)
            key = store(payload, "forecast", city, dry_run=dry_run)
            results.append({"city": city, "status": "success", "key": key})
            logger.info("forecast ✓ %s → %s", city, key)
        except Exception as exc:  # noqa: BLE001
            results.append({"city": city, "status": "error", "error": str(exc)})
            logger.error("forecast ✗ %s | %s", city, exc)

    failures = [r for r in results if r["status"] == "error"]
    if failures:
        raise RuntimeError(
            f"{len(failures)} cities failed during forecast extraction: "
            + ", ".join(f["city"] for f in failures)
        )

    context["ti"].xcom_push(key="forecast_results", value=results)
    return results


def run_quality_checks(**context) -> None:
    """
    Pull XCom results from both extract tasks and assert quality gates:
    - All 10 cities succeeded in both endpoints
    - No city is missing from either result set
    """
    ti = context["ti"]

    current_results = ti.xcom_pull(
        task_ids="extract_current", key="current_results"
    ) or []
    forecast_results = ti.xcom_pull(
        task_ids="extract_forecast", key="forecast_results"
    ) or []

    expected_cities = {c["name"] for c in CITIES}

    current_cities  = {r["city"] for r in current_results  if r["status"] == "success"}
    forecast_cities = {r["city"] for r in forecast_results if r["status"] == "success"}

    missing_current  = expected_cities - current_cities
    missing_forecast = expected_cities - forecast_cities

    if missing_current:
        raise AssertionError(f"Missing current data for: {missing_current}")
    if missing_forecast:
        raise AssertionError(f"Missing forecast data for: {missing_forecast}")

    logger.info(
        "Quality checks passed | current=%d/10 | forecast=%d/10",
        len(current_cities), len(forecast_cities),
    )


def notify_on_failure(**context) -> None:
    """
    Called when any upstream task fails (TriggerRule.ONE_FAILED).
    Extend this to send a Slack message, PagerDuty alert, or email.
    """
    dag_id   = context["dag"].dag_id
    run_id   = context["run_id"]
    task_id  = context.get("task_instance").task_id

    message = (
        f"🔴 Pipeline failure\n"
        f"DAG     : {dag_id}\n"
        f"Run ID  : {run_id}\n"
        f"Task    : {task_id}\n"
        f"Time    : {datetime.now(timezone.utc).isoformat()}\n"
    )
    logger.error(message)

    # ── Slack webhook (uncomment + set SLACK_WEBHOOK_URL in .env) ────────────
    # import requests
    # webhook = os.getenv("SLACK_WEBHOOK_URL", "")
    # if webhook:
    #     requests.post(webhook, json={"text": message}, timeout=5)


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="weather_pipeline",
    description="Hourly extraction of weather data from OpenWeatherMap for 10 Indian cities",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 6, 1, tzinfo=timezone.utc),
    schedule_interval="0 * * * *",      # top of every hour
    catchup=False,                       # don't backfill missed runs
    max_active_runs=1,                   # prevent overlapping runs
    tags=["weather", "ingestion", "owm"],
    params={
        "dry_run": False,               # set True in UI to test without uploading
    },
    doc_md="""
## Weather Pipeline DAG

Extracts **current weather** and **5-day forecasts** from OpenWeatherMap
for 10 Indian cities every hour and lands raw JSON to the data lake.

### Task flow
```
validate_api_key
    ├── extract_current  ──┐
    └── extract_forecast ──┴── quality_checks
                               (on_failure) notify
```

### Parameters
| param | default | description |
|-------|---------|-------------|
| `dry_run` | `false` | Print JSON to logs, skip storage upload |

### Connections needed
- `OWM_API_KEY` in environment / Airflow Variables
- `STORAGE_BACKEND` = `local` | `s3` | `gcs`
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    t_validate = PythonOperator(
        task_id="validate_api_key",
        python_callable=validate_api_key,
    )

    t_current = PythonOperator(
        task_id="extract_current",
        python_callable=extract_all_current,
    )

    t_forecast = PythonOperator(
        task_id="extract_forecast",
        python_callable=extract_all_forecast,
    )

    t_quality = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality_checks,
    )

    t_notify = PythonOperator(
        task_id="notify_on_failure",
        python_callable=notify_on_failure,
        trigger_rule=TriggerRule.ONE_FAILED,   # only runs if something failed
    )

    # ── Task dependencies ──────────────────────────────────────────────────────
    # start → validate → [current, forecast] → quality_checks
    #                                         → notify (on failure only)

    start >> t_validate >> [t_current, t_forecast] >> t_quality
    [t_current, t_forecast, t_quality] >> t_notify