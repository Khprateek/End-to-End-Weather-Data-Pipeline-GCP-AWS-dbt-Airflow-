"""
Unit tests for extract_weather.py
Run with: pytest tests/ -v
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from extract_weather import (
    _make_filename,
    _make_partition_path,
    validate_current,
    validate_forecast,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def valid_current_payload():
    return {
        "name": "Delhi",
        "dt": 1717488000,
        "coord": {"lat": 28.66, "lon": 77.23},
        "weather": [{"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}],
        "main": {
            "temp": 38.5,
            "feels_like": 37.1,
            "temp_min": 36.0,
            "temp_max": 40.0,
            "pressure": 1002,
            "humidity": 22,
        },
        "wind": {"speed": 4.1, "deg": 270},
        "ingested_at": "2026-06-04T10:00:00+00:00",
        "pipeline_city_query": "Delhi",
    }


@pytest.fixture
def valid_forecast_payload():
    interval = {
        "dt": 1717488000,
        "dt_txt": "2026-06-04 12:00:00",
        "main": {"temp": 38.5, "humidity": 22},
        "weather": [{"main": "Clear"}],
        "wind": {"speed": 4.1},
    }
    return {
        "city": {"id": 1273294, "name": "Delhi", "country": "IN"},
        "list": [interval] * 10,
        "ingested_at": "2026-06-04T10:00:00+00:00",
        "pipeline_city_query": "Delhi",
    }


# ── validate_current ──────────────────────────────────────────────────────────

class TestValidateCurrent:
    def test_valid_payload_passes(self, valid_current_payload):
        validate_current(valid_current_payload)  # should not raise

    def test_missing_main_key_raises(self, valid_current_payload):
        del valid_current_payload["main"]
        with pytest.raises(ValueError, match="Missing keys"):
            validate_current(valid_current_payload)

    def test_missing_weather_key_raises(self, valid_current_payload):
        del valid_current_payload["weather"]
        with pytest.raises(ValueError, match="Missing keys"):
            validate_current(valid_current_payload)

    def test_empty_weather_list_raises(self, valid_current_payload):
        valid_current_payload["weather"] = []
        with pytest.raises(ValueError, match="empty or not a list"):
            validate_current(valid_current_payload)

    def test_impossible_temperature_raises(self, valid_current_payload):
        valid_current_payload["main"]["temp"] = 999
        with pytest.raises(ValueError, match="Temperature out of plausible range"):
            validate_current(valid_current_payload)

    def test_negative_extreme_temperature_raises(self, valid_current_payload):
        valid_current_payload["main"]["temp"] = -100
        with pytest.raises(ValueError, match="Temperature out of plausible range"):
            validate_current(valid_current_payload)

    def test_humidity_out_of_range_raises(self, valid_current_payload):
        valid_current_payload["main"]["humidity"] = 150
        with pytest.raises(ValueError, match="Humidity out of range"):
            validate_current(valid_current_payload)


# ── validate_forecast ─────────────────────────────────────────────────────────

class TestValidateForecast:
    def test_valid_payload_passes(self, valid_forecast_payload):
        validate_forecast(valid_forecast_payload)  # should not raise

    def test_missing_list_key_raises(self, valid_forecast_payload):
        del valid_forecast_payload["list"]
        with pytest.raises(ValueError, match="Missing keys"):
            validate_forecast(valid_forecast_payload)

    def test_empty_list_raises(self, valid_forecast_payload):
        valid_forecast_payload["list"] = []
        with pytest.raises(ValueError, match="empty"):
            validate_forecast(valid_forecast_payload)

    def test_interval_missing_dt_txt_raises(self, valid_forecast_payload):
        del valid_forecast_payload["list"][0]["dt_txt"]
        with pytest.raises(ValueError, match="dt_txt"):
            validate_forecast(valid_forecast_payload)


# ── Partition path helpers ────────────────────────────────────────────────────

class TestPartitionPath:
    def test_path_format(self):
        from datetime import datetime, timezone
        now = datetime(2026, 6, 4, 10, 30, 0, tzinfo=timezone.utc)
        path = _make_partition_path("current", "Delhi", now)
        assert path == "raw/owm/current/city=delhi/year=2026/month=06/day=04/hour=10/"

    def test_city_name_lowercased_and_spaces_replaced(self):
        from datetime import datetime, timezone
        now = datetime(2026, 6, 4, 0, 0, 0, tzinfo=timezone.utc)
        path = _make_partition_path("forecast", "New Delhi", now)
        assert "city=new_delhi" in path

    def test_filename_format(self):
        from datetime import datetime, timezone
        now = datetime(2026, 6, 4, 10, 30, 0, tzinfo=timezone.utc)
        name = _make_filename("Delhi", now)
        assert name == "delhi_20260604T103000Z.json"