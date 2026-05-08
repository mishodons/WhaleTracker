from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.weather.strategy import (
    classify_ladder_shape,
    classify_lifecycle_timing,
    nearest_prior_metar_delta_seconds,
)


def test_lifecycle_timing_classification() -> None:
    assert classify_lifecycle_timing(12 * 60, 5) == "posted_immediate"
    assert classify_lifecycle_timing(90, None) == "overnight"
    assert classify_lifecycle_timing(10 * 60, None) == "late_morning"
    assert classify_lifecycle_timing(13 * 60, None) == "midday"
    assert classify_lifecycle_timing(15 * 60, None) == "afternoon"
    assert classify_lifecycle_timing(19 * 60, None) == "late_day"


def test_ladder_shape_flat_arb() -> None:
    rows = [
        {"bucket_label": "16C", "outcome": "Yes", "lower_temp": 16, "upper_temp": 16, "bound_type": "exact", "buy_notional": 20, "net_size": 100},
        {"bucket_label": "17C", "outcome": "Yes", "lower_temp": 17, "upper_temp": 17, "bound_type": "exact", "buy_notional": 20, "net_size": 101},
        {"bucket_label": "18C", "outcome": "Yes", "lower_temp": 18, "upper_temp": 18, "bound_type": "exact", "buy_notional": 20, "net_size": 99},
    ]
    assert classify_ladder_shape(rows, forecast_temp=17) == "flat_arb"


def test_ladder_shape_two_core_with_tail() -> None:
    rows = [
        {"bucket_label": "15C", "outcome": "Yes", "lower_temp": 15, "upper_temp": 15, "bound_type": "exact", "buy_notional": 2, "net_size": 20},
        {"bucket_label": "16C", "outcome": "Yes", "lower_temp": 16, "upper_temp": 16, "bound_type": "exact", "buy_notional": 70, "net_size": 120},
        {"bucket_label": "17C", "outcome": "Yes", "lower_temp": 17, "upper_temp": 17, "bound_type": "exact", "buy_notional": 65, "net_size": 110},
        {"bucket_label": "18C", "outcome": "Yes", "lower_temp": 18, "upper_temp": 18, "bound_type": "exact", "buy_notional": 4, "net_size": 40},
    ]
    assert classify_ladder_shape(rows, forecast_temp=None) == "two_core_ladder"


def test_ladder_shape_forecast_core() -> None:
    rows = [
        {"bucket_label": "15C", "outcome": "Yes", "lower_temp": 15, "upper_temp": 15, "bound_type": "exact", "buy_notional": 2, "net_size": 30},
        {"bucket_label": "16C", "outcome": "Yes", "lower_temp": 16, "upper_temp": 16, "bound_type": "exact", "buy_notional": 20, "net_size": 60},
        {"bucket_label": "17C", "outcome": "Yes", "lower_temp": 17, "upper_temp": 17, "bound_type": "exact", "buy_notional": 60, "net_size": 80},
        {"bucket_label": "18C", "outcome": "Yes", "lower_temp": 18, "upper_temp": 18, "bound_type": "exact", "buy_notional": 20, "net_size": 50},
        {"bucket_label": "21C", "outcome": "Yes", "lower_temp": 21, "upper_temp": 21, "bound_type": "exact", "buy_notional": 1, "net_size": 30},
    ]
    assert classify_ladder_shape(rows, forecast_temp=17) == "forecast_core_ladder"


def test_nearest_prior_metar_delta_seconds() -> None:
    reports = [
        {"first_seen_at": "2026-05-05T12:00:00+00:00"},
        {"first_seen_at": "2026-05-05T12:05:00+00:00"},
        {"first_seen_at": "2026-05-05T12:10:00+00:00"},
    ]
    assert nearest_prior_metar_delta_seconds("2026-05-05T12:05:47+00:00", reports) == 47
    assert nearest_prior_metar_delta_seconds("2026-05-05T11:59:59+00:00", reports) is None
