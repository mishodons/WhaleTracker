from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def to_utc_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return to_utc_datetime(int(raw))
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        return to_utc_datetime(parsed)
    return None


def to_iso(value: Any) -> str | None:
    parsed = to_utc_datetime(value)
    return parsed.isoformat() if parsed else None


def parse_duration(value: str | int | float) -> timedelta:
    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))
    text = str(value).strip().lower()
    if not text:
        raise ValueError("duration cannot be empty")
    suffix = text[-1]
    amount = float(text[:-1]) if suffix in {"s", "m", "h", "d"} else float(text)
    if suffix == "s" or suffix.isdigit():
        return timedelta(seconds=amount)
    if suffix == "m":
        return timedelta(minutes=amount)
    if suffix == "h":
        return timedelta(hours=amount)
    if suffix == "d":
        return timedelta(days=amount)
    raise ValueError(f"unsupported duration: {value}")


def human_duration(delta: timedelta) -> str:
    seconds = int(delta.total_seconds())
    if seconds % 86_400 == 0 and seconds:
        return f"{seconds // 86_400}d"
    if seconds % 3_600 == 0 and seconds:
        return f"{seconds // 3_600}h"
    if seconds % 60 == 0 and seconds:
        return f"{seconds // 60}m"
    return f"{seconds}s"

