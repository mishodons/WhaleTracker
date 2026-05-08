from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable


WEATHER_EVENT_RE = re.compile(r"^highest-temperature-in-(?P<city>.+)-on-(?P<month>[a-z]+)-(?P<day>\d{1,2})-(?P<year>\d{4})$")
BETWEEN_TITLE_RE = re.compile(
    r"highest temperature in (?P<city>.+?) be between (?P<lower>-?\d+)-(?P<upper>-?\d+)\s*(?:deg|degrees|°)?(?P<unit>[cf]) on (?P<date>.+?)\??$",
    re.IGNORECASE,
)
SINGLE_TITLE_RE = re.compile(
    r"highest temperature in (?P<city>.+?) be (?P<temp>-?\d+)\s*(?:deg|degrees|°)?(?P<unit>[cf])(?P<suffix> or higher| or above| or lower| or below)? on (?P<date>.+?)\??$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class WeatherBasketInfo:
    event_slug: str
    city: str | None
    forecast_date: str | None
    unit: str | None
    event_title: str | None = None
    status: str = "active"


@dataclass(frozen=True)
class WeatherBucketInfo:
    event_slug: str
    city: str | None
    forecast_date: str | None
    unit: str | None
    condition_id: str | None
    token_id: str
    market_slug: str | None
    market_title: str | None
    outcome: str | None
    bucket_label: str | None
    lower_temp: float | None
    upper_temp: float | None
    bound_type: str | None
    active: bool = True
    closed: bool = False
    raw: dict[str, Any] | None = None

    def basket_row(self) -> dict[str, Any]:
        return {
            "event_slug": self.event_slug,
            "city": self.city,
            "forecast_date": self.forecast_date,
            "unit": self.unit,
            "event_title": self.event_slug.replace("-", " ").title() if self.event_slug else None,
            "status": "closed" if self.closed else "active",
            "raw": self.raw or {},
        }

    def bucket_row(self, basket_id: int | None = None) -> dict[str, Any]:
        return {
            "basket_id": basket_id,
            "event_slug": self.event_slug,
            "city": self.city,
            "forecast_date": self.forecast_date,
            "unit": self.unit,
            "condition_id": self.condition_id,
            "token_id": self.token_id,
            "market_slug": self.market_slug,
            "market_title": self.market_title,
            "outcome": self.outcome,
            "bucket_label": self.bucket_label,
            "lower_temp": self.lower_temp,
            "upper_temp": self.upper_temp,
            "bound_type": self.bound_type,
            "active": self.active,
            "closed": self.closed,
            "raw": self.raw or {},
        }


def normalize_weather_text(value: str | None) -> str:
    text = (value or "").replace("Â°", "°").replace("º", "°")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def is_weather_slug(slug: str | None) -> bool:
    return bool(slug and "highest-temperature-in-" in slug and "-on-" in slug)


def is_weather_trade(raw: dict[str, Any]) -> bool:
    return any(
        is_weather_slug(str(raw.get(name) or ""))
        for name in ("eventSlug", "event_slug", "slug", "market_slug")
    ) or "highest temperature in" in normalize_weather_text(str(raw.get("title") or raw.get("market_title") or "")).lower()


def _title_case_city(value: str | None) -> str | None:
    if not value:
        return None
    return " ".join(part.capitalize() for part in value.replace("-", " ").split())


def _parse_event_slug(event_slug: str | None) -> tuple[str | None, str | None]:
    if not event_slug:
        return None, None
    match = WEATHER_EVENT_RE.match(event_slug)
    if not match:
        return None, None
    city = _title_case_city(match.group("city"))
    month = match.group("month").capitalize()
    day = int(match.group("day"))
    year = int(match.group("year"))
    try:
        forecast_date = datetime.strptime(f"{month} {day} {year}", "%B %d %Y").date().isoformat()
    except ValueError:
        forecast_date = None
    return city, forecast_date


def weather_event_slug(raw: dict[str, Any]) -> str | None:
    direct = raw.get("eventSlug") or raw.get("event_slug")
    if direct and is_weather_slug(str(direct)):
        return str(direct)
    slug = str(raw.get("slug") or raw.get("market_slug") or "")
    if not is_weather_slug(slug):
        return None
    match = re.match(r"^(highest-temperature-in-.+-on-[a-z]+-\d{1,2}-\d{4})(?:-.+)?$", slug)
    return match.group(1) if match else None


def parse_bucket_from_title(title: str | None) -> dict[str, Any]:
    text = normalize_weather_text(title)
    lowered = text.lower()
    if lowered.startswith("will the "):
        lowered = lowered[len("will the ") :]
    between = BETWEEN_TITLE_RE.search(lowered)
    if between:
        lower = float(between.group("lower"))
        upper = float(between.group("upper"))
        unit = between.group("unit").upper()
        return {
            "city": _title_case_city(between.group("city")),
            "unit": unit,
            "bucket_label": f"{int(lower)}-{int(upper)}{unit}",
            "lower_temp": lower,
            "upper_temp": upper,
            "bound_type": "range",
        }
    single = SINGLE_TITLE_RE.search(lowered)
    if single:
        temp = float(single.group("temp"))
        unit = single.group("unit").upper()
        suffix = (single.group("suffix") or "").strip().lower()
        if suffix in {"or higher", "or above"}:
            label = f"{int(temp)}{unit}+"
            upper = None
            bound_type = "lower_bound"
        elif suffix in {"or lower", "or below"}:
            label = f"<={int(temp)}{unit}"
            upper = temp
            temp = None
            bound_type = "upper_bound"
        else:
            label = f"{int(temp)}{unit}"
            upper = temp
            bound_type = "exact"
        return {
            "city": _title_case_city(single.group("city")),
            "unit": unit,
            "bucket_label": label,
            "lower_temp": temp,
            "upper_temp": upper,
            "bound_type": bound_type,
        }
    return {
        "city": None,
        "unit": None,
        "bucket_label": None,
        "lower_temp": None,
        "upper_temp": None,
        "bound_type": None,
    }


def _decode_jsonish(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except ValueError:
        return value


def extract_token_outcomes(raw: dict[str, Any]) -> list[tuple[str, str | None]]:
    asset = raw.get("asset") or raw.get("asset_id") or raw.get("token_id")
    if asset:
        return [(str(asset), raw.get("outcome") or "Yes")]

    token_values = (
        raw.get("clobTokenIds")
        or raw.get("clob_token_ids")
        or raw.get("tokens")
        or raw.get("tokenIds")
        or []
    )
    outcomes = raw.get("outcomes") or raw.get("outcomePrices") or []
    token_values = _decode_jsonish(token_values) or []
    outcomes = _decode_jsonish(outcomes) or []

    if isinstance(token_values, dict):
        token_values = list(token_values.values())
    if isinstance(outcomes, dict):
        outcomes = list(outcomes.values())

    pairs: list[tuple[str, str | None]] = []
    for index, token in enumerate(token_values):
        if isinstance(token, dict):
            token_id = token.get("token_id") or token.get("asset_id") or token.get("id")
            outcome = token.get("outcome") or token.get("name")
        else:
            token_id = token
            outcome = outcomes[index] if index < len(outcomes) and isinstance(outcomes[index], str) else None
        if token_id:
            pairs.append((str(token_id), outcome))
    return pairs


def bucket_infos_from_market(raw: dict[str, Any]) -> list[WeatherBucketInfo]:
    event_slug = weather_event_slug(raw)
    if not event_slug:
        return []
    title = raw.get("title") or raw.get("question") or raw.get("market_title")
    parsed = parse_bucket_from_title(title)
    slug_city, forecast_date = _parse_event_slug(event_slug)
    city = parsed.get("city") or slug_city
    unit = parsed.get("unit")
    condition_id = raw.get("conditionId") or raw.get("condition_id") or raw.get("market")
    active = bool(raw.get("active", True))
    closed = bool(raw.get("closed", False))
    buckets: list[WeatherBucketInfo] = []
    for token_id, outcome in extract_token_outcomes(raw):
        buckets.append(
            WeatherBucketInfo(
                event_slug=event_slug,
                city=city,
                forecast_date=forecast_date,
                unit=unit,
                condition_id=str(condition_id).lower() if condition_id else None,
                token_id=token_id,
                market_slug=raw.get("slug") or raw.get("market_slug"),
                market_title=title,
                outcome=outcome,
                bucket_label=parsed.get("bucket_label"),
                lower_temp=parsed.get("lower_temp"),
                upper_temp=parsed.get("upper_temp"),
                bound_type=parsed.get("bound_type"),
                active=active,
                closed=closed,
                raw=raw,
            )
        )
    return buckets


async def discover_weather_markets(
    gamma_client: Any,
    repository: Any,
    *,
    market_limit: int = 500,
    max_pages: int = 8,
) -> list[WeatherBucketInfo]:
    discovered: list[WeatherBucketInfo] = []
    for page in range(max_pages):
        markets = await gamma_client.get_markets(active="true", closed="false", limit=market_limit, offset=page * market_limit)
        if not markets:
            break
        for raw in markets:
            for bucket in bucket_infos_from_market(raw):
                basket_id = repository.upsert_weather_basket(bucket.basket_row())
                repository.upsert_weather_bucket_market(bucket.bucket_row(basket_id))
                discovered.append(bucket)
        if len(markets) < market_limit:
            break
    return discovered


def upsert_weather_trade_market(repository: Any, raw_trade: dict[str, Any]) -> WeatherBucketInfo | None:
    infos = bucket_infos_from_market(raw_trade)
    if not infos:
        return None
    info = infos[0]
    basket_id = repository.upsert_weather_basket(info.basket_row())
    repository.upsert_weather_bucket_market(info.bucket_row(basket_id))
    return info


def upsert_weather_event_markets(repository: Any, event: dict[str, Any]) -> list[WeatherBucketInfo]:
    event_slug = event.get("slug") or event.get("eventSlug") or event.get("event_slug")
    markets = event.get("markets") or []
    discovered: list[WeatherBucketInfo] = []
    for market in markets:
        if not isinstance(market, dict):
            continue
        raw = dict(market)
        if event_slug and not raw.get("eventSlug"):
            raw["eventSlug"] = event_slug
        for bucket in bucket_infos_from_market(raw):
            basket_id = repository.upsert_weather_basket(bucket.basket_row())
            repository.upsert_weather_bucket_market(bucket.bucket_row(basket_id))
            discovered.append(bucket)
    return discovered
