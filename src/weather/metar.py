from __future__ import annotations

from typing import Any
from datetime import datetime
from zoneinfo import ZoneInfo

from src.utils.dedupe import raw_json
from src.utils.time import to_iso, utc_now_iso


DEFAULT_CITY_STATIONS: dict[str, dict[str, Any]] = {
    "Amsterdam": {"station_id": "EHAM", "station_name": "Amsterdam Schiphol", "timezone": "Europe/Amsterdam"},
    "Ankara": {"station_id": "LTAC", "station_name": "Ankara Esenboga", "timezone": "Europe/Istanbul"},
    "Atlanta": {"station_id": "KATL", "station_name": "Atlanta Hartsfield-Jackson", "timezone": "America/New_York"},
    "Austin": {"station_id": "KAUS", "station_name": "Austin-Bergstrom", "timezone": "America/Chicago"},
    "Beijing": {"station_id": "ZBAA", "station_name": "Beijing Capital", "timezone": "Asia/Shanghai"},
    "Buenos Aires": {"station_id": "SABE", "station_name": "Buenos Aires Aeroparque", "timezone": "America/Argentina/Buenos_Aires"},
    "Busan": {"station_id": "RKPK", "station_name": "Busan Gimhae", "timezone": "Asia/Seoul"},
    "Cape Town": {"station_id": "FACT", "station_name": "Cape Town International", "timezone": "Africa/Johannesburg"},
    "Chengdu": {"station_id": "ZUUU", "station_name": "Chengdu Shuangliu", "timezone": "Asia/Shanghai"},
    "Chicago": {"station_id": "KORD", "station_name": "Chicago O'Hare", "timezone": "America/Chicago"},
    "Chongqing": {"station_id": "ZUCK", "station_name": "Chongqing Jiangbei", "timezone": "Asia/Shanghai"},
    "Guangzhou": {"station_id": "ZGGG", "station_name": "Guangzhou Baiyun", "timezone": "Asia/Shanghai"},
    "Helsinki": {"station_id": "EFHK", "station_name": "Helsinki-Vantaa", "timezone": "Europe/Helsinki"},
    "Hong Kong": {"station_id": "VHHH", "station_name": "Hong Kong International", "timezone": "Asia/Hong_Kong"},
    "Houston": {"station_id": "KIAH", "station_name": "Houston Intercontinental", "timezone": "America/Chicago"},
    "Istanbul": {"station_id": "LTFM", "station_name": "Istanbul Airport", "timezone": "Europe/Istanbul"},
    "Jakarta": {"station_id": "WIII", "station_name": "Jakarta Soekarno-Hatta", "timezone": "Asia/Jakarta"},
    "Jeddah": {"station_id": "OEJN", "station_name": "Jeddah King Abdulaziz", "timezone": "Asia/Riyadh"},
    "Kuala Lumpur": {"station_id": "WMKK", "station_name": "Kuala Lumpur International", "timezone": "Asia/Kuala_Lumpur"},
    "Lagos": {"station_id": "DNMM", "station_name": "Lagos Murtala Muhammed", "timezone": "Africa/Lagos"},
    "London": {"station_id": "EGLL", "station_name": "London Heathrow", "timezone": "Europe/London"},
    "Los Angeles": {"station_id": "KLAX", "station_name": "Los Angeles International", "timezone": "America/Los_Angeles"},
    "Madrid": {"station_id": "LEMD", "station_name": "Madrid Barajas", "timezone": "Europe/Madrid"},
    "Mexico City": {"station_id": "MMMX", "station_name": "Mexico City International", "timezone": "America/Mexico_City"},
    "Miami": {"station_id": "KMIA", "station_name": "Miami International", "timezone": "America/New_York"},
    "Milan": {"station_id": "LIMC", "station_name": "Milan Malpensa", "timezone": "Europe/Rome"},
    "Moscow": {"station_id": "UUEE", "station_name": "Moscow Sheremetyevo", "timezone": "Europe/Moscow"},
    "Munich": {"station_id": "EDDM", "station_name": "Munich Airport", "timezone": "Europe/Berlin"},
    "New York City": {"station_id": "KNYC", "station_name": "New York Central Park/City", "timezone": "America/New_York"},
    "Panama City": {"station_id": "MPTO", "station_name": "Panama City Tocumen", "timezone": "America/Panama"},
    "Paris": {"station_id": "LFPG", "station_name": "Paris Charles de Gaulle", "timezone": "Europe/Paris"},
    "San Francisco": {"station_id": "KSFO", "station_name": "San Francisco International", "timezone": "America/Los_Angeles"},
    "Sao Paulo": {"station_id": "SBGR", "station_name": "Sao Paulo Guarulhos", "timezone": "America/Sao_Paulo"},
    "Seattle": {"station_id": "KSEA", "station_name": "Seattle-Tacoma", "timezone": "America/Los_Angeles"},
    "Seoul": {"station_id": "RKSI", "station_name": "Seoul Incheon", "timezone": "Asia/Seoul"},
    "Shanghai": {"station_id": "ZSPD", "station_name": "Shanghai Pudong", "timezone": "Asia/Shanghai"},
    "Shenzhen": {"station_id": "ZGSZ", "station_name": "Shenzhen Bao'an", "timezone": "Asia/Shanghai"},
    "Singapore": {"station_id": "WSSS", "station_name": "Singapore Changi", "timezone": "Asia/Singapore"},
    "Taipei": {"station_id": "RCTP", "station_name": "Taipei Taoyuan", "timezone": "Asia/Taipei"},
    "Tel Aviv": {"station_id": "LLBG", "station_name": "Tel Aviv Ben Gurion", "timezone": "Asia/Jerusalem"},
    "Tokyo": {"station_id": "RJTT", "station_name": "Tokyo Haneda", "timezone": "Asia/Tokyo"},
    "Toronto": {"station_id": "CYYZ", "station_name": "Toronto Pearson", "timezone": "America/Toronto"},
    "Warsaw": {"station_id": "EPWA", "station_name": "Warsaw Chopin", "timezone": "Europe/Warsaw"},
    "Wellington": {"station_id": "NZWN", "station_name": "Wellington International", "timezone": "Pacific/Auckland"},
    "Wuhan": {"station_id": "ZHHH", "station_name": "Wuhan Tianhe", "timezone": "Asia/Shanghai"},
}


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def seed_known_station_mappings(repository: Any) -> int:
    with repository.connect() as conn:
        cities = [
            row["city"]
            for row in conn.execute(
                """
                SELECT DISTINCT city
                FROM weather_baskets
                WHERE city IS NOT NULL
                ORDER BY city
                """
            ).fetchall()
        ]
    inserted = 0
    for city in cities:
        station = DEFAULT_CITY_STATIONS.get(city)
        if not station:
            continue
        repository.upsert_weather_station_mapping(
            {
                "city": city,
                "station_id": station["station_id"],
                "station_name": station.get("station_name"),
                "latitude": station.get("latitude"),
                "longitude": station.get("longitude"),
                "timezone": station.get("timezone"),
                "mapping_confidence": "likely_city_airport_station",
                "source": "static_station_seed",
                "notes": "Review against Polymarket settlement source before treating as official.",
                "raw_json": raw_json(station),
            }
        )
        inserted += 1
    return inserted


def normalize_metar_report(payload: dict[str, Any], *, city: str | None, first_seen_at: str) -> dict[str, Any]:
    station_id = str(payload.get("icaoId") or payload.get("station_id") or payload.get("stationId") or "").upper()
    raw_text = str(payload.get("rawOb") or payload.get("raw_text") or payload.get("rawText") or "").strip()
    report_time = to_iso(payload.get("obsTime") or payload.get("reportTime") or payload.get("receiptTime"))
    if not raw_text:
        raw_text = f"{station_id} {report_time or first_seen_at}".strip()
    report_type = "SPECI" if raw_text.upper().startswith("SPECI") else "METAR"
    if raw_text.upper().startswith("METAR"):
        report_type = "METAR"
    quality_flags = ["public_metar", "first_seen_by_local_poller"]
    seen_dt = _parse_dt(first_seen_at)
    report_dt = _parse_dt(report_time)
    if seen_dt and report_dt and (seen_dt - report_dt).total_seconds() > 600:
        quality_flags.append("initial_recent_history_not_true_release_time")
    return {
        "station_id": station_id,
        "city": city,
        "source": "aviationweather",
        "report_type": payload.get("reportType") or report_type,
        "report_time": report_time,
        "first_seen_at": first_seen_at,
        "raw_text": raw_text,
        "temperature_c": _num(_first_present(payload, "temp", "temp_c", "temperature")),
        "dewpoint_c": _num(_first_present(payload, "dewp", "dewpoint", "dewpoint_c")),
        "wind_direction": _num(_first_present(payload, "wdir", "wind_direction")),
        "wind_speed_kt": _num(_first_present(payload, "wspd", "wind_speed_kt")),
        "visibility_statute_mi": _num(_first_present(payload, "visib", "visibility_statute_mi")),
        "altimeter_in_hg": _num(_first_present(payload, "altim", "altimeter_in_hg")),
        "quality_flags": ",".join(quality_flags),
        "raw_json": raw_json(payload),
    }


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
    return parsed


async def capture_metars_for_active_baskets(repository: Any, client: Any) -> int:
    seed_known_station_mappings(repository)
    mappings = [dict(row) for row in repository.list_weather_station_mappings_for_active_baskets()]
    station_ids = sorted({str(row.get("station_id") or "").upper() for row in mappings if row.get("station_id")})
    if not station_ids:
        repository.log("WARN", "weather_metar", "no station mappings available for active weather baskets")
        return 0
    city_by_station = {str(row["station_id"]).upper(): row.get("city") for row in mappings}
    first_seen_at = utc_now_iso()
    reports = await client.get_metars(station_ids)
    inserted = 0
    for payload in reports:
        station_id = str(payload.get("icaoId") or payload.get("station_id") or payload.get("stationId") or "").upper()
        if not station_id:
            continue
        report = normalize_metar_report(payload, city=city_by_station.get(station_id), first_seen_at=first_seen_at)
        _, was_inserted = repository.insert_weather_metar_report(report)
        if was_inserted:
            inserted += 1
    repository.log("INFO", "weather_metar", "METAR capture completed", {"stations": len(station_ids), "inserted": inserted})
    return inserted
