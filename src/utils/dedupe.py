from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from typing import Any


def _clean_decimal(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return str(value).strip().lower()
    return format(dec.normalize(), "f")


def _field(raw: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in raw and raw[name] not in (None, ""):
            return raw[name]
    return ""


def trade_dedupe_payload(raw: dict[str, Any]) -> dict[str, str]:
    tx_hash = str(_field(raw, "transactionHash", "transaction_hash", "tx_hash")).lower()
    external_id = str(_field(raw, "id", "trade_id", "external_trade_id")).lower()
    wallet = str(_field(raw, "proxyWallet", "proxy_wallet", "user", "wallet")).lower()
    condition = str(_field(raw, "conditionId", "condition_id", "market")).lower()
    token = str(_field(raw, "asset", "asset_id", "token_id")).lower()
    return {
        "external_id": external_id,
        "tx_hash": tx_hash,
        "wallet": wallet,
        "condition": condition,
        "token": token,
        "side": str(_field(raw, "side")).upper(),
        "timestamp": str(_field(raw, "timestamp", "matchtime", "trade_timestamp")),
        "price": _clean_decimal(_field(raw, "price")),
        "size": _clean_decimal(_field(raw, "size")),
        "outcome": str(_field(raw, "outcome")).strip().lower(),
    }


def trade_dedupe_key(raw: dict[str, Any]) -> str:
    payload = trade_dedupe_payload(raw)
    compact = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(compact.encode("utf-8")).hexdigest()


def raw_json(raw: Any) -> str:
    return json.dumps(raw, sort_keys=True, default=str, separators=(",", ":"))

