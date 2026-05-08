from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.utils.dedupe import trade_dedupe_key


def test_dedupe_key_is_stable_for_same_trade() -> None:
    one = {
        "proxyWallet": "0xABC",
        "transactionHash": "0xTX",
        "conditionId": "0xCOND",
        "asset": "123",
        "side": "BUY",
        "timestamp": 1710000000,
        "price": "0.5000",
        "size": "10.00",
        "outcome": "Yes",
    }
    two = {
        "outcome": "YES",
        "size": 10,
        "price": 0.5,
        "timestamp": "1710000000",
        "side": "BUY",
        "asset": "123",
        "conditionId": "0xcond",
        "transactionHash": "0xtx",
        "proxyWallet": "0xabc",
    }
    assert trade_dedupe_key(one) == trade_dedupe_key(two)


def test_dedupe_key_changes_for_distinct_fill() -> None:
    base = {
        "proxyWallet": "0xABC",
        "transactionHash": "0xTX",
        "conditionId": "0xCOND",
        "asset": "123",
        "side": "BUY",
        "timestamp": 1710000000,
        "price": "0.50",
        "size": "10",
        "outcome": "Yes",
    }
    changed = dict(base, size="11")
    assert trade_dedupe_key(base) != trade_dedupe_key(changed)

