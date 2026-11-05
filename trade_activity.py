"""Read-only summaries of captured public execution prints."""

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any


def summarize_trade_prints(
    trade_log: Iterable[Any] | None,
    contract_value_btc: float = 0.001,
) -> list[dict[str, float | int | str]]:
    """Summarize retained public trade prints by aggressor side.

    Each app trade-log record has the shape (timestamp, price, contracts, side).
    The count is a count of classified execution messages, not unique parent orders.
    USD notional is estimated as contracts × BTC per contract × execution price.
    Invalid, non-positive, and unclassified records are ignored.
    """
    try:
        contract_btc = float(contract_value_btc)
    except (TypeError, ValueError) as exc:
        raise ValueError("contract_value_btc must be a finite positive number") from exc
    if not math.isfinite(contract_btc) or contract_btc <= 0:
        raise ValueError("contract_value_btc must be a finite positive number")

    totals: dict[str, dict[str, float | int]] = {
        "buy": {"count": 0, "contracts": 0.0, "btc": 0.0, "usd": 0.0},
        "sell": {"count": 0, "contracts": 0.0, "btc": 0.0, "usd": 0.0},
    }

    for record in trade_log or ():
        if not isinstance(record, (tuple, list)) or len(record) < 4:
            continue
        _, raw_price, raw_contracts, raw_side = record[:4]
        side = str(raw_side).strip().lower()
        if side not in totals:
            continue
        try:
            price = float(raw_price)
            contracts = float(raw_contracts)
        except (TypeError, ValueError):
            continue
        if (
            not math.isfinite(price)
            or not math.isfinite(contracts)
            or price <= 0
            or contracts <= 0
        ):
            continue

        btc = contracts * contract_btc
        usd = btc * price
        side_total = totals[side]
        side_total["count"] += 1
        side_total["contracts"] += contracts
        side_total["btc"] += btc
        side_total["usd"] += usd

    return [
        {
            "Side": side.upper(),
            "Trade prints": int(totals[side]["count"]),
            "Contracts": float(totals[side]["contracts"]),
            "BTC equivalent": float(totals[side]["btc"]),
            "Approx USD value": float(totals[side]["usd"]),
        }
        for side in ("buy", "sell")
    ]
