"""Read-only OKX public candle-history reference, kept separate from Bybit and Delta.

This adapter supplies an independent historical OHLCV reference only. It does
not provide executions, order book, funding or liquidation data, and its candles
must never be merged into another venue's live series or frozen evaluator.
"""
from __future__ import annotations

import math
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests

REST_BASE_URLS = ("https://openapi.okx.com", "https://www.okx.com")
DEFAULT_INST_ID = "BTC-USDT-SWAP"
TIMEFRAMES = {
    "1m": ("1m", 60),
    "5m": ("5m", 300),
    "15m": ("15m", 900),
    "1h": ("1H", 3600),
}
MAX_CANDLES = 300


def _number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _positive(value: Any) -> float | None:
    number = _number(value)
    return number if number is not None and number > 0 else None


def normalize_okx_candle(row: Any, timeframe: str, *, now_epoch: float | None = None) -> dict[str, Any] | None:
    """Normalize OKX V5 candles: [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]."""
    if timeframe not in TIMEFRAMES or not isinstance(row, (list, tuple)) or len(row) < 9:
        return None
    timestamp_ms = _positive(row[0])
    o, h, low, close = (_number(row[i]) for i in (1, 2, 3, 4))
    volume_contracts = _number(row[5])
    volume_base = _number(row[6])
    turnover_quote = _number(row[7])
    confirm = str(row[8]).strip()
    if (
        timestamp_ms is None or any(v is None for v in (o, h, low, close, volume_contracts, volume_base, turnover_quote))
        or min(o, h, low, close) <= 0
        or volume_contracts < 0 or volume_base < 0 or turnover_quote < 0
        or h < max(o, low, close) or low > min(o, h, close)
        or confirm not in {"0", "1"}
    ):
        return None
    bar, seconds = TIMEFRAMES[timeframe]
    start = int(timestamp_ms // 1000 // seconds) * seconds
    now = time.time() if now_epoch is None else float(now_epoch)
    is_closed = confirm == "1" and start + seconds <= now
    return {
        "time": start,
        "open": o,
        "high": h,
        "low": low,
        "close": close,
        # Use the documented base-currency volume for the BTC-volume ratio.
        # Keep contract volume and quote turnover separately with units.
        "volume": volume_base,
        "volume_btc": volume_base,
        "volume_contracts": volume_contracts,
        "turnover_usdt": turnover_quote,
        "interval": timeframe,
        "is_closed": is_closed,
        "source": "OKX V5 public candles",
    }


class OKXHistoryFeed:
    """Polling-only public market candles; intentionally no account/order methods."""

    def __init__(
        self,
        inst_id: str = DEFAULT_INST_ID,
        *,
        rest_base_urls: tuple[str, ...] = REST_BASE_URLS,
        request_timeout: float = 8.0,
        refresh_seconds: float = 60.0,
    ):
        instrument = str(inst_id).strip().upper()
        if (
            not instrument
            or "/" in instrument
            or instrument.count("-") < 2
            or not instrument.endswith("-SWAP")
        ):
            raise ValueError("inst_id must be a linear/swap instrument such as BTC-USDT-SWAP")
        self.inst_id = instrument
        self.rest_base_urls = tuple(dict.fromkeys(str(x).rstrip("/") for x in rest_base_urls if x))
        if not self.rest_base_urls:
            raise ValueError("at least one documented OKX public REST host is required")
        self.request_timeout = float(request_timeout)
        self.refresh_seconds = max(15.0, float(refresh_seconds))
        self.lock = threading.RLock()
        self.data: dict[str, Any] = {
            "provider": "OKX V5 public candle reference",
            "inst_id": self.inst_id,
            "connection": "not_started",
            "rest_endpoint_active": None,
            "rest_endpoint_health": "not_checked",
            "last_error": "",
            "started_at": time.time(),
            "last_success_at": None,
            "last_poll_at": None,
            "poll_count": 0,
            "candles_by_interval": {tf: {} for tf in TIMEFRAMES},
            "history_loaded": {tf: False for tf in TIMEFRAMES},
            "history_updated_at": {},
            "history_source_by_interval": {},
            "history_errors": {tf: "" for tf in TIMEFRAMES},
        }
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._poll_forever, name="okx-public-candle-history", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            snap = dict(self.data)
            snap["candles_by_interval"] = {
                tf: [dict(candle) for _, candle in sorted(rows.items())]
                for tf, rows in self.data["candles_by_interval"].items()
            }
            snap["history_loaded"] = dict(self.data["history_loaded"])
            snap["history_updated_at"] = dict(self.data["history_updated_at"])
            snap["history_source_by_interval"] = dict(self.data["history_source_by_interval"])
            snap["history_errors"] = dict(self.data["history_errors"])
            return snap

    def analyst_context(self, *, now_epoch: float | None = None) -> dict[str, Any]:
        now = time.time() if now_epoch is None else float(now_epoch)
        snap = self.snapshot()
        return {
            "venue": "OKX",
            "instrument": self.inst_id,
            "data_role": "independent historical candle reference only; not merged into Bybit or Delta",
            "observed_at_utc": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "rest_endpoint_health": snap.get("rest_endpoint_health"),
            "active_rest_host": snap.get("rest_endpoint_active"),
            "last_success_age_seconds": (
                round(max(0.0, now - float(snap["last_success_at"])), 2)
                if snap.get("last_success_at") else None
            ),
            "history": {
                timeframe: {
                    "loaded": bool(snap["history_loaded"].get(timeframe)),
                    "closed_candles": sum(1 for row in snap["candles_by_interval"].get(timeframe, []) if row.get("is_closed")),
                    "latest_closed_candle_time_utc": (
                        datetime.fromtimestamp(
                            max(float(row["time"]) for row in snap["candles_by_interval"].get(timeframe, []) if row.get("is_closed")),
                            tz=timezone.utc,
                        ).isoformat()
                        if any(row.get("is_closed") for row in snap["candles_by_interval"].get(timeframe, []))
                        else None
                    ),
                    "updated_at_utc": (
                        datetime.fromtimestamp(float(snap["history_updated_at"][timeframe]), tz=timezone.utc).isoformat()
                        if snap.get("history_updated_at", {}).get(timeframe) else None
                    ),
                    "source_host": snap.get("history_source_by_interval", {}).get(timeframe),
                    "error": snap.get("history_errors", {}).get(timeframe) or None,
                }
                for timeframe in TIMEFRAMES
            },
            "limitations": [
                "Only independent OKX public OHLCV candle snapshots are provided; this source has no Bybit/Delta order book, trades, funding or liquidation stream.",
                "OKX history is not merged into Bybit or Delta candles and is not substituted into another venue's signal engine, backtest or release evaluator.",
                "The in-memory set is limited to the latest 300 candles per timeframe and is refreshed by polling.",
            ],
        }

    def _get_candles(self, timeframe: str, bar: str) -> tuple[list[dict[str, Any]], str]:
        errors: list[str] = []
        for base in self.rest_base_urls:
            try:
                response = requests.get(
                    f"{base}/api/v5/market/candles",
                    params={"instId": self.inst_id, "bar": bar, "limit": str(MAX_CANDLES)},
                    headers={"Accept": "application/json", "User-Agent": "ReadOnlyMarketMonitor/1.0"},
                    timeout=self.request_timeout,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("OKX REST returned a non-object JSON response")
                code = payload.get("code")
                if code != "0":
                    raise ValueError(f"OKX REST code={code}: {payload.get('msg', 'unknown API error')}")
                result = payload.get("data")
                if not isinstance(result, list):
                    raise ValueError("OKX candle response lacks a data array")
                now = time.time()
                candles = [
                    normalized
                    for row in result
                    if (normalized := normalize_okx_candle(row, timeframe, now_epoch=now)) is not None
                ]
                if not candles:
                    raise ValueError(f"OKX returned no valid {timeframe} candle rows")
                candles.sort(key=lambda row: row["time"])
                return candles[-MAX_CANDLES:], base
            except Exception as exc:
                errors.append(f"{base}: {type(exc).__name__}: {str(exc)[:180]}")
        raise RuntimeError("All configured OKX public candle hosts failed: " + " | ".join(errors))

    def poll_once(self) -> dict[str, Any]:
        """Refresh each timeframe once; returns per-timeframe success flags for diagnostics/tests."""
        results: dict[str, bool] = {}
        errors: dict[str, str] = {}
        now = time.time()
        with self.lock:
            self.data["last_poll_at"] = now
            self.data["poll_count"] += 1
            self.data["connection"] = "polling"
        active_base = None
        for timeframe, (bar, _seconds) in TIMEFRAMES.items():
            try:
                candles, base = self._get_candles(timeframe, bar)
                active_base = base
                with self.lock:
                    target = self.data["candles_by_interval"][timeframe]
                    for candle in candles:
                        target[int(candle["time"])] = candle
                    for key in sorted(target)[:-MAX_CANDLES]:
                        target.pop(key, None)
                    self.data["history_loaded"][timeframe] = True
                    self.data["history_updated_at"][timeframe] = time.time()
                    self.data["history_source_by_interval"][timeframe] = base
                    self.data["history_errors"][timeframe] = ""
                results[timeframe] = True
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"[:450]
                errors[timeframe] = error
                results[timeframe] = False
                with self.lock:
                    self.data["history_errors"][timeframe] = error
        with self.lock:
            self.data["rest_endpoint_active"] = active_base or self.data.get("rest_endpoint_active")
            poll_success_count = sum(1 for ok in results.values() if ok)
            loaded_count = sum(1 for ok in self.data["history_loaded"].values() if ok)
            if poll_success_count == len(TIMEFRAMES):
                self.data["rest_endpoint_health"] = "healthy"
                self.data["connection"] = "connected"
                self.data["last_success_at"] = time.time()
                self.data["last_error"] = ""
            elif poll_success_count > 0:
                self.data["rest_endpoint_health"] = "partial"
                self.data["connection"] = "partial"
                self.data["last_success_at"] = time.time()
                self.data["last_error"] = "; ".join(f"{k}: {v}" for k, v in errors.items())[:500]
            else:
                # Previously loaded candles remain available for display, but a failed
                # poll must not refresh the last-success timestamp or imply fresh data.
                self.data["rest_endpoint_health"] = "stale" if loaded_count else "unavailable"
                self.data["connection"] = "stale" if loaded_count else "unavailable"
                self.data["last_error"] = "; ".join(f"{k}: {v}" for k, v in errors.items())[:500]
        return results

    def _poll_forever(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                self.poll_once()
            except Exception as exc:
                with self.lock:
                    self.data["connection"] = "unavailable"
                    self.data["rest_endpoint_health"] = "unavailable"
                    self.data["last_error"] = f"{type(exc).__name__}: {exc}"[:500]
            elapsed = time.monotonic() - started
            self._stop.wait(max(2.0, self.refresh_seconds - elapsed))
