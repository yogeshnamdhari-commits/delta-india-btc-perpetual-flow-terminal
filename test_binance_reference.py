import json

from binance_reference import BinanceReferenceFeed


def envelope(event, data, stream=None):
    result = {"e": event, **data}
    payload = {"data": result}
    if stream:
        payload["stream"] = stream
    return payload


def test_aggregate_trade_side_and_cvd_are_classified_from_buyer_is_maker():
    feed = BinanceReferenceFeed()
    assert feed.ingest_message(
        envelope("aggTrade", {"s": "BTCUSDT", "p": "100000", "q": "0.25", "m": False, "T": 1760000000000}),
        received_at=1760000000.0,
    )
    assert feed.ingest_message(
        envelope("aggTrade", {"s": "BTCUSDT", "p": "100001", "q": "0.10", "m": True, "T": 1760000000100}),
        received_at=1760000000.1,
    )
    state = feed.snapshot()
    assert state["last_price"] == 100001.0
    assert state["buy_qty_btc"] == 0.25
    assert state["sell_qty_btc"] == 0.10
    assert round(state["cvd_btc"], 8) == 0.15
    assert state["recent_trades"][0]["side"] == "SELL TAKER"


def test_partial_depth_snapshot_replaces_levels_and_marks_book_ready():
    feed = BinanceReferenceFeed()
    payload = envelope("depthUpdate", {
        "s": "BTCUSDT",
        "b": [["99990.0", "1.2"], ["99980.0", "0.8"]],
        "a": [["100010.0", "1.0"], ["100020.0", "1.1"]],
        "E": 1760000000000,
    })
    assert feed.ingest_message(payload, received_at=1760000000.0)
    state = feed.snapshot()
    assert state["book_ready"] is True
    assert state["bids"] == {99990.0: 1.2, 99980.0: 0.8}
    assert state["asks"] == {100010.0: 1.0, 100020.0: 1.1}


def test_mark_price_funding_and_next_funding_are_parsed():
    feed = BinanceReferenceFeed()
    assert feed.ingest_message(envelope("markPriceUpdate", {
        "s": "BTCUSDT", "p": "100005.0", "i": "100000.0", "r": "0.0001",
        "T": 1760000005000, "E": 1760000000000,
    }), received_at=1760000000.0)
    state = feed.snapshot()
    assert state["mark_price"] == 100005.0
    assert state["index_price"] == 100000.0
    assert state["funding_rate"] == 0.0001
    assert state["next_funding_ms"] == 1760000005000


def test_latest_force_order_is_stored_as_a_single_sample():
    feed = BinanceReferenceFeed()
    assert feed.ingest_message(envelope("forceOrder", {
        "E": 1760000000000,
        "o": {"s": "BTCUSDT", "S": "SELL", "X": "FILLED", "ap": "99900", "p": "99900",
              "q": "0.2", "z": "0.2", "T": 1760000000000},
    }), received_at=1760000000.0)
    assert feed.snapshot()["last_liquidation"]["side"] == "SELL"
    assert feed.snapshot()["last_liquidation"]["qty_btc"] == 0.2


def test_wrong_symbol_malformed_json_and_nonmarket_events_are_ignored():
    feed = BinanceReferenceFeed()
    assert not feed.ingest_message("{not-json")
    assert not feed.ingest_message(envelope("aggTrade", {
        "s": "ETHUSDT", "p": "2000", "q": "1", "m": False,
    }))
    assert not feed.ingest_message(envelope("unknownEvent", {"s": "BTCUSDT"}))
    assert feed.snapshot()["trade_count"] == 0


def test_snapshot_does_not_expose_mutable_internal_books_or_trade_buffers():
    feed = BinanceReferenceFeed()
    feed.ingest_message(envelope("depthUpdate", {
        "s": "BTCUSDT", "b": [["99990", "1"]], "a": [["100010", "1"]],
    }), received_at=1760000000.0)
    state = feed.snapshot()
    state["bids"].clear()
    state["recent_trades"].append({"side": "corrupted"})
    after = feed.snapshot()
    assert after["bids"] == {99990.0: 1.0}
    assert after["recent_trades"] == []
