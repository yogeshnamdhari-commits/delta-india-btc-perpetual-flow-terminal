from binance_market import BinanceUSDMFeed


def envelope(event, data):
    return {"data": {"e": event, **data}}


def snapshot_payload(last_id=100):
    return {
        "lastUpdateId": last_id,
        "bids": [["100", "1.5"], ["99", "2.0"]],
        "asks": [["101", "1.0"], ["102", "2.5"]],
    }


def test_aggregate_trade_side_and_cvd_use_buyer_is_maker_flag():
    feed = BinanceUSDMFeed()
    assert feed.ingest_message(envelope("aggTrade", {
        "s": "BTCUSDT", "p": "100.0", "q": "0.25", "m": False, "T": 1760000000000,
    }), received_at=1760000000.0)
    assert feed.ingest_message(envelope("aggTrade", {
        "s": "BTCUSDT", "p": "100.5", "q": "0.10", "m": True, "T": 1760000000100,
    }), received_at=1760000000.1)
    state = feed.snapshot()
    assert state["last_price"] == 100.5
    assert state["buy_qty_btc"] == 0.25
    assert state["sell_qty_btc"] == 0.10
    assert round(state["cvd_btc"], 8) == 0.15
    assert state["recent_trades"][0]["side"] == "SELL TAKER"


def test_book_requires_snapshot_bridge_then_contiguous_previous_update_id():
    feed = BinanceUSDMFeed()
    feed.install_snapshot_payload(snapshot_payload(100))
    assert feed.snapshot()["book_synced"] is False
    assert feed.ingest_message(envelope("depthUpdate", {
        "s": "BTCUSDT", "U": 100, "u": 102, "pu": 99,
        "b": [["100", "0"], ["98", "3.0"]], "a": [["101", "1.2"]],
    }), received_at=1760000000.0)
    state = feed.snapshot()
    assert state["book_synced"] is True
    assert state["book_update_id"] == 102
    assert 100.0 not in state["book_bids"]
    assert state["book_bids"][98.0] == 3.0
    assert state["book_asks"][101.0] == 1.2
    assert state["book_checksum_status"].startswith("not provided")
    assert feed.ingest_message(envelope("depthUpdate", {
        "s": "BTCUSDT", "U": 103, "u": 105, "pu": 102,
        "b": [["98", "0.5"]], "a": [],
    }), received_at=1760000000.1)
    assert feed.snapshot()["book_synced"] is True
    assert feed.snapshot()["book_update_id"] == 105
    assert feed.snapshot()["book_bids"][98.0] == 0.5


def test_gap_invalidates_book_instead_of_preserving_stale_levels():
    feed = BinanceUSDMFeed()
    feed.install_snapshot_payload(snapshot_payload(100))
    assert feed.ingest_message(envelope("depthUpdate", {
        "s": "BTCUSDT", "U": 101, "u": 102, "pu": 100, "b": [], "a": [],
    }))
    assert feed.snapshot()["book_synced"] is True
    before_gaps = feed.snapshot()["book_sequence_gaps"]
    assert feed.ingest_message(envelope("depthUpdate", {
        "s": "BTCUSDT", "U": 108, "u": 110, "pu": 107, "b": [], "a": [],
    }))
    state = feed.snapshot()
    assert state["book_synced"] is False
    assert state["book_bids"] == {}
    assert state["book_asks"] == {}
    assert state["book_sequence_gaps"] == before_gaps + 1
    assert "gap" in state["last_error"].lower()


def test_buffered_depth_events_are_replayed_against_rest_snapshot():
    feed = BinanceUSDMFeed()
    assert feed.ingest_message(envelope("depthUpdate", {
        "s": "BTCUSDT", "U": 98, "u": 101, "pu": 97,
        "b": [["99", "4.0"]], "a": [],
    }))
    feed.install_snapshot_payload(snapshot_payload(100))
    state = feed.snapshot()
    assert state["book_synced"] is True
    assert state["book_update_id"] == 101
    assert state["book_bids"][99.0] == 4.0


def test_kline_candle_uses_exchange_closed_flag():
    feed = BinanceUSDMFeed()
    row = {"s": "BTCUSDT", "k": {
        "i": "1m", "t": 1760000000000, "T": 1760000059999,
        "o": "100", "h": "102", "l": "99", "c": "101",
        "v": "12.5", "q": "1250", "V": "7.5", "x": False,
    }}
    assert feed.ingest_message(envelope("kline", row))
    assert feed.snapshot()["candles_by_interval"]["1m"][0]["is_closed"] is False
    row["k"]["x"] = True
    assert feed.ingest_message(envelope("kline", row))
    assert feed.snapshot()["candles_by_interval"]["1m"][0]["is_closed"] is True


def test_funding_and_sampled_liquidation_fields_are_separate():
    feed = BinanceUSDMFeed()
    assert feed.ingest_message(envelope("markPriceUpdate", {
        "s": "BTCUSDT", "p": "100.5", "i": "100.2", "r": "0.0001",
        "T": 1760000005000, "E": 1760000000000,
    }), received_at=1760000000.0)
    assert feed.snapshot()["funding_rate"] == 0.0001
    assert feed.ingest_message(envelope("forceOrder", {
        "E": 1760000000000,
        "o": {"s": "BTCUSDT", "S": "SELL", "X": "FILLED",
              "ap": "99.9", "p": "99.8", "q": "0.2", "z": "0.2", "T": 1760000000000},
    }), received_at=1760000000.0)
    liquidation = feed.snapshot()["last_liquidation"]
    assert liquidation["side"] == "SELL"
    assert liquidation["qty_btc"] == 0.2
    assert "sampled" in liquidation["semantics"]


def test_invalid_json_wrong_symbols_and_invalid_trades_are_ignored():
    feed = BinanceUSDMFeed()
    assert not feed.ingest_message("{invalid")
    assert not feed.ingest_message(envelope("aggTrade", {
        "s": "ETHUSDT", "p": "2000", "q": "1", "m": False,
    }))
    assert not feed.ingest_message(envelope("aggTrade", {
        "s": "BTCUSDT", "p": "NaN", "q": "1", "m": False,
    }))
    assert feed.snapshot()["trade_count"] == 0


def test_snapshot_is_copy_and_book_is_never_claimed_crc_verified():
    feed = BinanceUSDMFeed()
    feed.install_snapshot_payload(snapshot_payload(100))
    state = feed.snapshot()
    state["book_bids"].clear()
    state["recent_trades"].append({"side": "corrupted"})
    assert feed.snapshot()["book_bids"] == {100.0: 1.5, 99.0: 2.0}
    assert feed.snapshot()["recent_trades"] == []
    assert feed.snapshot()["book_checksum_status"] == "not provided by Binance USDⓈ-M public depth stream"
