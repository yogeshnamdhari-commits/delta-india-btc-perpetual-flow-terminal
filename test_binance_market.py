from unittest.mock import Mock, patch

from binance_market import BinanceUSDMFeed


def envelope(event, data):
    return {"data": {"e": event, **data}}


def snapshot_payload(last_id=100):
    return {
        "lastUpdateId": last_id,
        "bids": [["100", "1.5"], ["99", "2.0"]],
        "asks": [["101", "1.0"], ["102", "2.5"]],
    }



def test_depth_rest_stops_host_fallback_on_explicit_http_451():
    feed = BinanceUSDMFeed()
    response = Mock()
    response.status_code = 451
    session = Mock()
    session.get.return_value = response

    with patch("binance_market.requests.Session", return_value=session):
        try:
            feed._fetch_depth_snapshot()
        except RuntimeError as exc:
            assert "HTTP 451" in str(exc)
            assert "stopping REST host fallback" in str(exc)
        else:
            raise AssertionError("Expected regional/access denial to stop depth REST host fallback")

    assert session.get.call_count == 1
    session.close.assert_called_once()


def test_open_interest_rest_stops_host_fallback_on_explicit_http_403():
    feed = BinanceUSDMFeed()
    response = Mock()
    response.status_code = 403
    session = Mock()
    session.get.return_value = response
    stop_event = Mock()
    stop_event.is_set.side_effect = [False, False, False, True]
    feed._stop_event = stop_event

    with patch("binance_market.requests.Session", return_value=session):
        feed._poll_open_interest()

    assert session.get.call_count == 1
    assert "HTTP 403" in feed.snapshot()["open_interest_error"]
    session.close.assert_called_once()


def test_websocket_streams_use_current_binance_public_and_market_routes():
    feed = BinanceUSDMFeed()
    public_url = feed.public_ws_url
    market_url = feed.market_ws_url

    assert public_url.startswith("wss://fstream.binance.com/public/stream?streams=")
    assert "btcusdt@depth@100ms" in public_url
    assert "btcusdt@bookTicker" in public_url
    assert "@aggTrade" not in public_url
    assert market_url.startswith("wss://fstream.binance.com/market/stream?streams=")
    for stream in (
        "btcusdt@aggTrade",
        "btcusdt@markPrice@1s",
        "btcusdt@forceOrder",
        "btcusdt@ticker",
        "btcusdt@kline_1m",
        "btcusdt@kline_5m",
        "btcusdt@kline_15m",
        "btcusdt@kline_1h",
    ):
        assert stream in market_url
    assert "/stream?streams=" not in public_url.replace("/public/stream?streams=", "")
    assert "/stream?streams=" not in market_url.replace("/market/stream?streams=", "")

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
    state = feed.snapshot()
    assert state["trade_count"] == 0
    assert state["total_messages_received"] == 0
    assert state["last_market_message_at"] is None


def test_snapshot_is_copy_and_book_is_never_claimed_crc_verified():
    feed = BinanceUSDMFeed()
    feed.install_snapshot_payload(snapshot_payload(100))
    state = feed.snapshot()
    state["book_bids"].clear()
    state["recent_trades"].append({"side": "corrupted"})
    assert feed.snapshot()["book_bids"] == {100.0: 1.5, 99.0: 2.0}
    assert feed.snapshot()["recent_trades"] == []
    assert feed.snapshot()["book_checksum_status"] == "not provided by Binance USDⓈ-M public depth stream"
