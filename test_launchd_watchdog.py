from launchd.watchdog import restart_due, runtime_failure_reasons


def _healthy_heartbeat(**overrides):
    payload = {
        "ts_epoch": 1000.0,
        "websocket_worker_alive": True,
        "sampler_worker_alive": True,
        "connection": "connected",
        "book_synced": True,
    }
    payload.update(overrides)
    return payload


def test_runtime_health_accepts_live_workers_even_when_exchange_is_disconnected():
    reasons = runtime_failure_reasons(
        http_ok=True,
        heartbeat=_healthy_heartbeat(connection="reconnecting", book_synced=False),
        now_epoch=1010.0,
    )
    assert reasons == []


def test_runtime_health_detects_dead_websocket_worker():
    reasons = runtime_failure_reasons(
        http_ok=True,
        heartbeat=_healthy_heartbeat(websocket_worker_alive=False),
        now_epoch=1010.0,
    )
    assert reasons == ["websocket_worker_not_alive"]


def test_runtime_health_detects_stale_heartbeat_and_dashboard_failure():
    reasons = runtime_failure_reasons(
        http_ok=False,
        heartbeat=_healthy_heartbeat(ts_epoch=800.0),
        now_epoch=1000.0,
    )
    assert "dashboard_health_endpoint_unavailable" in reasons
    assert "feed_worker_heartbeat_stale" in reasons


def test_runtime_health_rejects_missing_heartbeat_and_dead_sampler():
    reasons = runtime_failure_reasons(
        http_ok=True,
        heartbeat=_healthy_heartbeat(sampler_worker_alive=False),
        now_epoch=1010.0,
    )
    assert reasons == ["depth_sampler_worker_not_alive"]
    assert "feed_worker_heartbeat_missing_or_invalid" in runtime_failure_reasons(
        http_ok=True, heartbeat=None, now_epoch=1010.0
    )


def test_restart_requires_failure_threshold_and_respects_cooldown():
    assert not restart_due(
        consecutive_failures=2, last_restart_epoch=0, now_epoch=1000
    )
    assert restart_due(
        consecutive_failures=3, last_restart_epoch=0, now_epoch=1000
    )
    assert not restart_due(
        consecutive_failures=3, last_restart_epoch=950, now_epoch=1000,
        cooldown_seconds=600,
    )
