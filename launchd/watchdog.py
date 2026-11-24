"""Independent launchd watchdog for the read-only Delta BTC terminal."""
from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def runtime_failure_reasons(
    *,
    http_ok: bool,
    heartbeat: Any,
    now_epoch: float,
    max_heartbeat_age_seconds: float = 120.0,
) -> list[str]:
    """Return process/worker failures; exchange disconnection alone is not fatal."""
    reasons: list[str] = []
    if not http_ok:
        reasons.append("dashboard_health_endpoint_unavailable")
    if not isinstance(heartbeat, dict):
        reasons.append("feed_worker_heartbeat_missing_or_invalid")
        return reasons

    try:
        heartbeat_epoch = float(heartbeat.get("ts_epoch"))
    except (TypeError, ValueError, OverflowError):
        heartbeat_epoch = 0.0
    age = now_epoch - heartbeat_epoch
    if heartbeat_epoch <= 0 or age < -5 or age > max_heartbeat_age_seconds:
        reasons.append("feed_worker_heartbeat_stale")
    if heartbeat.get("websocket_worker_alive") is not True:
        reasons.append("websocket_worker_not_alive")
    if heartbeat.get("sampler_worker_alive") is not True:
        reasons.append("depth_sampler_worker_not_alive")
    return reasons


def restart_due(
    *,
    consecutive_failures: int,
    last_restart_epoch: float,
    now_epoch: float,
    failure_threshold: int = 3,
    cooldown_seconds: float = 600.0,
) -> bool:
    if consecutive_failures < failure_threshold:
        return False
    return (now_epoch - last_restart_epoch) >= cooldown_seconds


def _http_ok(url: str, timeout: float = 5.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status == 200
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def _read_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with temp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, allow_nan=False, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def main() -> int:
    now = time.time()
    health_url = os.environ.get(
        "DELTA_DASHBOARD_HEALTH_URL", "http://127.0.0.1:8502/_stcore/health"
    )
    heartbeat_path = Path(os.environ.get(
        "DELTA_FEED_HEALTH_FILE",
        str(Path.home() / "Library/Application Support/DeltaBTCTerminal/feed_health.json"),
    )).expanduser()
    state_path = Path(os.environ.get(
        "DELTA_WATCHDOG_STATE_FILE",
        str(Path.home() / "Library/Application Support/DeltaBTCTerminal/watchdog_state.json"),
    )).expanduser()
    label = os.environ.get("DELTA_MAIN_AGENT_LABEL", "com.yogesh.delta-btc-terminal")
    threshold = int(os.environ.get("DELTA_WATCHDOG_FAILURE_THRESHOLD", "3"))
    cooldown = float(os.environ.get("DELTA_WATCHDOG_RESTART_COOLDOWN_SECONDS", "600"))

    old_state = _read_json(state_path)
    if not isinstance(old_state, dict):
        old_state = {}
    failures = max(0, int(old_state.get("consecutive_failures", 0)))
    last_restart = float(old_state.get("last_restart_epoch", 0.0))
    heartbeat = _read_json(heartbeat_path)
    http_ok = _http_ok(health_url)
    reasons = runtime_failure_reasons(
        http_ok=http_ok, heartbeat=heartbeat, now_epoch=now
    )
    failures = failures + 1 if reasons else 0

    state = {
        "last_check_epoch": now,
        "consecutive_failures": failures,
        "last_restart_epoch": last_restart,
        "last_failure_reasons": reasons,
    }

    if reasons:
        print("Runtime watchdog failure: " + ", ".join(reasons), flush=True)
    elif isinstance(heartbeat, dict):
        # A disconnected exchange is an upstream availability problem. Do not
        # restart a live collector for it; its own WebSocket loop must reconnect.
        print(
            "Runtime watchdog OK; exchange_connection="
            + str(heartbeat.get("connection", "unknown"))
            + "; book_synced="
            + str(heartbeat.get("book_synced", False)),
            flush=True,
        )

    if restart_due(
        consecutive_failures=failures,
        last_restart_epoch=last_restart,
        now_epoch=now,
        failure_threshold=threshold,
        cooldown_seconds=cooldown,
    ):
        uid = os.getuid()
        target = f"gui/{uid}/{label}"
        result = subprocess.run(
            ["/bin/launchctl", "kickstart", "-k", target],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            print(f"Requested supervised restart: {target}", flush=True)
            state["last_restart_epoch"] = now
            state["consecutive_failures"] = 0
        else:
            print(
                f"Failed to request restart for {target}: "
                f"{result.stderr.strip() or result.stdout.strip()}",
                flush=True,
            )

    try:
        _write_json_atomic(state_path, state)
    except OSError as exc:
        print(f"Unable to persist watchdog state: {type(exc).__name__}: {exc}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
