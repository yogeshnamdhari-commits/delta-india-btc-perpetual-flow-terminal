#!/usr/bin/env python3
"""Read-only acceptance audit for a real Streamlit diagnostic reconnect.

The audit consumes event/decision JSONL logs produced by app.py. It never
opens a socket, changes application state, or submits orders. A PASS is evidence
from one logged diagnostic run; it is not OOS profitability certification.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

UTC = timezone.utc
REQUIRED_CHANNELS = ("ticker", "ob_updates", "trades")


def parse_timestamp(value: Any) -> datetime | None:
    """Parse ISO-8601 or Unix timestamps (s, ms, us, ns) as UTC."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if number <= 0:
            return None
        if number >= 1e17:
            number /= 1e9
        elif number >= 1e14:
            number /= 1e6
        elif number >= 1e11:
            number /= 1e3
        try:
            return datetime.fromtimestamp(number, UTC)
        except (OverflowError, OSError, ValueError):
            return None
    elif isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def load_jsonl(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    """Read JSONL records without editing source evidence."""
    records: list[dict[str, Any]] = []
    errors: list[str] = []
    try:
        with path.expanduser().open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError as exc:
                    errors.append(f"line {line_no}: invalid JSON ({exc.msg})")
                    continue
                if not isinstance(value, dict):
                    errors.append(f"line {line_no}: JSON value is not an object")
                    continue
                records.append(value)
    except OSError as exc:
        errors.append(f"cannot read {str(path)!r}: {type(exc).__name__}: {exc}")
    return records, errors


def sha256_file(path: Path) -> str | None:
    digest = hashlib.sha256()
    try:
        with path.expanduser().open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return None
    return digest.hexdigest()


def _record_time(record: dict[str, Any]) -> datetime | None:
    return parse_timestamp(record.get("ts_utc") or record.get("timestamp"))


def _is_healthy_decision(record: dict[str, Any]) -> tuple[bool, list[str]]:
    """Require a non-BLOCKED decision with explicit, verified feed-health gates."""
    problems: list[str] = []
    if record.get("decision") == "BLOCKED":
        problems.append("decision is BLOCKED")
    if record.get("feed_fresh") is not True:
        problems.append("feed_fresh is not true")
    if record.get("book_synced") is not True:
        problems.append("book_synced is not true")
    if record.get("no_unresolved_sequence_gap", record.get("sequence_valid")) is not True:
        problems.append("sequence gap is unresolved or unverified")
    if record.get("crc_valid") is not True:
        problems.append("CRC state is not verified")
    if record.get("orderbook_integrity_ok") is not True:
        problems.append("orderbook_integrity_ok is not true")

    metadata = record.get("metadata") or {}
    failed_gates = metadata.get("failed_data_quality_gates") or []
    if failed_gates:
        problems.append("failed data-quality gates: " + ", ".join(map(str, failed_gates)))

    feed_state = record.get("feed_state") or {}
    derived = feed_state.get("derived_health") or {}
    channel_freshness = derived.get("channel_freshness") or {}
    for channel in REQUIRED_CHANNELS:
        if channel_freshness.get(channel) is not True:
            problems.append(f"required channel {channel!r} is not fresh")
    return not problems, problems


def audit_records(
    event_records: list[dict[str, Any]],
    decision_records: list[dict[str, Any]],
    *,
    event_parse_errors: list[str] | None = None,
    decision_parse_errors: list[str] | None = None,
    max_recovery_seconds: float = 120.0,
) -> dict[str, Any]:
    """Audit the latest requested controlled reconnect in the supplied records."""
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    event_errors = event_parse_errors or []
    decision_errors = decision_parse_errors or []
    add("event_log_readable", not event_errors, "; ".join(event_errors) if event_errors else "read without parse/I/O errors")
    add("decision_log_readable", not decision_errors, "; ".join(decision_errors) if decision_errors else "read without parse/I/O errors")

    timed_events = [(t, r) for r in event_records if (t := _record_time(r)) is not None]
    timed_decisions = [(t, r) for r in decision_records if (t := _record_time(r)) is not None]
    timed_events.sort(key=lambda item: item[0])
    timed_decisions.sort(key=lambda item: item[0])
    add("event_timestamps_available",
        len(timed_events) == len(event_records) and bool(event_records),
        f"{len(timed_events)} of {len(event_records)} events have parseable timestamps")
    add("decision_timestamps_available",
        len(timed_decisions) == len(decision_records) and bool(decision_records),
        f"{len(timed_decisions)} of {len(decision_records)} decisions have parseable timestamps")

    markers = [(t, r) for t, r in timed_events if r.get("event") == "diagnostic_reconnect_requested"]
    if not markers:
        add("diagnostic_trigger_recorded", False, "no diagnostic_reconnect_requested event found; trigger the UI control once")
        return {
            "status": "FAIL", "generated_at_utc": datetime.now(UTC).isoformat(),
            "checks": checks, "diagnostic": None,
            "note": "No reconnect marker was found; no runtime recovery claim can be made.",
        }

    marker_time, marker_record = markers[-1]
    events_after_marker = [(t, r) for t, r in timed_events if t >= marker_time]
    marker_index = next(i for i, (t, r) in enumerate(events_after_marker)
                        if r is marker_record or (t == marker_time and r == marker_record))
    later_events = events_after_marker[marker_index + 1:]

    add("diagnostic_trigger_recorded", True, f"latest trigger at {marker_time.isoformat()}")

    disconnect = next(((t, r) for t, r in later_events if r.get("event") == "ws_disconnected"), None)
    if disconnect is None:
        add("disconnect_observed", False, "no ws_disconnected event occurred after the diagnostic trigger")
        return {
            "status": "FAIL", "generated_at_utc": datetime.now(UTC).isoformat(),
            "checks": checks,
            "diagnostic": {"triggered_at_utc": marker_time.isoformat()},
            "note": "The requested reconnect did not produce a logged close event.",
        }

    disconnect_time, _ = disconnect
    connect = next(((t, r) for t, r in later_events
                    if t > disconnect_time and r.get("event") == "ws_connected"), None)
    add("disconnect_observed", True, f"disconnect at {disconnect_time.isoformat()}")

    if connect is None:
        add("reconnection_observed", False, "no ws_connected event occurred after the disconnect")
        return {
            "status": "FAIL", "generated_at_utc": datetime.now(UTC).isoformat(),
            "checks": checks,
            "diagnostic": {"triggered_at_utc": marker_time.isoformat(),
                           "disconnected_at_utc": disconnect_time.isoformat()},
            "note": "The feed did not log a reconnect after the diagnostic disconnect.",
        }

    connect_time, _ = connect
    add("reconnection_observed", True, f"connected at {connect_time.isoformat()}")

    verified_book = next(
        ((t, r) for t, r in later_events
         if t > connect_time
         and r.get("event") == "ob_update"
         and r.get("checksummed") is True
         and r.get("crc_ok") is True),
        None,
    )
    if verified_book is None:
        add("verified_book_recovered", False,
            "no post-reconnect ob_update recorded both checksummed=true and crc_ok=true")
        return {
            "status": "FAIL", "generated_at_utc": datetime.now(UTC).isoformat(),
            "checks": checks,
            "diagnostic": {"triggered_at_utc": marker_time.isoformat(),
                           "disconnected_at_utc": disconnect_time.isoformat(),
                           "reconnected_at_utc": connect_time.isoformat()},
            "note": "A socket reconnect alone is not evidence of recovered book integrity.",
        }

    book_time, _ = verified_book
    add("verified_book_recovered", True, f"checksum-bearing book update verified at {book_time.isoformat()}")

    in_window = [(t, r) for t, r in timed_decisions
                 if disconnect_time <= t < book_time]
    blocked_records = [(t, r) for t, r in in_window if r.get("decision") == "BLOCKED"]
    add("blocked_during_failure", bool(blocked_records),
        f"{len(blocked_records)} BLOCKED decision(s) recorded between disconnect and verified book recovery")
    unsafe_decisions = [(t, r) for t, r in in_window if r.get("decision") != "BLOCKED"]
    add("no_entry_decisions_before_book_recovery", not unsafe_decisions,
        f"{len(unsafe_decisions)} non-BLOCKED decision(s) occurred before a verified book update")

    candidates = [(t, r) for t, r in timed_decisions if t >= book_time]
    healthy_candidate = None
    candidate_reasons: list[str] = []
    for t, record in candidates:
        healthy, problems = _is_healthy_decision(record)
        if healthy:
            healthy_candidate = (t, record)
            break
        if not candidate_reasons:
            candidate_reasons = problems

    if healthy_candidate is None:
        detail = ("no post-recovery decision met all health gates"
                  + (": " + "; ".join(candidate_reasons) if candidate_reasons else ""))
        add("healthy_decision_after_recovery", False, detail)
        recovery_time = None
    else:
        recovery_time, record = healthy_candidate
        add("healthy_decision_after_recovery", True,
            f"{record.get('decision')} decision passed explicit feed/book/channel gates at {recovery_time.isoformat()}")

    elapsed = (recovery_time - disconnect_time).total_seconds() if recovery_time else None
    latency_ok = elapsed is not None and 0 <= elapsed <= max_recovery_seconds
    add("recovery_within_time_limit", latency_ok,
        f"{elapsed:.3f}s from disconnect to first fully healthy decision (limit {max_recovery_seconds:.3f}s)"
        if elapsed is not None else "no fully healthy recovery decision to time")

    all_passed = all(check["passed"] for check in checks)
    return {
        "status": "PASS" if all_passed else "FAIL",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "diagnostic": {
            "triggered_at_utc": marker_time.isoformat(),
            "disconnected_at_utc": disconnect_time.isoformat(),
            "reconnected_at_utc": connect_time.isoformat(),
            "verified_book_at_utc": book_time.isoformat(),
            "healthy_decision_at_utc": recovery_time.isoformat() if recovery_time else None,
            "recovery_seconds": elapsed,
            "max_recovery_seconds": max_recovery_seconds,
            "recovered_decision": healthy_candidate[1].get("decision") if healthy_candidate else None,
        },
        "checks": checks,
        "note": "A PASS validates only the logged diagnostic run; it does not certify strategy profitability or authorize live orders.",
    }


def _input_summary(path: Path, records: list[dict[str, Any]], errors: list[str]) -> dict[str, Any]:
    times = [t for r in records if (t := _record_time(r)) is not None]
    return {
        "path": str(path.expanduser()),
        "sha256": sha256_file(path),
        "records": len(records),
        "parse_or_io_errors": errors,
        "first_timestamp_utc": min(times).isoformat() if times else None,
        "last_timestamp_utc": max(times).isoformat() if times else None,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", default="~/Downloads/delta_feed_events.jsonl",
                        help="app event JSONL file (default: %(default)s)")
    parser.add_argument("--decisions", default="~/Downloads/signal_decisions.jsonl",
                        help="app decision JSONL file (default: %(default)s)")
    parser.add_argument("--output", help="optional JSON report path; stored separately from evidence logs")
    parser.add_argument("--max-recovery-seconds", type=float, default=120.0,
                        help="maximum time from disconnect to first fully healthy decision (default: %(default)s)")
    args = parser.parse_args(argv)

    if args.max_recovery_seconds <= 0:
        parser.error("--max-recovery-seconds must be > 0")

    event_path = Path(args.events).expanduser()
    decision_path = Path(args.decisions).expanduser()
    events, event_errors = load_jsonl(event_path)
    decisions, decision_errors = load_jsonl(decision_path)
    report = audit_records(
        events, decisions,
        event_parse_errors=event_errors,
        decision_parse_errors=decision_errors,
        max_recovery_seconds=args.max_recovery_seconds,
    )
    report["event_log"] = _input_summary(event_path, events, event_errors)
    report["decision_log"] = _input_summary(decision_path, decisions, decision_errors)

    rendered = json.dumps(report, indent=2, sort_keys=True, allow_nan=False)
    print(rendered)
    if args.output:
        out_path = Path(args.output).expanduser()
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(rendered + "\n", encoding="utf-8")
        except OSError as exc:
            print(f"ERROR: could not write report {out_path}: {exc}", file=sys.stderr)
            return 2
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
