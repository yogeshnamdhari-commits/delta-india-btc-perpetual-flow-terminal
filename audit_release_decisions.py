#!/usr/bin/env python3
"""Schema-aware, read-only audit of signal_decisions.jsonl.

Usage:
  python3 audit_release_decisions.py ~/Downloads/signal_decisions.jsonl
  python3 audit_release_decisions.py ~/Downloads/signal_decisions.jsonl --since 2026-10-23T05:00:00Z
  python3 audit_release_decisions.py ~/Downloads/signal_decisions.jsonl --runtime-context live_signal

This audits record consistency. It cannot independently prove that the exchange feed
reported truthful state, that the dashboard called the evaluated engine, or profitability.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ENTRY_DECISIONS = {"BUY", "SELL"}


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include timezone, e.g. 2026-10-23T05:00:00Z")
    return parsed.astimezone(timezone.utc)


def gates_for(record: dict) -> dict:
    result = record.get("engine_result") or {}
    gates = result.get("data_quality_gate_results")
    if not isinstance(gates, dict):
        gates = result.get("data_quality_gates")
    return gates if isinstance(gates, dict) else {}


def engine_for(record: dict) -> dict:
    result = record.get("engine_result")
    return result if isinstance(result, dict) else {}


def record_timestamp(record: dict):
    value = record.get("ts_utc") or record.get("timestamp")
    if not isinstance(value, str):
        return None
    try:
        return parse_datetime(value)
    except (TypeError, ValueError):
        return None


def first_present(mapping: dict, *keys):
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return key, mapping[key]
    return None, None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path", nargs="?",
        default=str(Path.home() / "Downloads" / "signal_decisions.jsonl"),
        help="path to signal_decisions.jsonl",
    )
    parser.add_argument("--since", help="only inspect records at or after this timezone-aware ISO timestamp")
    parser.add_argument("--runtime-context", help="only inspect records with metadata.runtime_context equal to this value")
    args = parser.parse_args()

    try:
        since = parse_datetime(args.since) if args.since else None
    except ValueError as exc:
        parser.error(str(exc))

    path = Path(args.path).expanduser()
    if not path.is_file():
        print(f"ERROR: log not found: {path}")
        return 2

    parsed, malformed = [], []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
            if not isinstance(item, dict):
                raise ValueError("JSON line is not an object")
            parsed.append((line_no, item))
        except (json.JSONDecodeError, ValueError) as exc:
            malformed.append((line_no, str(exc)))

    selected = []
    excluded_bad_ts = 0
    excluded_context = 0
    for line_no, record in parsed:
        if since is not None:
            ts = record_timestamp(record)
            if ts is None:
                excluded_bad_ts += 1
                continue
            if ts < since:
                continue
        if args.runtime_context:
            metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
            if metadata.get("runtime_context") != args.runtime_context:
                excluded_context += 1
                continue
        selected.append((line_no, record))

    decisions = Counter(record.get("decision", "UNKNOWN") for _, record in selected)
    records_with_gates = 0
    records_without_gates = 0
    mapping_checked = 0
    mapping_mismatches = 0
    sequence_not_comparable = 0
    composite_checked = 0
    composite_mismatches = 0
    entry_missing_gates = 0
    unsafe_entries = 0
    safe_entries = 0
    issues = []

    for line_no, record in selected:
        decision = record.get("decision")
        engine = engine_for(record)
        gates = gates_for(record)

        if not gates:
            records_without_gates += 1
            if decision in ENTRY_DECISIONS:
                entry_missing_gates += 1
                issues.append(f"line {line_no}: {decision} has no engine gate data")
            continue

        records_with_gates += 1

        # Compare only fields whose semantics are directly aligned in the schema.
        # Do NOT equate `sequence_valid` with `no_unresolved_sequence_gap`: one
        # may be a composite/alias in historical schemas, and they are not universally
        # synonymous. Compare sequence_valid only when that exact gate is present.
        direct_pairs = [
            ("book_synced", ("book_synced",)),
            ("crc_valid", ("checksum_ok", "checksum_verified")),
            ("sequence_valid", ("sequence_valid",)),
            ("no_unresolved_sequence_gap", ("no_unresolved_sequence_gap",)),
        ]
        for top_key, gate_keys in direct_pairs:
            top_value = record.get(top_key)
            gate_key, gate_value = first_present(gates, *gate_keys)
            if gate_key is None or top_value is None:
                if top_key == "sequence_valid" and "no_unresolved_sequence_gap" in gates and "sequence_valid" not in gates:
                    sequence_not_comparable += 1
                continue
            mapping_checked += 1
            if top_value is not gate_value and top_value != gate_value:
                mapping_mismatches += 1
                issues.append(
                    f"line {line_no}: {top_key}={top_value!r} != engine gate {gate_key}={gate_value!r}"
                )

        # If the log includes the composite field, validate it only when all inputs
        # are explicitly available. This avoids inferring unlogged feed state.
        if "orderbook_integrity_ok" in record:
            book = gates.get("book_synced", record.get("book_synced"))
            seq_key, seq_value = first_present(gates, "sequence_valid", "no_unresolved_sequence_gap")
            crc_key, crc_value = first_present(gates, "checksum_ok", "checksum_verified")
            if all(isinstance(v, bool) for v in (book, seq_value, crc_value)):
                composite_checked += 1
                expected = book and seq_value and crc_value
                if record.get("orderbook_integrity_ok") is not expected:
                    composite_mismatches += 1
                    issues.append(
                        f"line {line_no}: orderbook_integrity_ok={record.get('orderbook_integrity_ok')!r}, expected {expected!r} from explicit gates"
                    )

        if decision in ENTRY_DECISIONS:
            # Confirmed entries must have a populated, all-true quality gate map.
            failed = [key for key, value in gates.items() if value is not True]
            expected_state = f"{decision}_CONFIRMED"
            engine_state = engine.get("state")
            if failed:
                unsafe_entries += 1
                issues.append(f"line {line_no}: {decision} has non-passing gates: {failed}")
            elif engine_state != expected_state:
                unsafe_entries += 1
                issues.append(f"line {line_no}: decision={decision}, engine state={engine_state!r}")
            else:
                safe_entries += 1

    print(f"Log: {path}")
    print(f"Records parsed: {len(parsed)}")
    print(f"Records selected: {len(selected)}")
    print(f"Malformed JSON lines: {len(malformed)}")
    print(f"Decision counts: {dict(decisions)}")
    print(f"Records with gate data: {records_with_gates}")
    print(f"Records without gate data (diagnostics/pre-engine): {records_without_gates}")
    print(f"Direct semantic mapping comparisons: {mapping_checked}")
    print(f"Direct mapping mismatches: {mapping_mismatches}")
    print(f"Sequence alias/composite comparisons not comparable: {sequence_not_comparable}")
    print(f"Composite integrity checks performed: {composite_checked}")
    print(f"Composite integrity mismatches: {composite_mismatches}")
    print(f"BUY/SELL with gates + confirmed engine state: {safe_entries}")
    print(f"BUY/SELL missing gate data: {entry_missing_gates}")
    print(f"BUY/SELL failing gate/state checks: {unsafe_entries}")
    if since:
        print(f"Since UTC: {since.isoformat()}")
        print(f"Records excluded for missing/invalid timestamp: {excluded_bad_ts}")
    if args.runtime_context:
        print(f"Runtime context filter: {args.runtime_context}")
        print(f"Records excluded by runtime context: {excluded_context}")

    if malformed or mapping_mismatches or composite_mismatches or entry_missing_gates or unsafe_entries:
        print("RESULT: FAIL — inspect findings; legacy schema differences are not automatically mismatches.")
        for issue in issues[:40]:
            print(" -", issue)
        for line_no, error in malformed[:10]:
            print(f" - malformed line {line_no}: {error}")
        return 1

    print("RESULT: PASS for inspected records' internal consistency only.")
    print("This does NOT independently verify source-feed truth, dashboard wiring, or profitability.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
