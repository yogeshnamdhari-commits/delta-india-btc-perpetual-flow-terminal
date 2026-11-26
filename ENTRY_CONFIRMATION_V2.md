# Entry Confirmation V2 — shadow-only implementation

This patch adds an independent evidence layer beside the frozen baseline. It does not change baseline thresholds, signal precedence, historical evaluator or execution behaviour. V2 only assesses whether a baseline BUY/SELL is supported by higher-timeframe, trade-flow, book-flow and execution-cost evidence.

## Feed and adapter corrections

- Use the bounded 100,000-print analytics tape instead of the 120-print display deque; retain fractional-second exchange trade timestamps.
- Supply confirmed 5m swings and CRC-verified L2 bid/ask levels to target selection, fixing the previously empty target inputs.
- Aggregate only complete contiguous 1m bars into 5m, 15m and 1h bars.
- Treat Delta ob_updates as change-driven, not a heartbeat. Quiet periods do not stale a valid book; connection/ticker/trade freshness, sequence continuity, book synchronization and CRC remain separate gates.
- Capture sequence/checksum-qualified L1 OFI events and archive raw L2 update price levels for future event replay. Clear rolling book-flow buffers on a full snapshot/resync or integrity failure so observations from different sequence epochs are not mixed.
- Require a minimum 20-event, 60-second OFI observation sample at both the adapter and V2 decision layer.
- Report same-price replenishment and unexplained depth-withdrawal candidates from raw observed level changes matched against public trades; these are not proof of hidden orders, spoofing or intent.

## V2 confirmation gates

1. Existing baseline feed-integrity and CRC-verified-book gates.
2. Aligned 1h and 15m EMA20/EMA50 plus EMA20 slope.
3. Completed-candle 5m sweep/reclaim, structure break, intact order block and 1m trigger.
4. Five-minute trade tape with minimum print count, observed span, directional contract share and volume-weighted price response. Process capture only; public Delta tape has no per-print sequence or historical backfill.
5. A combined book-flow gate using normalized L1 queue imbalance and event-level L1 OFI. Microprice is displayed as a diagnostic, not an extra independent vote.
6. Optional structure-break retest/hold is reported but not mandatory.
7. Net risk/reward after current spread, two-sided fees and two-sided slippage.
8. Zero-funding protection is enabled by default. A verified next funding timestamp and explicit maximum holding horizon must demonstrate exit before funding, including a buffer. Unknown inputs cause WAIT.

Aggregate OI, funding and mark/index basis are context only. Aggregate OI does not identify long-side or short-side participant positions.

## Local settings

Use the actual account fee tier and measured execution costs. Do not assume a generic fee schedule.

- ENTRY_CONFIRMATION_FEE_BPS_PER_SIDE
- ENTRY_CONFIRMATION_SLIPPAGE_BPS_PER_SIDE
- ENTRY_CONFIRMATION_MAX_HOLD_MINUTES
- ENTRY_CONFIRMATION_ZERO_FUNDING_REQUIRED=1 (default)

The feed must supply next_funding_at. The default zero-funding policy blocks V2 when funding timing or maximum holding time is unknown.

Research flow thresholds: ENTRY_CONFIRMATION_MIN_FLOW_TRADES (20), ENTRY_CONFIRMATION_MIN_FLOW_SPAN_SECONDS (60), ENTRY_CONFIRMATION_MIN_FLOW_DOMINANCE (0.60).

PAPER_REVIEW_CANDIDATE is not live-trading approval. This implementation never places or authorizes an order.

## Validation and release requirements

- Run full Python 3.11 and 3.12 CI.
- Verify live Delta timestamp units, aggressor mapping, ob_updates sequence/CRC continuity, depth deltas and top-of-book fields on the target Mac.
- Reconcile archived depth events by sequence and checksum before reconstructing historical OFI.
- Record actual fees, spread, slippage, queue position, fill probability, partial fills and funding exposure. The V2 shadow model does not simulate queue position or execute.
- Acquire venue-matched historical trade and raw L2 data. Existing historical coverage gaps and zero genuine holdout trades remain independent release blockers.
- Compare frozen baseline and V2 as separate arms. Use feature ablations, chronological splits, purging/embargo for overlapping labels and holding periods, uncertainty intervals, drawdown, turnover, net expectancy, profit factor and Deflated Sharpe Ratio. Use Probability of Backtest Overfitting when a parameter search is conducted.
- Require at least 100 genuine completed OOS trades as a minimum review sample, then prospective paper/shadow validation. Sample count alone is not profitability evidence.

## Safety

Public read-only data and shadow analysis only. No private order APIs, order submission, deployment, service restart, merge or trading enablement. Project release remains NOT APPROVED pending independent historical and prospective validation.
