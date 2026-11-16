# Hot-Path Decoupling and Windowed Pool Models

| Level | Result |
| --- | --- |
| [L0 dial baton & REST querier](L0_dial_querier/05_output.md) | Exclusive `TimedSpinLock` baton ownership, fresh handshake handoff, `SpscRing` reply-slot reservation under stalled consumers, and counted-writes-only retry bounds passed across primary and scaling configurations. |
| [L1 stateless windowed pool & associative fill ways](L1_windowed_pool/05_output.md) | Active-order eviction protection, retention-window intent/fill deduplication, `TRADE_LITE` vs `amendment` fee-delta exactness, per-order `in_doubt` isolation, and unstarved management headroom passed across primary and burst configurations. |
