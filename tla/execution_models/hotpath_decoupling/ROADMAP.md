# Hot-path decoupling and windowed pool models

| Level | Result |
| --- | --- |
| [L0 dial baton & REST querier](L0_dial_querier/05_output.md) | Exclusive baton ownership, fresh handshake handoff, reply-slot reservation under stalled consumers, and counted-writes-only retry bounds passed across primary and scaling configurations. |
| [L1 windowed pool & prefix compaction](L1_windowed_pool/05_output.md) | Active-order eviction protection, retention-window intent/fill deduplication, unstarved management headroom at order capacity, and journal `HistoryFrontier` continuity passed across primary and burst configurations. |
