# Local activation result

Every returned receipt identified a durable record. Historical retries preserved the desired and effective files. Every effective version had an earlier recorded runtime witness, a synced projection object and a last-good copy. At each explored crash point the on-disk files satisfied the materializer's reopen predicate. The logical journal view either equaled the sealed business prefix or lagged by its single in-flight append.

All eleven transition families and initialization fired. The two-operation run includes a retry of an older revision after the new revision is effective. A stop can occur immediately after the WAL seal and before either the caller’s receipt or the in-memory ledger update. [Results and action counts](03_run_log.md)

The process identity, writer scope and file contents are fixed, with distinct revisions representing distinct renders. The result depends on the physical journal and file installation delivering their documented durable prefix. External edits, filesystem corruption, disk exhaustion and authentication of the operator's control commit were outside this level.
