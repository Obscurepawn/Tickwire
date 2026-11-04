# Spot private input and first write

The account can lose its private socket before a REST scan completes, between a durable account reactivation and installation of the grant, or before its next order write. This model explores two connection epochs, duplicate deliveries of one trade and bounded lag between the capture and account cursors. A trade changes the booking count once, and a first write requires the current connection and complete captured prefix.

`BeginScan`, `Commit` and `Install` represent a successful comparison of the two native scans followed by journal sync and grant installation. `Discard` covers unsuccessful comparisons. The model treats REST evidence matching, receipt identity and authorization time as inputs to those transitions. The [Rust reconciliation tests](../../../crates/tickwire-spot-runner/tests/qualification.rs) and [process test](../../../crates/tickwire-spot-runner/tests/reconcile_process.rs) exercise the venue data and file formats. [Capture and grant checks](../../../crates/tickwire-spot-runner/src/live/account.rs) · [First-write guard](../../../crates/tickwire-spot-runner/src/live.rs)

Run `./scripts/check-tla.sh spot_account_recovery` from the workspace root. The configuration permits three deliveries of the same trade.
