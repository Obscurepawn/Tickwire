# `v2` verification output

PASS: all four bounded configurations verified all 16 safety invariants without counterexamples. Three added transitions separate the pre-callback `second` sample, post-callback monotonic advance, and final deadline rejection; `ExpiredCallbackNeverAccepted` proves requests whose budget callback crosses the local deadline accept zero bytes (`NoBytes`), while all 15 lifecycle invariants from `v1` continue to hold. See [03_run_log.md](03_run_log.md).

Fidelity: every transition cites its Rust source lines with zero unresolved branches across connection upgrade, request preparation and first-accept gating, reply correlation, Spot logon/signed subscription, USD-M `listenKey` creation/renewal/close, prior-epoch gap draining, and permanent `SinkFailure` stop. Diagnostic queries, frame-level byte budgets, and clock-calibration interval arithmetic are verified separately.

The post-callback deadline bug was identified during cross-crate composition review of `PrivateClient` + `ClockAdapter`, reproduced with a native HTTPS/WSS Red test, and verified to exhaustion in `v2`.
