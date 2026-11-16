# L0 output (archived `v1`)

PASS: four bounded configurations verified 15 safety invariants without counterexamples across Spot signed subscription, Spot logon subscription, USD-M `listenKey` create/renew/close, and capacity-2 event buffering. See [03_run_log.md](03_run_log.md).

Verified properties: `NoBytes` never becomes sent; confirmation requires full request acceptance; first byte acceptance passes synchronous gate and deadline checks; only the confirmed current generation reaches `Live`; `listenKey` lease expiration is anchored to confirmed request start time; observed `EOF` immediately closes `Live`; buffered prior-epoch events retain original epoch and order; ambiguous `DELETE` retains management ownership and stops auto-reconnect; and reconnect waits until buffered events drain.

## Fidelity & coverage

- Fidelity: 28 source-cited `Next` transitions per product configuration with zero unresolved branches.
- Code coverage: 8 of 10 lifecycle branch groups modeled (connect/upgrade, prepare/first-accept, reply correlation, auth/subscribe, `listenKey` renewal, explicit close, gap draining, and clock-domain/sink rejection; diagnostic queries and frame-budget scheduling excluded).
- Model coverage: `Init` + 28 `Next` entries per configuration all achieved `generated > 0` in the final coverage block.
