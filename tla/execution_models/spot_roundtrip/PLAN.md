# One spot account from quote to settlement

Track one honest command from each independent file queue through a shared, fee-inclusive cash budget. The account thread serializes intake, journal changes and output. The private reader, strategy producers and venue progress independently. The first level combines their observable boundaries for dedicated Bybit and OKX cash accounts on source revision 9b2ed63. Compare the [live account wiring](../../../crates/tickwire-spot-runner/src/live.rs) with the [Linux loopback deployment](../../../crates/tickwire-spot-runner/tests/reconcile_process.rs).
