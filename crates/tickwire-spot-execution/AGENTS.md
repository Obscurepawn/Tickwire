# Spot execution

Implement authenticated Bybit V5 and OKX V5 REST requests against the existing account gateway. Preserve account, environment, venue and command identity and use a trusted time source. Treat request acknowledgements separately from order and fill evidence. Keep API secrets out of Debug and all error text. See the [spot execution specification](../../specs/implementation/spot-execution.md). Run cargo test -p tickwire-spot-execution and cargo clippy -p tickwire-spot-execution --all-targets -- -D warnings.

Recovery reads produce a replayable, UID-bound transcript. Keep the network request plan identical on live collection and local replay, preserve each venue's paging and fee semantics, and refuse unsupported cash activity. The [REST recovery specification](../../specs/implementation/spot-rest-recovery.md) describes the account profile, history window and executable integration test.
