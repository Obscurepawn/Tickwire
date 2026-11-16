# L0 input

The target is the Binance JSON depth synchronization coordinator (`DepthSync`), covering cold startup, snapshot retries, connection rebuilds, and buffered delta replay. Prior `book_state` L0/L1 models verified `Book` lifecycle and finite-depth `Unknown` tracking in isolation without snapshot/delta buffering; this level expands the real venue policies directly rather than assuming compositional soundness from prior runs.

Source boundaries: `crates/tickwire-binance/src/depth_sync.rs:139-485`, with `BinanceSpot` / `BinanceUsdM` sequence policies in `crates/tickwire-book/src/lib.rs:182-224` and `install` / `apply` / `rebuild` / `view` transitions in `crates/tickwire-book/src/lib.rs:590-761`. Static configuration invariants are enforced by the validated `Config` constructor, and JSON wire parsing plus integer tick/lot grid validation are handled upstream by the feed decoder.

Independent source review and 108 native/Linux crate tests (including 20 coordinator protocol/behavior tests) complement the TLC runs here; see the [Binance depth synchronization specification](../../../../specs/implementation/binance-depth-sync.md) and [book state model](../../../../specs/implementation/book-state-model.md).
