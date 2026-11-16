# Paired SHM quote delivery and subscriber continuity gating

This level checks Bybit and OKX after their adapters have supplied a valid two-sided quote or an unavailable notice to one quote-only topic. The `tickwire-md` owner serves JOIN only when the market state is live. A new strategy worker must wait for the feed record of a fresh pair to invoke its callback. A `MarketSink` subscriber can also use a complete JOIN snapshot. A genuine market outage revokes same-generation continuity. We additionally test whether that legitimate outage permanently stops an otherwise healthy `ShmSource` subscription. [Public quote specification](../../../../specs/implementation/spot-quote-input.md) · [SHM source](../../../../crates/tickwire-market-source/src/shm.rs) · [Market sink contract](../../../../crates/tickwire-market-source/src/lib.rs)

This is the first level of the ladder. The input assumes a trusted topic owner, an active strategy subscriber, and an uninterrupted control connection. Kernel ring ordering and depth level merges have their own components. The model starts immediately after the first live pair. Both clients may independently join later at a fresh committed cut.

