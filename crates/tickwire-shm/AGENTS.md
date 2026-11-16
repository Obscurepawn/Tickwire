# tickwire-shm

`tickwire-shm` owns single-writer broadcast and SPSC ring publication (`resource_kind` `1..=3`: `512`-byte market,
`1,024`-byte inline `IntentRecord`, and `512`-byte inline `ReportRecord` / `OwnershipRecord` banks), reader lease
tracking, variable-depth market arena allocation (`configure_market_arena` on `resource_kind == 1`), zero-copy
`MarketRecord` / `MarketExactRecord` slot borrowing, and cross-process watermark reclamation (`Producer<M>`,
`Reader<B>`, `Publisher<M>`, `Subscriber<B>`, and `RecordBorrow`). Pure protocol state machines in
[src/lib.rs](src/lib.rs) and [src/arena.rs](src/arena.rs) run under `#![deny(unsafe_code)]` without OS mappings, while
[src/mapped.rs](src/mapped.rs) (`#![deny(unsafe_op_in_unsafe_fn)]`) is the sole module permitted to discharge
`tickwire-shm-sys` memory-access obligations; control-socket descriptor handshakes belong in `tickwire-join`.

## Invariants and Change Guidelines

* **Guarded `RecordBorrow` zero-copy views and release ordering**: `RecordBorrow` in [src/mapped.rs](src/mapped.rs)
  holds `NonNull<[u8]>` (never `&'a [u8]`) so `bytes(&self)`, `as_market_record(&self)`,
  `as_market_exact_record(&self)`, `as_market_state(&self)`, and `depth_payload(&self)` tie returned references strictly
  to `&self` (`compile_fail` doctests `E0515` and `E0505`) and immutability ends strictly before `Drop` stores the
  updated `RELEASED` watermark with `Ordering::Release`. `Subscriber::borrow` loads `PUBLISHED` with `Acquire` before
  exposing 512-byte 8-byte-aligned (`SLOTS = 8192`) slot or external arena bytes.
* **Compile-time `crc32` feature gate (`default = []`)**: By default (`not(feature = "crc32")`),
  `Publisher::publish_market` clears flag `0x08` on `RecordHeader.flags`, writes zero `record_crc32c` / `payload_crc32c`
  fields, and skips CRC32C computation across ring slots and `ArenaBlockHeader` depth blocks; `--features crc32`
  restores hardware-accelerated `crc32c` / `crc32c_masked` generation and verification.
* **Watermark sampling order**: When `Publisher::activate_join` or `Publisher::cursor_watermarks` samples a reader
  cursor page in [src/mapped.rs](src/mapped.rs), load `applied` and `released` before `read` (`Acquire` on each) so a
  monotonic `read` prefix covers both earlier watermarks (`applied <= read`, `released <= read`, `read <= published`).
* **Gated slot and arena reclamation**: `Producer` ([src/lib.rs](src/lib.rs)) and `Arena` ([src/arena.rs](src/arena.rs))
  reclaim ring slots and size-classed depth blocks only past the minimum `released` sequence across all registered
  active and pending `SubscriptionKey` gates. Raw byte `Publisher::publish` rejects `payload_kind == 2` so only
  `publish_market` can bind sequence-checked arena blocks.
* **Epoch freeze and quiesce**: Keep `prepare_freeze` / `freeze` cutover sequences exact (`cutover_seq == first_seq - 1`
  on the successor ring) and transition reader cursors cleanly through `start_quiesce` (state `4`) to `quiesced` (state
  `5`) once all outstanding `ReadLease` guards drop.

## Verification and Specifications

* **Source, benches, and tests**: [src/lib.rs](src/lib.rs) · [src/arena.rs](src/arena.rs) ·
  [src/mapped.rs](src/mapped.rs) · [benches/market_record_borrow.rs](benches/market_record_borrow.rs) ·
  [tests/protocol.rs](tests/protocol.rs) · [tests/mapped.rs](tests/mapped.rs) ·
  [tests/join_pending.rs](tests/join_pending.rs)
* **Specifications and models**: [shm-ring.md](../../specs/implementation/shm-ring.md) ·
  [shm-mapping.md](../../specs/implementation/shm-mapping.md) ·
  [broadcast_ring](../../tla/execution_models/broadcast_ring/ROADMAP.md) ·
  [join_owner](../../tla/execution_models/join_owner/ROADMAP.md) ·
  [quote_delivery](../../tla/execution_models/quote_delivery/ROADMAP.md)

```sh
cargo test --locked -p tickwire-shm --all-targets
cargo test --locked -p tickwire-shm --features crc32 --all-targets
cargo test --locked -p tickwire-shm --doc
./scripts/check-linux.sh
./scripts/check-tla.sh broadcast_ring
```
