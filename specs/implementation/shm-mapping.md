# Single-writer Linux mappings

`tickwire-shm-sys` creates and maps a Linux memfd with one established writable mapping and bounded read-only observers. The ring owner separately enforces publication, slot ownership and release. [Mapping implementation](../../crates/tickwire-shm-sys/src/lib.rs) · [Ring protocol](shm-ring.md)

## Create, seal and read

1. Create the memfd, set its exact page-aligned length and map it shared read/write. Apply `MADV_DONTFORK` and initialize the zero-filled pages.
2. Set and verify the single-writer seal mask `0x17`. Only then export a CLOEXEC descriptor. The existing writable mapping can still publish. The kernel denies new shared writable mappings, direct writes, growth and shrinkage.
3. A reader checks regular-file type, exact length and matching seals, then maps read-only with `MADV_DONTFORK`. An immutable resource uses a different exact mask, `0x0f`. Drop unmaps first and closes the owned FD. The mapping types do not expose Clone, Send or Sync.

All accessors check arithmetic and ranges. A 64-bit atomic operation also checks alignment. The remote writer must obey the same atomic width and ordering, and the reader may borrow or copy a slot only while the ring guarantees it cannot be overwritten. The low-level mutable mapping accessors therefore remain unsafe. Authentication and FD delivery are separate steps. [Control transport](control-transport.md) · [Immutable resources](immutable-resource.md)

## Verify

```sh
cargo test --locked -p tickwire-shm-sys
cargo clippy --locked -p tickwire-shm-sys --all-targets -- -D warnings
cargo rustc --locked -p tickwire-shm-sys --release -- --emit=asm
```

The Linux process test maps a real child reader before a parent writes 60 KiB. A release store followed by the reader’s acquire load makes all bytes visible. The regression suite checks seal bits, zero pages, CLOEXEC, range/alignment, rejected write upgrades and zero/non-page-multiple lengths. On the archived Linux 6.8.0-117-generic AArch64 guest with 4096-byte pages, the acquire load compiled to `ldr` plus `dmb ishld`. The release store used `stlr`. Linux memfd visibility provides no disk durability. CPU isolation, page faults, locking, NUMA and latency on a deployment host require separate measurements. [Mapping tests](../../crates/tickwire-shm-sys/tests/mapping.rs) · [Child process test](../../crates/tickwire-shm-sys/tests/process.rs)
