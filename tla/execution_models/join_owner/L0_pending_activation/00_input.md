# L0 input

Verifies the boundary between `JoinOwner` and mapped SHM `Publisher`: `crates/tickwire-join/src/lib.rs:90-664`, `crates/tickwire-join/src/mapped.rs:9-309`, `crates/tickwire-shm/src/mapped.rs:205-407`, and contiguous `Reader` watermark/borrow rules in `crates/tickwire-shm/src/lib.rs:417-558` (see the [JOIN protocol specification](../../../../specs/implementation/join.md)).

Independent review and Linux 3-process integration tests (`tickwire-md` + strategy subscribers) accompany this model; prior Red/Green fixes for idempotent duplicate `JOIN_READY` catch-up checks and historical `Quiesce` ACK comparison are verified in the modeled code.

Building on `broadcast_ring` L0 (`Capacity = 64`, conservative `cached_min`, independent `released` / `applied`, live cursor `Quiesced` evidence) and `control_requests` L0/L1 (request deduplication and monotonic session leases), L0 fixes authenticated owner/subscriber identities and read-only ring FDs while modeling wrong bindings and JOIN deadline expiration explicitly.
