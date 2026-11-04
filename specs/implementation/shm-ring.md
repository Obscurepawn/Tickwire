# Shared-memory broadcast ring

A bank has one writer, an immutable resource ID and generation, and up to 32 registered consumers. Each consumer has its own cursor. A JOIN reserves the current cut for its reader immediately, so a slow pending reader already limits how far the producer can advance. The mapping layer owns Linux memfd, permissions, seals and aligned atomic access. [SHM implementation](../../crates/tickwire-shm/src/mapped.rs) · [Mapping contract](shm-mapping.md)

## Publish and borrow

Let `last` be the last sequence of a proposed batch and `min_released` the lowest released watermark among its gated readers. Claim the batch only if `last - min_released <= capacity`. A membership change invalidates the cached minimum. Advancing a cursor can leave the cached minimum as a conservative lower bound.

`Publisher::publish` validates every record's length and sequence before claiming. It writes all slots under one exclusive claim, then release-stores the batch’s last sequence. A consumer acquire-loads that published watermark before exposing data. `publish_market` also handles depth payloads and their arena lifetime. A BBO-only spot topic uses fixed-size records without depth payloads.

A noncopyable `RecordBorrow` pins the slot. `bytes(&self)` lends a slice tied to that guard. Internally the guard stores a raw nonnull slice pointer so no long-lived Rust reference survives drop and subsequent reuse. Dropping a guard records release of its sequence, then advances only the contiguous `released` prefix. `applied` tracks the independent business-processing prefix. Bounded outstanding reads prevent reuse of completion slots while either obligation remains. Forgetting a guard retains the reservation.

## Detach and freeze

Close new borrows, finish outstanding guards and mark the **actual mapped cursor** Quiesced. The publisher checks lifecycle and exact read, applied, released and quiesced sequence against that registration before removing the reader. A fabricated proof from a separate in-process reader, a timeout or socket EOF cannot free the gate. Repeating `start_quiesce` preserves an already Quiesced cursor.

Prepare freezes the successor identity and cutoff. Freeze requires no outstanding claim and `published == cutoff`. After freeze, the writer cannot publish again. Existing guards can still read their protected bytes. The JOIN/control layer authenticates peer credentials and passes descriptors, validates the baseline and links the actual successor. The codec validates business record semantics. [JOIN contract](join.md)

## Verify

```sh
cargo test --locked -p tickwire-shm -p tickwire-shm-sys
```

The tests cover full/empty/wrapped rings, the slowest peer, pending JOIN, atomic batches, out-of-order release and application, held guards, forged detach and cutoff. The Linux fan-out fixture runs 1, 2, 4, 8, 16 and 32 readers. Each verifies every byte of 1024 records of 512 bytes. It measures correctness. The finite broadcast model checks publication, borrow, reuse and freeze under acquire/release assumptions. [Mapped tests](../../crates/tickwire-shm/tests/mapped.rs) · [Broadcast model](../../tla/execution_models/broadcast_ring/PLAN.md)
