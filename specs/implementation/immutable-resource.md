# Immutable Linux resources

`ImmutableMapping` holds a sealed, read-only memfd. `ImmutableResource` verifies that mapping against an independently authenticated descriptor and object hash. Use the full validated file when checking a topic snapshot. A valid resource envelope alone grants no right to join. [Mapping](../../crates/tickwire-shm-sys/src/immutable.rs) · [Resource validator](../../crates/tickwire-shm-sys/src/resource.rs)

## Produce and receive

Creation checks platform and length, creates the memfd, sets its size and writes **all** bytes with `pwrite`. Retry short writes and EINTR. A zero write fails. Apply and read back the exact `0x0f` seal mask, obtain a read-only descriptor and map it read-only. The construction path creates no writable mapping and exports no unsealed descriptor.

A receiver verifies regular-file type, exact expected size and `0x0f` seals, then rechecks length at mapping. An incoming O_RDONLY descriptor is duplicated with CLOEXEC. For O_RDWR, keep the input FD alive while reopening its `/proc/self/fd/<fd>` path read-only. Only an O_RDONLY descriptor survives into the returned object. The locked WRITE/GROW/SHRINK seals permit a safe borrowed slice. `MADV_DONTFORK` prevents inheritance of the mapping. Drop unmaps before closing the FD. A single-writer ring uses its distinct `0x17` profile. [Ring mappings](shm-mapping.md)

## Verify the resource envelope

Match the full 4096-byte decoded header to the approved header. Recompute its SHA-256 with only the descriptor-hash bytes zeroed. Check ABI, schema, architecture, page configuration, supported features, nonzero resource/producer/host/namespace identities, generation and bounded counters. Verify kinds 5–9, content starting at offset 4096, unused cursor/producer/arena fields zero, and exact file length.

The object is bounded to 64 MiB. Check `4096 + content_bytes` and page rounding without overflow. The outer page tail must be zero and the whole-file hash must match the external expected hash. For projected config (kind 9), require a nonzero consumer, config feature, one-byte records, capacity equal to valid content size and at most 4 MiB. For snapshot kinds, validate the 256-byte snapshot header, entry offset 4352, object size, kind and entry shape.

`content()` removes the outer resource header and padding. `bytes()` exposes the complete validated envelope. The snapshot layer checks internal member digests, coverage, cut, validity windows and JOIN identity. Computing expected metadata from the file itself would only establish internal consistency. [Topic receiver](topic-snapshot.md)

```sh
cargo test --locked -p tickwire-shm-sys --test immutable
cargo test --locked -p tickwire-snapshot --test immutable_linux
```

The Linux tests verify read-only CLOEXEC export, exact seal sets, denied writes, resizes, new writable mappings and permission upgrades. They exercise sealing failure while a writable mapping exists and success once it is removed. Header-digest tests keep the expected fields equal to the corrupted header so the independent digest check must catch it.
