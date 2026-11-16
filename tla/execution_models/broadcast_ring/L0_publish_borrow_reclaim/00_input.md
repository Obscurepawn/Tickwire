# L0 input

Builds the baseline broadcast-ring model from `Producer`, `Reader`, and mapped SHM wrappers in `tickwire-shm`. Callers supply valid, fixed, distinct bank and subscription identities, and cursors are created via `Subscriber::attach` before registration under the matching identity (matching the safety preconditions of `attach` / `register_cursor`).

- `crates/tickwire-shm/src/lib.rs:130-153`: ring capacity is a power of two `>= 64` (fixed to `64` here).
- `crates/tickwire-shm/src/lib.rs:194-233`: `join` establishes a pending reclamation gate at current `published`.
- `crates/tickwire-shm/src/lib.rs:235-312`: `join_ready`, release observation, conservative `cached_min`, and `claim`.
- `crates/tickwire-shm/src/lib.rs:322-388`: `publish`, `abort`, `prepare_freeze` / `freeze`, and `detach`.
- `crates/tickwire-shm/src/lib.rs:433-557`: reader borrow window, independent contiguous `applied` / `released` prefixes, and `quiesce`.
- `crates/tickwire-shm/src/mapped.rs:209-341`: live cursor registration, `Acquire` release observation, per-slot writes, and `Release` publication.
- `crates/tickwire-shm/src/mapped.rs:360-402`: `detach` verifies live registered cursor `lifecycle == 5` and exact watermarks.
- `crates/tickwire-shm/src/mapped.rs:405-547`: independent subscriber mapping, borrow guard, release, apply, and quiesce.
- `crates/tickwire-shm/src/mapped.rs:550-581`: `RecordBorrow` holds `NonNull<[u8]>`, exposes short-lived `&[u8]` slices via `bytes(&self)`, and advances `released` on `Drop`.

Forged pure-state `Quiesced` tokens cannot detach a live registered cursor holding active borrows; `mmap` syscalls and hardware weak-memory litmus behavior are verified by Linux integration tests.
