# tickwire-supervisor

`tickwire-supervisor` (`#![forbid(unsafe_code)]`) owns `Registry`, spawning directly supervised child
processes from sealed ELF images, binding optional cgroup v2 `ResourcePlan` limits, authenticating post-`exec`
`AF_UNIX` control connections (`AuthenticatedChild`), and sampling live process identity and resource usage
(`LiveChildObservation`, `LiveResourceObservation`). Low-level `memfd`, socket, and `/proc` syscalls stay
isolated in `tickwire-control-sys`, while domain-specific strategy or market-data protocols belong in
`tickwire-strategy-host` and `tickwire-md`.

## Invariants and Change Guidelines

* **TOCTOU-free executable digesting and verification**: `Registry::spawn` ([src/lib.rs](src/lib.rs)) opens
  `ExecutableImage` (`<= 512 MiB`), hashes the sealed `memfd` reader bytes against `spec.identity.build_hash`
  before spawning (never trusting a mutable filesystem pathname across `exec`), clears inherited environment
  variables (`env_clear`), and re-verifies `image.matches_process(actual.pid)` during `Registry::accept`.
* **Kernel credential and epoch binding on `accept`**: Never use an inherited `socketpair` as post-`exec`
  identity proof. `Registry::accept` ([src/lib.rs](src/lib.rs)) checks that the owned `Child` has not exited,
  requires strictly increasing `control_epoch`, rejects ancillary descriptors on `HELLO`, and matches kernel
  `SO_PEERCRED` (`pid`, `uid`, `gid`) against the spawned `Entry` before completing `Session` handshake.
* **Exit vs descendant resource ownership**: `ExitObservation` proves only that the direct child process
  exited; callers must still verify shared-memory cursor quiescence (`tickwire-join` / `tickwire-shm`) before
  reclaiming shared mappings. Keep `/proc` and cgroup observation helpers bounded in
  [src/observation.rs](src/observation.rs) and [src/resources.rs](src/resources.rs).

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/observation.rs](src/observation.rs) ·
  [src/resources.rs](src/resources.rs) · [tests/authenticated_image.rs](tests/authenticated_image.rs) ·
  [tests/current_process_identity.rs](tests/current_process_identity.rs) ·
  [tests/observation.rs](tests/observation.rs) · [tests/resources.rs](tests/resources.rs)
* **Specifications and models**: [supervisor-process.md](../../specs/implementation/supervisor-process.md) ·
  [control-session.md](../../specs/implementation/control-session.md) ·
  [runtime-observation.md](../../specs/implementation/runtime-observation.md) ·
  [control_requests](../../tla/execution_models/control_requests/ROADMAP.md)

```sh
cargo test --locked -p tickwire-supervisor --all-targets
./scripts/check-linux.sh
./scripts/check-tla.sh control_requests
```
