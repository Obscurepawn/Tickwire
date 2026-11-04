# tickwire-strategy-host

The host owns strategy child processes, authenticated bootstrap, JOIN and intent intake.

Trace PID, instance, generation, cursor and permissions through startup and handover. An observer or shadow worker can consume inputs while the active owner supplies executable intents.

[Crate API](src/lib.rs) · [capture process](tests/capture_process.rs)

## Track one worker

The host retains its child handle, authenticates its PID and process identity, and owns the report writer and intent receiver. With an independent market source the worker joins the market owner directly. During rotation, the host asks the old child to quiesce, waits for its market detach and released cursor, and observes successful child exit before activating the successor. Keep socket closure and observed process exit as separate events. [Remote host](src/remote.rs) · [Retirement protocol](../../specs/implementation/strategy-graceful-retirement.md)

The preflight worker binds its READY seal to the approved binary, bootstrap and account position. An independent rollback probe receives the candidate's synchronized state through authenticated control. Its successful exit and receipts complete the proof. The capture-only worker has market and report inputs while the host retains intent authority. Exercise malformed receipts, an old child still running and rollback failure when changing release logic. [Preflight](src/preflight.rs) · [Capture process](src/capture.rs)

Specifications include [instrument catalog](../../specs/implementation/instrument-catalog.md), [release preflight](../../specs/implementation/release-preflight.md), [runtime observation](../../specs/implementation/runtime-observation.md), [strategy graceful retirement](../../specs/implementation/strategy-graceful-retirement.md), [strategy handover](../../specs/implementation/strategy-handover.md), [strategy input consumption](../../specs/implementation/strategy-input-consumption.md), [strategy parameter consumption](../../specs/implementation/strategy-parameter-consumption.md), [supervisor process](../../specs/implementation/supervisor-process.md).

```sh
cargo test --locked -p tickwire-strategy-host --all-targets
```
