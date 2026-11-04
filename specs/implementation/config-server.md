# Starting a configuration server

`tickwire-config-server` is a binary in the `tickwire-control-peer` crate. It loads a schema 1 startup file and composes one `ApiOwner` with one durable `Replica<GenerationStore>` per process. The same owner drives HTTPS requests, Raft peer traffic, application of committed records and snapshots. Starting the service does not initialize a business head. Initial desired configuration requires signed bootstrap. [Binary](../../crates/tickwire-control-peer/src/bin/tickwire-config-server.rs) · [API owner](../../crates/tickwire-control-peer/src/api_runner.rs)

`config_runner::prepare` completes bounded configuration, peer and HTTP TLS, bootstrap trust, independent evidence trust and optional `TWDSRC01` source bundle parsing before binding any listener. It opens real FDs and rejects leaf symlinks, FIFOs and oversized inputs. A TLS private key must be a regular file with one hard link and no group/other permission bits. Prepared trust, settings and source data are owned bytes. `Prepared::assemble` consumes them without rereading the paths, so path replacement cannot replace an approved provider.

HTTP client identity and evidence issuers have separate trust settings. A request cannot select verifier keys or source files. The service rechecks signed evidence at candidate validation time. Current RBAC, safe reads, If-Match and retained registry state each belong to their own step. [Server preparation](../../crates/tickwire-control-peer/src/config_runner.rs) · [Control state](config-control-state.md)

Assembly failure returns no service. On `--turns` completion, a STOP marker or fatal error, shutdown cancels pending HTTP requests and stops peer I/O. Each loop processes a bounded batch of Applied records. Once the applied position advances at least 256 beyond the last snapshot, that owner compacts. Tests cover path replacement, rejected file inputs, actual TLS and HTTPS across three nodes and reopening after the binary stops. [Startup tests](../../crates/tickwire-control-peer/tests/config_startup.rs) · [Three-voter API tests](../../crates/tickwire-control-peer/tests/api_owner.rs)

```sh
cargo test -p tickwire-control-peer --test config_runner --test config_startup --test api_owner
```
