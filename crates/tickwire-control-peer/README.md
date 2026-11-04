# Control peer

This binary runs a single control replica. Its event loop drives mutually authenticated Raft connections, durable configuration records, snapshots and membership changes. Applied results reach the caller after the business replica has scanned the entries.

```sh
cargo build --locked -p tickwire-control-peer --bin tickwire-control-peer
target/debug/tickwire-control-peer --config /srv/tickwire/control/node-1.json
```

Copy the [example configuration](examples/node.json) and replace addresses, paths, domain, node IDs and pinned leaf digests. Certificates carry the matching cluster/group/node URI, server name and both TLS uses. Provide a current CRL and a private key with one hard link and owner-only access. The loader opens and validates the actual descriptor, rejects a final symlink, and checks file type and length.

Use `create` once for each of the original three voters. A new identity uses `join` with the same initial voters. The current membership always comes from the persisted ConfState. An existing directory uses `recover` and the appropriate writer epoch. A joining learner catches up by Append or snapshot before promotion. Retired node IDs remain reserved.

Applications call `runner::assemble`, use `with_control` between polls for authenticated operations, and retrieve results with `take_applied`. `api_runner::ApiOwner::assemble` shares this owner with HTTPS. Bootstrap is an explicit command. `--turns N` bounds event-loop turns. The service attempts compaction every 256 newly applied indexes, keeps the current and preceding complete generations, and retains up to 16 identities and 64 membership operations. See the [peer protocol](../../specs/implementation/control-raft-service.md).
