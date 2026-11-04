# Recovery dependencies and verified closures

The account passes a manifest and its dependency bytes to `Persistence::publish_with_objects`. The store verifies and synchronizes each dependency, then writes the manifest and its immutable binding. The resulting `VerifiedClosure` identifies the exact data available for replay. A caller that already stored the dependencies uses `publish` to load and check them. After restart, `verify` reopens the binding, checks the stored bytes and confirms synchronization. The account gateway evaluates the current grant before sending an order. [Persistence](../../crates/tickwire-recovery/src/lib.rs)

## Binding and manifest

A `ClosureBinding` fixes the risk domain, intent ID, original execution epoch, business digest, strategy generation, configuration revision and recovery-object ID. One recovery-object ID in a domain can name only one manifest root. An exact retry may succeed. Different content or order identity under that ID is rejected. The manifest uses deterministic, definite-length CBOR arrays, shortest-form integer and length encoding, and a SHA-256 over the whole encoding. Type errors, truncation and trailing data fail before a capability is issued. [Manifest codec](../../crates/tickwire-recovery/src/codec.rs)

Descriptors sort strictly by dependency kind and content digest. The required kinds must be present. The single intent object must match the business digest. Grant and release-operation references must identify the correct kinds. Edges reference objects within the manifest, are strictly ordered and unique, and cannot create cycles. Each descriptor must match the length and SHA-256 of the supplied or stored bytes, including objects with no incoming graph edge. Command dependency kind 13 carries the canonical command where needed. Kinds 14 and 15 carry the account baseline and history for account-backed closures. [Manifest validation](../../crates/tickwire-recovery/src/lib.rs)

| Limit | Maximum |
|---|---:|
| Single object | 64 MiB |
| Total object bytes per closure | 256 MiB |
| Manifest encoding | 1 MiB |
| Objects / edges | 4,096 / 16,384 |

## Ordered inputs

Kind 12 contains actual sequenced strategy inputs covering market events, execution reports, timers, configuration and model updates. Every block binds its input stream, starting sequence, predecessor digest and complete records. The verifier inspects the payloads and checks types, byte lengths, consecutive sequences and rolling digests before joining blocks. Gaps, overlaps, a different stream or a final decision frontier mismatch reject the closure. Exactly one empty block represents an empty input history. `DeterministicReplay` and `DeclaredRebuild` are explicit modes. A gap fails verification in both modes. The collector is responsible for capturing every input actually processed by the strategy. [Input blocks](../../crates/tickwire-recovery/src/inputs.rs)

## File persistence, restart and retention

The file backend locks a private directory exclusively and retains its canonical absolute path. Creation and reopening synchronize the directory and its parent. A new content object is written to a temporary file, synchronized, renamed to its immutable key, then followed by a directory sync. A same-key retry must match every byte and still synchronizes the existing file and directory. Reading a visible rename alone cannot prove an earlier failed directory sync succeeded. `verify` rechecks synchronization and cannot recreate a missing dependency. Unbound objects left by a failed publication still consume disk space. [File backend](../../crates/tickwire-recovery/src/files.rs) · [Closure tests](../../crates/tickwire-recovery/tests/closure.rs)

The exclusive owner installs its unlock guard immediately after acquiring the lock. Construction failure and normal drop explicitly unlock before closing. A temporarily inherited descriptor cannot extend an ended ownership period or unlock a new owner's independent lock. Process-crash model cases distinguish rename visibility, confirmed directory synchronization and delivery of the result. [Durability model](../../tla/execution_models/recovery_closure/PLAN.md)
