# Working on Tickwire

Start with the [running example](README.md#deploy-two-strategies) and the [specification index](specs/INDEX.md). Read the `AGENTS.md` next to the crate you change. Its tests establish which component owns each transition. Workspace crates live under `crates/`. The pinned Rust toolchain and Cargo lockfile also cover examples and integration tests.

## Follow ownership

* Market data owns subscription, instrument identity, venue sequence rules and publication. A strategy reads its own market replica. Each worker owns its algorithm state and output sequence.
* An account process owns cash, reservations, orders and economic attribution for one risk domain. The gateway synchronizes the command's recovery dependencies and admission record before the first application byte. At that byte it checks the current owner, lease and permission again.
* A connector owns venue authentication, native IDs and wire messages. Keep acknowledgement, order status and individual fill as distinct evidence. Durable input and trade identity make retries and restart reproducible.
* Load configuration, bind the venue and authenticate worker processes during startup. Keep these operations outside quote processing and order admission.

## Change a contract

Before changing behavior, specify the input, output, state owner and failure that the caller observes in the relevant [module specification](specs/implementation/README.md). Write the smallest failing regression that demonstrates the change, implement it and check the contract again. Register new modules in both specification indexes. If wire bytes or risk arithmetic change, update the matching [machine contract and golden vectors](specs/contracts/README.md). For an ordering or recovery change, check the corresponding [TLA+ model](tla/README.md) and its source anchors.

Treat the normalized market stream, intent identity and canonical account bytes as compatibility boundaries. During replay, use the original recorded input and decision time. Restore pending orders with their reservations intact until sufficient exchange evidence arrives.

## Review performance

For every design decision, identify the work paid at startup and the work paid per quote, order or fill. Count copies, allocations, queue hops, clock reads and system calls. Include saturation and tail behavior for a slow strategy or a full queue. Distinguish a cost inferred from code from a measurement. Record the hardware, load and latency interval beside benchmark results.

Use safe Rust by default. For a measured bottleneck, isolate any necessary unsafe operation behind a safe API, document bounds, aliasing, initialization and lifetime obligations, and compare it against the safe implementation. Keep the lint exception local to the audited boundary. The shared-memory and clock system crates provide existing examples. Use integer ticks, lots and checked fixed-point amounts in execution code.

## Verify and document

```sh
cargo fmt --all -- --check
cargo test --locked -p <affected-crate> --all-targets
cargo clippy --workspace --all-targets -- -D warnings
python3 scripts/verify-documentation.py
```

Before committing, run the workspace tests. For shared memory and file-descriptor changes, also run `./scripts/check-linux.sh`. For changes to an ordering, admission or durability rule, review the model's source anchors and run `./scripts/check-tla.sh`. Run the affected `scripts/verify-*.py` for wire, config, catalog or risk changes. The [test guide](scripts/README.md) covers the full workspace, Linux process tests and model checks. Commit related source, specification and verification changes together and push each verified stage to its tracked branch. Verify the remote revision and report the commit.

Write README files, specifications and contributor guides in English. Put a working command and its observable result in a README. Give a specification concrete inputs, transitions and acceptance cases. Use the crate guide to tell a contributor where to make a change and which tests exercise it. Keep captured traffic, checksums, golden data and historical evidence byte-exact.

The [README diagram](assets/architecture.svg) is hand-authored SVG. When the process boundary or message direction changes, update the SVG and its Markdown alt text together. Check that labels and arrowheads stay readable at 320 pixels wide.
