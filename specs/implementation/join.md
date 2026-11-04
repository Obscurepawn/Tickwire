# Join a shared market topic

The owner retains a topic cut `S`. It gives the registered subscriber a sealed snapshot at `S` and read-only access to subsequent bank records. The consumer builds its replica, catches up and proves its cursor before the owner marks the subscription active. Each subscription has its own cursor. Binance depth topics and Bybit/OKX BBO-only topics use this same protocol. The topic’s required states determine what the baseline contains. [Owner](../../crates/tickwire-join/src/lib.rs) · [Mapped consumer](../../crates/tickwire-join/src/mapped.rs)

## Owner operations

| Operation | Evidence and effect |
| --- | --- |
| `prepare` | Authenticate the supervisor, subscription, membership and deadline. Install the publisher's pending gate before building and sealing the snapshot. A build, seal or result-store failure retains the gate. Replaying a registered request cannot build a different cut. |
| `deliver` | Match the authenticated subscriber to its complete registration. Offer the bank and snapshot with read-only capability masks 0x17 and 0x0f. A failed send retains the reservation and marks the registration failed. |
| `receive_cursor` | Register the one qualified SDK writer and exact cursor descriptor. Reject a replacement mapping or a premature released value. |
| `read_ready` | Confirm each resource independently against its original offer, header, length, seals, access, generation and membership version. Failure does not set the confirmation bit. |
| `join_ready` | Require both confirmations, a registered cursor, a complete snapshot satisfying required states, and `applied >= published` sampled in this call. If behind, return the catch-up target. An authenticated retry with the original body and membership version returns its original activation watermark. |
| `expire` | An expired JOIN or backward clock can move a pending registration to `Expired`. Its gate remains installed. |
| `quiesced` | Require the mapped cursor in lifecycle 5 with no outstanding borrow and a final watermark equal to `quiesced_seq`. A matching retry returns the full ACK. |
| `all_detached` | True for an empty registry or when **every** stored registration is detached. Pending, active, failed, expired and quiescing peers still count. EOF and timeout do not prove detachment. |

`JoinOwner<M>` keeps at most `M` historical registrations, including detached entries. The single writer serializes ownership changes. Consumers independently advance their own mapped cursors.

## Drive a nonblocking client

Create `PendingJoin::new(config, now)` with the approved peer, bank, catalog, members, clock and startup deadline. Each `poll(now, budget)` performs at most `min(budget, 64)` protocol steps or records. A zero budget is rejected. The driver uses nonblocking connect/send/receive. On backpressure or EINTR it retains the phase, pending bytes and any FDs.

Receive WELCOME and validate it in separate polls. The receipt rejects attached FDs and saves a bounded packet. Sample the same clock after receipt, then validate the handshake and lease. This ordering allows the issuer to have created its lease after the first clock sample. Startup expiry and backward time still fail the join.

After mapping the baseline, apply records in topic order. Retain each catch-up record, payload and observation time before advancing the cursor. The retained prefix is limited to bank capacity and 64 MiB. The catch-up target cannot retreat. Resend JOIN_READY when caught up. `take_client()` yields a client once, after authenticated Active status binds the complete JOIN and its watermark has been applied.

Cancel, drop or failure closes local resources. It cannot acknowledge QUIESCED on behalf of a dead consumer. `Client::connect` drives the same state machine as a blocking convenience entry point. A poll budget bounds work items. Mapping, validating and instantiating one sealed snapshot is still a cold step bounded by 64 MiB, 4096 members and the permitted number of levels.

## Verify

```sh
cargo test --locked -p tickwire-join -p tickwire-market-source
```

The Linux flow authenticates three processes, transfers real descriptors with SCM_RIGHTS, maps an immutable snapshot, catches up and checks that a held record guard prevents detach. Owner recovery, reclaiming dead peers and granting execution risk have separate authority. [Real process tests](../../crates/tickwire-join/tests/process_linux.rs) · [Owner state tests](../../crates/tickwire-join/tests/owner.rs) · [Finite-state model scope](../../tla/execution_models/join_owner/PLAN.md)
