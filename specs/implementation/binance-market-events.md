# Binance event publication

One public event owner subscribes to one product, instrument and event category. It decodes complete source frames, converts prices and sizes using the approved instrument increments, and delivers typed events to the topic publisher. A depth feed can run alongside these sources. [Event decoder](../../crates/tickwire-binance/src/market.rs) · [Publisher](../../crates/tickwire-market-publisher/src/events.rs)

| Input | Published observations |
| --- | --- |
| Spot JSON | Individual trade and best bid/ask |
| Spot production SBE | Individual trade and best bid/ask |
| USD-M JSON | Aggregate trade, best bid/ask and mark/funding |

## Source and wire semantics

The publisher rejects an off-grid or overflowing price or quantity. Each exact record retains instrument and version, source ID, connection epoch and encoding. A resource binds the catalog. Message types 264, 265 and 266 share a 128-byte header and 384-byte body. They require feature bit 9 end to end, including the ring and JOIN snapshot. Types 257–263 retain their original encoding. [Exact record validation](../../crates/tickwire-ipc/src/market_exact.rs)

* **Trade (264).** Preserve aggregate ID, first/last raw trade IDs, total quantity and optional non-RPI quantity when the venue supplies them.
* **Best bid/ask (265).** Keep one bid and ask, quantities and source sequence. Spot JSON has no exchange timestamp for this event. Its validity bit remains clear. SBE microseconds convert to nanoseconds with overflow checking.
* **Mark/funding (266).** Keep mark, index and estimated settlement prices and funding fields. Missing delivery fields differ from numeric zero. `DecimalWire` stores a signed little-endian 128-bit coefficient, exponent in −38…38 and seven reserved bytes. Normalize zero and trailing decimal zeros.

An SBE owner validates the entire binary group before delivering its first row. If a callback budget runs out, retain the row position. Reject an unknown schema, template, version or truncated group. Spot production SBE authenticates the Upgrade with an API key. Public TLS verifies the hostname. Connect, Upgrade, Subscribe and Pong consume the configured public permits. Explicit subscriptions require the exact success ACK after the request has been sent. If EOF arrives in the same batch as complete data, invalidate the connection first and deliver those observations with `usable=false`.

## What a strategy can read

Each instrument has a slot for Quote (1), Trade (2), MarkFunding (4) and FeedState (8). A configured independent BBO supplies the quote slot. Depth levels and coverage still come from the depth source. Each event carries its own ingress time. On a Binance depth topic, an independent trade or BBO must never make old depth look fresh. FeedState requires continuous depth and all configured event categories.

A reader joins at a complete snapshot `S` and applies every topic record from `S+1`. A gap, identity change or capacity violation blocks its replica. The snapshot state sequence numbers identify rows within the snapshot. `last_event` records the original topic position. A `TWMR0001` checkpoint includes the materialized instrument views and blocked flag, with a 4096-instrument and 64 MiB bound. Restore checks the digest, identity, record semantics, bounds, coverage, sort order and original depth capacity. The strategy’s input log captures the canonical records and arena payloads it consumed.

## Storage and hot path

Stable `u64` instrument IDs appear on the wire, in triggers, checkpoints and the input log. Each topic maps them to contiguous local slots at setup. MD binds a publishing slot to the stable ID of its row. A `BookKey` combines venue, local instrument and source. Slot zero is valid. Reusing a full key within a topic is an identity error, even if the external IDs have distinct high bits. A successor with the same catalog preserves the mapping. [Instrument index](instrument-index.md)

The publisher reserves four state entries and two depth arrays per row. A topic shares scratch space for 4096 total delta levels and 4096 encoded levels across all rows. A cached side may contain up to 5000 prices. A normal depth update uses a fixed array of up to three records, then publishes them in one batch. The allocation tests exercise first and later JSON/SBE publications, the 12/13 inline-to-arena boundary and a 2 MiB thread stack. Their allocation scope starts at publication. TLS and upstream parsing have separate costs.

`cache_bytes` covers retained heap capacity for rows, their states and levels, catalog strings, decoder contexts, index and workspaces. `reserved_cache_bytes()` uses actual capacities. Allocator overhead, the call stack, SHM mappings and JOIN snapshots have separate budgets. The consumer merges sorted updates in two shared workspaces, checks capacity and crossing on both sides, then commits. At its maximum of 10,000 levels per side, those two workspaces occupy 320,000 bytes. A covered, absent price returns zero. A missing price outside coverage returns unknown.

A rejected view cannot modify the wrong instrument. If raw decoding or publication fails after a valid view was cached, the publisher can retain that newer view. The published prefix does not advance. The fault sink invalidates the bank, closes the live gate and prevents new snapshots until the successor is ready.

## Add an instrument

`prepare_expansion` creates a private candidate for a strict superset of the current instruments, with the same catalog and a greater set version. Existing versions, required states, book keys and event source bindings must match. Only new rows are warmed. A gap or unusable event clears readiness. Before committing, check the candidate’s freshness, the old subscribers’ detachment, and the old and new cache budgets, including the peak while both exist.

The next bank advances feed and ring generations, points to its predecessor and starts at the old published sequence plus one. Commit copies current state, validates snapshot size, republishes all member states, then freezes the old bank. An invalid plan or failed candidate write leaves the old publisher running. A freeze failure invokes the fault sink. [Expansion implementation](../../crates/tickwire-market-publisher/src/expansion.rs)

## Verify

```sh
cargo test --locked -p tickwire-md --test market_events --test publisher_hot --test instrument_slots --test sbe_publication
cargo test --locked -p tickwire-market-publisher -p tickwire-ipc
```

The seven event combinations run against local TLS/WebSocket fixtures and real shared memory on Linux. The tests inspect exact records, late JOIN, replica checkpoints, independent freshness and publication capacity failures. Byte fixtures for exact IPC records and replay control remain separate from codec-to-codec round trips. [Event round trips](../../crates/tickwire-md/tests/market_events.rs) · [Allocation and capacity tests](../../crates/tickwire-md/tests/publisher_hot.rs) · [IPC vectors](../../crates/tickwire-ipc/tests)
