# Nonblocking transport and framing

`tickwire-transport::Connection` uses a reactor, mio 1.2.4 and rustls 0.23.46. Resolve addresses before constructing the connection. The caller supplies readiness and schedules the next poll. The same crate also exposes independent HTTP, WebSocket and FIX framing codecs. Bybit/OKX public quote ingestion reuses those codecs on a dedicated blocking I/O thread. [Connection](../../crates/tickwire-transport/src/connection.rs) · [Spot input](spot-quote-input.md)

## The first-byte boundary

`queue` stores one immutable application message. It does not give the TLS writer any application bytes. Handshake traffic can already be on the wire.

`try_send` checks readiness and expiry. While accepted bytes remain zero, it calls the authorization gate immediately **before** writing plaintext to rustls or the socket, resamples the deadline after the gate, and rejects an expired send. A zero-byte or WouldBlock attempt must pass the gate again later. The first positive write changes the state to `MayHaveSent`. Subsequent progress uses the same buffer and offset.

A cancel or failure can return `NoBytes` only while accepted bytes equal zero. After any positive acceptance, retain the count and report failure/uncertainty. Cancellation closes the TLS session and socket. `Flushed` requires the complete message to be accepted and all pending TLS write data to drain. An EOF observed in that poll cannot erase an already completed flush. Local flush does not acknowledge an order at the venue.

## Per-poll work and codecs

| Boundary | Contract |
| --- | --- |
| Network | Limit calls, bytes and decrypted plaintext deliveries per poll. Stop reading when receive capacity is full. Verify certificate roots and hostname. Reject TLS early-data configuration. |
| HTTP | Reject header injection, repeated lengths, ambiguous Transfer-Encoding/Content-Length, overlong chunks and truncation. A HEAD response’s Content-Length never causes a body allocation. |
| WebSocket | Validate Upgrade/Accept, masked client frames, server flags and minimal lengths, control frames, fragmentation and UTF-8. A decoder distinguishes one complete message, a need for more bytes and exhaustion of this poll’s processing budget. |
| Text FIX | Locate CheckSum using BodyLength and validate it. Reject truncated, corrupt and oversized messages. |

Request preparation and parsing may allocate within their bounds. Session owners handle venue errors, reconnect, WebSocket heartbeat and Close, FIX logon/sequences and business acknowledgements. A send ID is local to one connection. Higher layers bind a stable order identity.

```sh
cargo test --locked -p tickwire-transport
```

Tests use actual TCP/TLS backpressure, wrong hostnames, deadline and cancellation boundaries, fragmented WebSockets and HTTP chunking. The finite first-output model checks acceptance and expiry, without modeling TLS cryptography or hardware transmit time. [Network tests](../../crates/tickwire-transport/tests/network.rs) · [Frame tests](../../crates/tickwire-transport/tests/framing.rs) · [First-output model](../../tla/execution_models/transport/L0_first_output/05_output.md)
