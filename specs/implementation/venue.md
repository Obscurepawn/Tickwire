# Canonical trading commands and venue events

`tickwire-venue` gives the executor and its recovery store the same immutable command bytes and SHA-256 identity. It validates a command or observation before encoding it. Network submission belongs to the adapter. Asset accounting belongs to the account owner. [Public types](../../crates/tickwire-venue/src/types.rs) · [Constructors and decoders](../../crates/tickwire-venue/src/lib.rs)

## Business identity

* The request binds the risk domain, venue, environment, account, product, original intent and execution epoch, strategy generation, configuration revision, deadline and recovery object. The strategy context includes the logical strategy and instance IDs, intent sequence, authorization epoch, risk-policy and order-configuration versions, creation time, economic intent, hedge group, leg and existing recovery-manifest hash. The executor assigns journal sequence numbers separately.
* The instrument binding carries instrument and catalog identity, versions and digest, native symbol, base, quote and settlement assets, quantity and price increments, contract kind and applicable delivery or expiry time. Native symbols preserve their UTF-8 bytes. Whitespace and control characters are rejected. Case and Unicode form are significant.
* The order contains its client ID, side, type, quantity unit, price, time in force, position side, trading modes, reduce-only flag, self-trade prevention, trigger, trailing and iceberg terms. Spot trailing deltas use basis points. Derivative trailing parameters retain their percentage units and working-price source. A management command preserves the original order identity, management revision and original execution epoch. Amend, modify and cancel-replace encode their new parameters explicitly.
* A list or algorithm has its own parent identity. Spot OCO legs are ordered `[above, below]`, OTO legs `[working, pending]`, and OTOCO legs `[working, pendingAbove, pendingBelow]`. The typed command enum also contains explicitly supported spot extensions. A message can be representable even when a specific gateway declines to execute it. [Command validation](../../crates/tickwire-venue/src/validate.rs)

`Command` is an editable draft. `CanonicalCommand` privately owns the validated command, its encoding and its digest and exposes read-only access. Both direct construction and decoding enforce the semantic checks. [Command binding](../../crates/tickwire-venue/src/lib.rs)

## Observation and fill identity

A venue event records the account scope, product, instrument version, source connection epoch, optional source sequence, event ID and timestamps. An order observation retains original intent and execution epoch, local order and parent IDs, and client and venue order/list/algo IDs where known. An uncorrelated event uses zero internal order and intent IDs and must retain a native identity for later association. A new connection changes provenance. The venue trade ID still identifies the economic fill. [Event types](../../crates/tickwire-venue/src/types.rs)

Fill observations distinguish order price and cumulative quantities from the last trade's price, quantity, liquidity and native ID. Optional reported cumulative quote, last quote and realized PnL remain distinct. A missing value stays `None`. Realized PnL carries its own `AssetDelta`. One event can carry signed fees in up to eight distinct assets. Each fee identifies the asset, version, symbol and exponent. Repeated asset identities within that event fail validation. An adapter may aggregate fees for the same asset or emit a separate fee event when supported. [Event tests](../../crates/tickwire-venue/tests/events.rs)

## Encoding and validation

The top-level CBOR arrays have the following fixed shape.

```text
command = ["tickwire.venue-command", 1, CommandContext, CommandBody]
event   = ["tickwire.venue-event", version, EventIdentity, VenueEventBody]
digest  = SHA256(complete top-level encoding)
```

Ordinary order, fill and fee events use version 1. Structure observations use version 2. Amendment observations and rejections use version 3. Decoding requires the version for that event body. Structs encode in declared field order. Options encode as `[]` or `[value]`. Bounded lists use definite-length arrays. Enum tags and the arity of each complex variant are explicit. Unknown tags, missing or surplus fields, trailing data and encodings that fail canonical re-encoding are rejected. Changes to existing wire meanings require versioning and compatibility checks. [CBOR codec](../../crates/tickwire-venue/src/codec.rs) · [Test vectors](../../crates/tickwire-venue/tests/vectors.rs)

Integers, lengths and CBOR bignum tags 2 and 3 use their shortest representation within the supported `i128` range. A negative integer uses `-1 - magnitude`, including `i128::MIN`. A decimal is `[coefficient, exponent]`, normalized over exponents −38 through 38. Zero is exactly `[0, 0]`. Floating point and indefinite-length items are rejected. The input limit is 16 KiB. String and array bounds are checked before allocating. [CBOR parser](../../crates/tickwire-venue/src/cbor.rs) · [Strict decoding tests](../../crates/tickwire-venue/tests/strict.rs)

Validation covers required non-zero identities, deadlines, trigger references, product and contract combinations, price ticks and quantity steps, order-type/TIF/price combinations, close-position rules and list structure. Ordinary spot market buys and sells can represent quote quantity. Tick alignment uses unsigned remainders to avoid overflowing a multiplication when decimal exponents differ. An adapter and the trusted catalog must still verify venue support, current market constraints, bands, fees, minimum notional, account modes, management grants and reductions against the actual order. Transport timestamps and request IDs do not rewrite client order identity or the business deadline. [Validator](../../crates/tickwire-venue/src/validate.rs) · [Command-binding model](../../tla/execution_models/business_journal/L1_command_binding/00_input.md)
