# tickwire-account-codec

This crate encodes account baselines and ordered recovery records. Other processes use the canonical bytes to verify the same history.

Keep bounds, field order, sequence and baseline digest stable across a restore. Exercise round trips, truncated input and single-byte mutations when changing encoding.

[Crate API](src/lib.rs) · [account inputs](tests/account_inputs.rs) · [amendment](tests/amendment.rs) · [baseline](tests/baseline.rs) · [file replay](tests/file_replay.rs) · [ingress](tests/ingress.rs)

Specifications include [account codec](../../specs/implementation/account-codec.md).

```sh
cargo test --locked -p tickwire-account-codec --all-targets
```
