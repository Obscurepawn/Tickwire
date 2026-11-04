# tickwire-account

This process owns multi-asset balances, holds, orders and fills for one risk domain. Strategies share the available balance and retain distinct origin and manager identities.

Trace admission into the reservation and the journal image. For a late fill, compare cumulative observations with the individual trade IDs already applied. A confirmed fee, partial send or uncovered fill affects how much cash can be released.

[Crate API](src/lib.rs) · [account inputs](tests/account_inputs.rs) · [admission](tests/admission.rs) · [amend](tests/amend.rs) · [conditional](tests/conditional.rs) · [edge cases](tests/edge_cases.rs)

Specifications include [account](../../specs/implementation/account.md), [conditional orders](../../specs/implementation/conditional-orders.md), [spot amend](../../specs/implementation/spot-amend.md), [usdm account facts](../../specs/implementation/usdm-account-facts.md).

```sh
cargo test --locked -p tickwire-account --all-targets
```
