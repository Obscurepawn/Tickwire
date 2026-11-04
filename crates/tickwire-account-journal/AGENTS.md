# tickwire-account-journal

The account journal persists the baseline and the ordered account records used for recovery.

Check the previous hash and sequence on append and reopen. A writer change carries a new writer epoch while an outstanding order retains its original identity.

[Crate API](src/lib.rs) · [income closure](tests/income_closure.rs) · [ingress](tests/ingress.rs) · [persistence](tests/persistence.rs)

Specifications include [account journal](../../specs/implementation/account-journal.md).

```sh
cargo test --locked -p tickwire-account-journal --all-targets
```
