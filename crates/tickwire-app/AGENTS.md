# tickwire-app

The app reads one deployment file, starts its strategy workers and runs the account event loop. `inspect` and `run` share the [group preflight](src/launch/group.rs). Validate the 1–32 logical slots, instance identities, risk domain and member-owned paths before launching children. A shared market topic needs one cursor per worker and distinct JOIN requests within a supervisor epoch.

When changing [strategy scheduling](src/strategy.rs), verify that a busy or failed worker leaves its peers able to make progress and that a submission result returns to its origin. The [two-worker example](../../README.md#deploy-two-strategies) exercises distinct processes on one public subscription.

```sh
cargo test --locked -p tickwire-app --all-targets
python3 scripts/run-two-strategies.py
```
