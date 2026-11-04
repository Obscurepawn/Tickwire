# Tickwire TLA+ models

The [model inventory](ROADMAP.md) identifies the source revision, state domain, failure assumptions and result for each layer. The models cover order submission and outcomes, shared rings, control requests, journals, recovery closures and order books. Each directory includes the specification, configuration, source mapping and recorded run.

Run the checker from the workspace root.

```sh
./scripts/check-tla.sh
```

The script verifies the checksum of TLC 1.7.4, takes a temporary copy of each model and runs its configurations with one worker and action coverage. A successful configuration exhausts the search queue. The coverage check uses generated action counts across the configurations of one model. These counts describe model transitions. Rust source coverage has a separate measurement.

When an implementation changes, compare the code against `02_models/anchors.lock` and the model's abstraction notes before applying an earlier result to the new revision.
