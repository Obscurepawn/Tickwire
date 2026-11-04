# Tickwire TLA+ models

From the workspace root, run the order submission models.

```sh
./scripts/check-tla.sh submission
```

The runner checks the checksum of the pinned TLC distribution, copies the models and their configurations to a temporary directory and checks each configuration with one worker. A completed run reports zero states left on the queue. After the last configuration for a model, the coverage report lists the number of successors generated for each action.

Pass another [model family](ROADMAP.md) to select a different boundary. Run `./scripts/check-tla.sh` for all families. When the Rust implementation changes, check the model's abstractions and the source revision recorded beside its anchors before using a past result.
