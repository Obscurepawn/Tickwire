# Bootstrap Configuration Locator and Root-Migration Chaining

`tickwire-config::locator::Locator` parses, validates, and hashes the fixed `ConfigLocator` bootstrap pointer from either JSON or TOML, establishing a format-independent content identity (`Locator::hash()`) and cryptographic predecessor chain across configuration-directory migrations.

`Locator` owns pure schema, path-hygiene, checksum, and successor-link validation (`validate_successor`). It confers no root-activation authority on its own: host bootstrap code independently verifies static startup configuration files and prior process termination before switching an active configuration root.

## State Model and Bounds

`Locator::decode` parses JSON or TOML through `Candidate::parse` under tight bootstrap limits (`MAX_BYTES = 16 KiB`, `max_depth = 8`) and validates the document offline against `#/$defs/ConfigLocator` in `system-config.schema.json`.

| Field or Digest | Enforced Representation / Bound |
| --- | --- |
| `locator_version` & `generation` | `locator_version == 1`; `generation` encoded as a canonical decimal string in `1..=2^63 - 1` |
| `cluster_id` & `active_root` | `cluster_id` length `1..=63` bytes; `active_root` absolute path (`<= 4,096` bytes) starting with `/` with no empty segment (`//` or trailing `/`), `.`, or `..` |
| `migration_id` | Lowercase canonical 36-character UUID (`[0; 16]` permitted only at `generation == 1`; must be non-zero for `generation > 1`) |
| `manifest_hash` & `previous_locator_hash` | Lowercase `"sha256:<64-hex>"`; `manifest_hash != [0; 32]`; `previous_locator_hash == [0; 32]` iff `generation == 1` |
| Embedded `checksum` vs. `Locator::hash()` | `checksum` = `SHA-256` of RFC 8785 canonical JSON with `"checksum"` removed; `Locator::hash()` = `SHA-256` of full RFC 8785 canonical JSON including `"checksum"` |

## Core Invariants

* **Format-agnostic canonical identity**: Because both JSON and TOML inputs normalize into the same RFC 8785 canonical JSON object, a `ConfigLocator` serialized as TOML (`Locator::toml()`, also capped at `16 KiB`) and its equivalent JSON document produce identical `checksum` fields, identical `canonical_bytes()`, and identical `Locator::hash()` digests.
* **Genesis versus migrated root invariants**: Generation `1` must carry an all-zero `previous_locator_hash` (`[0; 32]`), whereas every generation `> 1` must carry both a non-zero `previous_locator_hash` and a non-zero `migration_id`, preventing an unchained locator from masquerading as a migrated successor or vice versa.

## Operations and Failure Semantics

* **Strict root-migration chaining (`validate_successor`)**: `old.validate_successor(&next)` succeeds only when `next.generation == old.generation + 1` (checked against overflow), `next.cluster_id == old.cluster_id`, `next.active_root != old.active_root`, `next.previous_locator_hash == old.hash()`, and `next.migration_id` is both non-zero and distinct from `old.migration_id`. Any skipped generation, unchanged root directory, broken hash link, or reused migration UUID fails with `Error::Semantic("locator successor")`.

## Verification and References

```sh
cargo test --locked -p tickwire-config --test locator
cargo clippy --locked -p tickwire-config --all-targets -- -D warnings
```

Automated tests verify JSON/TOML hash equivalence, `checksum` tamper rejection, path-traversal and trailing-slash rejection on `active_root`, generation-1 vs. post-migration zero-hash rules, and `validate_successor` enforcement across root and generation transitions.

* Locator implementation and schema contract: [locator.rs](../../crates/tickwire-config/src/locator.rs), [system-config.schema.json](../contracts/system-config.schema.json)
* Locator test suite: [locator.rs](../../crates/tickwire-config/tests/locator.rs)
* Related specifications: [Configuration candidates](config-candidate.md), [Archived rollout plans](config-plan-archive.md)
