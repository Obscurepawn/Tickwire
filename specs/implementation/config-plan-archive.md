# Saved configuration plans

A caller can save the canonical bytes from `ComputedPlan` and reopen them with `EncodedPlan::decode`. The plan binds the base configuration, requested changes, affected accounts, projections and permitted actions. A rules digest identifies the source files, contracts, compiler and dependencies used by the planner.

The decoder checks the supplied digest, canonical encoding and ordered sets. To compare a saved plan against a new calculation, supply the original base revision and exact candidate, prepare their projections and compare the resulting bytes. An accepted plan describes the requested transition. Each running process supplies its own application receipt. [Round-trip and mutation tests](../../crates/tickwire-config/tests/plan_archive.rs) · [Configuration candidates](config-candidate.md)
