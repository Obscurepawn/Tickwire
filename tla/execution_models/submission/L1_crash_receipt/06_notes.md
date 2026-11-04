# Source review

Read `MockRuntime::submit`, `resume_submit` and `restart` together with `Engine::accept_commit`. The mock serializes calls using a mutable owner. The separate crash steps model loss of process state inside one submit operation. The recovery contract here covers the intent-only history. [Mock runtime](../../../../crates/tickwire-mock/src/lib.rs) · [Archived model, regressions and earlier review](https://github.com/Obscurepawn/Tickwire/tree/dc50649ebdcada26f7740181c16139cd3b32064a/tla/execution_models/submission/L1_crash_receipt)
