# Source review

Check `Engine::reserve`, `accept_commit`, `begin_send` and `transport_rejected_without_bytes` when changing order admission. The durable record is local. The reducer receives the gate as an input, so clock freshness and socket timing belong to runtime tests. [Execution reducer](../../../../crates/tickwire-execution/src/lib.rs) · [Archived model and earlier review](https://github.com/Obscurepawn/Tickwire/tree/dc50649ebdcada26f7740181c16139cd3b32064a/tla/execution_models/submission/L0_commit_gate)
