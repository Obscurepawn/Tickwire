# tickwire-lifecycle 接手说明

## 1. 职责（架构位置：生命周期协调器 / 签名控制面）

多成员策略生命周期的签名协调器与 CLI：`Coordinator<S: Store>` 以审计 WAL 为唯一状态来源，驱动 Prepare→Commit→Release→Active→Reconciling→Resume/Halt/Frozen 阶段机；`replay`（schema 3 重放准备/观察）与 `rebuild`（schema 4 重建准备/观察）两条成员 adoption 线；`wal` 负责事件追加与恢复；`cli` 提供面向目录的操作员循环（读请求目录、写回执、签名、轮询 readiness）。二进制 `tickwire-lifecycle` 即 CLI 本体。

架构位置：签名控制面（与 `tickwire-runtime-policy` 的实时门禁互补：本 crate 管阶段迁移与签名收据，不管逐命令准入）；被 `tickwire-lab` 集成、`tickwire-release`/`tickwire-replica`/策略宿主消费签名产物。

## 2. 公共 API 入口

- `coordinator`：`MemberAuthority`、`CoordinatorConfig`、`Stage`（Preparing/PreparedAll/Committing/PartialCommitted/CommittedAll/Releasing/ActiveAll/Reconciling/AwaitingResume/Halting/Halted/Frozen）、`ApplyResult{version,stage,duplicate}`、`Coordinator<S>`（`create`/`recover`/`config`/`version`/`stage`/`audit_position`/`audit_frontier`/`pending_requests`/`latest_receipt`/`group_release`/`begin_commit`/`release`/`halt`/`query_applied`/`update_readiness`/`resume_after_queries`/`ingest`/`refresh_replay`/`ingest_replay_observed`/`ingest_replay`/`refresh_rebuild`/`ingest_rebuild_observed`/`ingest_rebuild`）。
- `replay`：`ReplayObject`/`ReplayCut`/`ReplayPrepared`/`ReplayAdoption`/`SignedReplayPrepared`/`SignedReplayObserved`、`sign_replay_prepared`/`verify_replay_prepared`/`sign_replay_observed`/`verify_replay_observed`。
- `rebuild`：`RebuildCursor`/`RebuildFrontier`/`RebuildRoots`/`RebuildPrepared`/`RebuildAdoption`/`SignedRebuildPrepared`/`SignedRebuildObserved` 及对应 sign/verify。
- `wal`：`Wal<S>`（`create`/`recover`/`append`/`position`/`frontier`）。
- `cli`：`Deployment`（coordinator/writer_epoch/recover/resume_after_queries/signer_file/audit_directory/receipts_directory/requests_directory/readiness_directory/maximum_wait_ns/poll_interval_ns）、`run(path, halt)`。
- `lib.rs`：`Error`、`Member`、`ControlAction`、`SignedControlRequest`、`SignedReceipt`、`SignedGroupRelease`、`Readiness`。

## 3. workspace 位置

- 上游依赖（Cargo.toml 实测）：`tickwire-clock`、`tickwire-clock-sys`、`tickwire-journal`；ring 0.17、serde、serde_json、sha2。本 crate 不依赖账户/执行/恢复/额度等业务 crate，签名与阶段状态在内部自洽。
- 下游（grep `Cargo.toml` 核对）：`tickwire-app`、`tickwire-lab`、`tickwire-release`。

## 4. 对应规格

- [../../specs/implementation/lifecycle-coordinator.md](../../specs/implementation/lifecycle-coordinator.md) — 协调器阶段机、签名收据、replay/rebuild adoption 与目录协议主规格。

## 5. 构建与测试

```sh
cargo test -p tickwire-lifecycle
```

- 测试内置于源码（无 `tests/` 目录）：`src/tests.rs`、`src/rebuild_tests.rs`（经 `#[cfg(test)] mod` 接入），共 34 个 `#[test]`，覆盖阶段迁移、重复回执幂等、签名/验签、replay schema 3 与 rebuild schema 4 边界、WAL 恢复。
- CLI 运行：`tickwire-lifecycle [halt] --config <deployment.json>`（`run(path, halt)`；`halt=true` 在 create/recover 后立即进入 Halted）。

## 6. 关键不变量与注意事项

- **版本绑定**：每个控制动作绑定精确 `expected_version`，`ApplyResult.duplicate` 表示已应用的重复回执；版本不符拒绝，不得跳版本迁移。
- **签名先于发布**：Prepare/Commit/Release/Halt 均由 coordinator 私钥签名，成员侧 `verify_*` 独立验签；测试传输保持原始签名字节（strategy-handover.md）。
- **Release 只能在真实 Committed 回执之后**：`release()` 要求全部成员 CommittedAll 收据齐备，不得凭请求直接生成 GroupRelease。
- **resume 必须等齐 QueryApplied**：`resume_after_queries` 语义下，只有所有当前 QueryApplied 回复到达才退出 AwaitingResume；不用固定等待时间代表完成。
- **schema 分层**：replay 线 schema 3（`p.schema != 3` 拒绝），rebuild 线 schema 4（`operation.schema != 4` 拒绝）；adoption 收据与准备对象必须同 schema、同成员、同请求摘要。
- **目录私有且互斥**：cli `private_directory` 对 audit/receipts/requests/readiness 目录做权限检查，且三者不得指向同一目录（Identity 错误）；成员按 risk-domain-hex 子目录文件名顺序消费。
- **WAL 为唯一状态来源**：`recover` 重放事件重建 State，内存状态不得脱离 WAL；单事件 ≤4MiB、事件总数 ≤4096、WAL ≤128MiB 上限。
- **时钟域**：CLI 用 `tickwire-clock-sys::SystemClock` 绑定 `clock_boot_id/clock_generation` 构造的 ClockDomain；域不符/回退直接失败。

## 7. 改动检查清单

1. `cargo test -p tickwire-lifecycle` 全绿。
2. 改阶段机（Stage 迁移边）时，在 tests.rs 补正向/重复/版本冲突用例，并对照 lifecycle-coordinator.md 状态图。
3. 改签名结构（replay/rebuild receipt）必须同步 sign/verify 两侧与 `tickwire-release`、`tickwire-replica` 的消费。
4. 改 WAL 事件格式时保证旧日志可 recover（或显式标注不兼容并迁移）。
5. CLI 目录协议改动同步 `tickwire-lab` 的场景编排与 README 用法。
