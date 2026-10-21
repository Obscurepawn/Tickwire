# tickwire-mock 接手说明

## 1. 职责（架构位置：测试替身 / 执行内核的确定性外部服务）

确定性外部服务测试替身：内存关系库（`MockSql` 实现 `ConfigStore`）、不可变 KV（`MockKv` 实现 `ObjectStore`）、确定性日志（`MockJournal` 实现 `ports::Journal`）、可编排交易所（`MockExchange` 实现 `ports::Exchange`），以及参考编排器 `MockRuntime`。堆分配只允许出现在测试替身中，热 reducer 仍在 `tickwire-execution`。

**本 crate 仅用于测试与本地闭环演示，不得进入生产依赖。**

架构位置：执行内核端口的内存实现，配合 example 跑通"本地市场→策略→风险→IPC→WAL→交易所→成交/恢复"全闭环，供 TLA L0/L1/L2 与回归测试对照。

## 2. 公共 API 入口

- `MockSql`（`new(payload)`、`unavailable` 开关，实现 `ConfigStore::load/compare_exchange`）。
- `MockKv`（`fail_writes` 开关，实现 `ObjectStore::put_immutable/get`；同 key 同字节幂等，异字节 `Conflict`）。
- `Fact`（重导出 `tickwire_execution::ports::ExecutionFact`）、`MockJournalEvent`（Intent(DurableRecord)/Fact(Fact)）、`MockJournal`（`objects: MockKv`、`records`、`facts`、`events`；故障开关 `fail_next_commit`/`lose_next_receipt`/`fail_next_fact`/`replica_ready`；`events()` 给实际追加顺序）。
- `MockExchange`（`next_outcome: SubmitOutcome`、`accepted`、`submit_calls`）。
- `RuntimeError`（Domain/Service 两分）。
- `MockRuntime<N, T>`：`new`、`submit`、`resume_submit`、`record_fill`/`record_fee`/`record_terminal`、`restart`；公开字段 `engine`/`journal`/`config`/`exchange`。

## 3. workspace 位置

- 上游依赖：`tickwire-execution`（端口与引擎）、`tickwire-book`、`tickwire-risk`、`tickwire-ipc`。
- 下游：**无任何 crate 在 Cargo.toml 中依赖本 crate**（grep 全仓仅命中自身）；它是独立的测试/example 载体，由开发者直接 `cargo test`/`cargo run --example` 使用。

## 4. 对应规格

- [../../specs/implementation/execution-core.md](../../specs/implementation/execution-core.md) — 内核与 MockService 行为基线。
- [../../specs/implementation/mock-abort.md](../../specs/implementation/mock-abort.md) — 未发送中止事实与顺序恢复在替身中的确定性复现。

## 5. 构建与测试

```sh
cargo test -p tickwire-mock
cargo run -p tickwire-mock --example spot-roundtrip
```

- `tests/services.rs`：21 个测试（关系 CAS/不可变 KV 契约、缺恢复依赖阻止 commit、存储失败不到交易所、KV 失败不到日志/交易所、ACK 丢失与恢复不重发、已提交但丢回执保守恢复、同步 replica pending 不降级本地、事实在存储失败时不被 apply、场所中立/域不混、空日志 restart 仍需恢复授权、healthy async pending 区别于超时、持久中止释放预算不在 restart 重占、无字节中止存活 restart 等）。
- `examples/spot-roundtrip.rs`：本地确定性闭环（book→risk 预留→IPC IntentRecord→MockRuntime 提交→重复成交去重→终态/费用→restart），文件内自带确定性断言测试；运行命令见上。

## 6. 关键不变量与注意事项

- **仅测试用，不得进生产**：lib.rs 首句 "Heap allocation is allowed in test doubles, not hot reducers"；不要把 MockSql/MockKv/MockJournal 接到真实适配器。
- **确定性故障注入**：`fail_next_commit`（commit 前失败）、`lose_next_receipt`（commit 已落盘但丢回执）、`fail_next_fact`（事实追加失败）、`replica_ready`（控制 SyncReplica 回执）均为一次性 `std::mem::take`，用后复位。
- **事实先持久化后应用**：`MockRuntime::record` 先 `journal.append_fact`，失败则 `engine.freeze()` 并返回错误，**仍保留收到的事实**（恢复时报告未决事实而不是丢弃金融证据）；成功后才 `fact.apply`。
- **restart 先冻结旧引擎**：`restart()` 先 `self.engine.freeze()`，重建的新引擎也先置 frozen，再按 `journal.events()` 顺序 `recover_intent`/`fact.apply`；重启不恢复任何发送权限（"No permission is restored"）。
- **交易所结果可编排**：`next_outcome` 逐次替换为默认 Acknowledged；`MayHaveSent` 走 `uncertain` 冻结，`NoBytesAccepted` 走 AbortedBeforeSend 事实，`PendingResponse` 保持等待窗口内状态，绝不发隐藏重试。
- **无睡眠无重试**：参考编排器没有 wall-clock sleep 或隐藏重试，时间推进由调用者显式驱动。
- **依赖字节是占位**：`dependency_bytes` 用 `format!("{:?}")` 调试字节，仅测试用；生产依赖走内容寻址 RecoveryManifest ABI（见 tickwire-recovery）。
- config 版本核对：`submit`/`resume_submit` 都先核对 `config.load().revision == request.config_revision`，不符返回 Service Conflict。

## 7. 改动检查清单

1. `cargo test -p tickwire-mock` 全绿，并手跑 `cargo run -p tickwire-mock --example spot-roundtrip` 核对输出（submit_calls=1、spent=101、held=1、frozen=true）。
2. 改 `ports` 实现后，同步 `tickwire-execution` 的 trait 签名与 services.rs 故障用例。
3. 新增故障开关时保持一次性 take 语义，并在 services.rs 补对应确定性测试。
4. 不要在此引入网络、时钟等待或自动重试；任何不确定行为都会破坏 TLA 对照。
5. example 改动需保持两次 `run_scenario()` 结果完全相同（确定性断言）。
