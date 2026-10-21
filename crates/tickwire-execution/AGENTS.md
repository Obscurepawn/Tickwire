# tickwire-execution 接手说明

## 1. 职责（架构位置：执行域内核）

单所有者、与场所无关的执行原语内核：订单/引擎状态机。所有状态转换是纯 reducer，**无 I/O、无堆分配**（`src/lib.rs` 顶部注释："No I/O or allocation on transitions"，`#![forbid(unsafe_code)]`）。它不认识 Binance，不做网络、不做持久化；持久化、交易所、配置、对象存储全部通过 `ports` 抽象边界由外部适配器或 mock 实现。

架构位置：执行域最底层的资金/订单账本内核，上接业务 WAL（`tickwire-business-journal`）、发送网关（`tickwire-gateway`）与测试替身（`tickwire-mock`）。

## 2. 公共 API 入口

- 核心类型：`Product`（Spot/UsdM）、`Domain`（venue/account/product）、`OrderKey`（含 `intent`）、`OrderRequest`（`command_digest: [u8;32]`、`quantity`、`unit_budget`、`fee_budget`、`epoch`、`strategy_generation`、`config_revision`、`deadline`、`recovery_object`、`commit_profile`）。
- 门控：`Gate`（`epoch`/`strategy_generation`/`config_revision`/`now`/`lease_until`/`enabled`/`market_valid`/`account_valid`/`recovered`/`owner_isolated`），私有 `permits()` 做九项布尔校验。
- 状态：`SendState`（NotSubmitted/MayHaveSent/Confirmed）、`Terminal`（Filled/Canceled/Rejected/Expired）、`CancelState`、`CommitProfile`（LocalDurable/SyncReplica）。
- 引擎：`Engine<N, T>`（容量 N 笔订单、每笔 T 个经济事件槽），主要方法：`new`、`update_gate`、`freeze/frozen`、`available/held/spent`、`reserve`、`durable_record`、`accept_commit`、`begin_send`、`abort_unsent`、`transport_rejected_without_bytes`、`uncertain`、`not_found`、`ack`、`request_cancel`、`cancel_rejected`、`terminal`、`fill`、`fee`、`recover_intent`。
- 记录：`DurableRecord`、`CommitReceipt`、`Fill`、`Order<T>`。
- `pub mod ports`：trait `Journal`（`commit`/`committed`/`append_fact`/`facts`）、`Exchange`（`submit -> SubmitOutcome`）、`ConfigStore`（`load`/`compare_exchange`）、`ObjectStore`（`put_immutable`/`get`）；`ServiceError`、`SubmitOutcome`（PendingResponse/NoBytesAccepted/MayHaveSent/Acknowledged）、`ExecutionFact`（AbortedBeforeSend/Fill/Fee/Terminal，带 `apply`）。

## 3. workspace 位置

- 上游依赖：**无任何 workspace crate 依赖**（Cargo.toml 无 `[dependencies]`），也无第三方依赖；是纯计算内核。
- 下游（grep `Cargo.toml` 核对）：`tickwire-business-journal`、`tickwire-gateway`、`tickwire-mock`。

## 4. 对应规格

- [../../specs/implementation/execution-core.md](../../specs/implementation/execution-core.md) — 执行内核与 MockService 主规格。
- [../../specs/implementation/mock-abort.md](../../specs/implementation/mock-abort.md) — 未发送中止（AbortedBeforeSend）与顺序恢复。
- [../../specs/implementation/command-binding.md](../../specs/implementation/command-binding.md) — `command_digest` 必填绑定与全零拒绝。

## 5. 构建与测试

```sh
cargo test -p tickwire-execution
```

- 测试：`tests/lifecycle.rs`（18 个 `#[test]`，覆盖预留/提交/发送/ACK 丢失/未知冻结/费用尾款/重复成交/恢复重放）。
- TLA+ 有限模型（与 Rust 转录对照）：`tla/execution_models/submission/L0_commit_gate`（提交门）、`L1_crash_receipt`（崩溃回执）、`L2_economic_facts`（经济事实重放），经 `scripts/check-tla.sh` 用固定校验和的 TLC jar 运行（扫描 `tla/execution_models/**/02_models/*.cfg`）。模型说明见各目录 `00_input.md`/`01_reasoning.md` 与 `tla/execution_models/submission/PLAN.md`。

## 6. 关键不变量与注意事项

- **先持久化后发送**：`begin_send` 要求 `durable == true`（否则 `NotDurable`），且发送前再次检查 `frozen` 与 `gate.permits`；`MayHaveSent` 在调用任何 adapter/TLS 缓冲**之前**置位（`begin_send` 注释："Happens BEFORE calling any adapter/TLS buffer: loss of its callback stays Unknown"），回调丢失即未知，绝不自动重试。
- **门控九项全等**：`Gate::permits` 要求 enabled、market_valid、account_valid、recovered、owner_isolated 全为真，`now < lease_until`、`now < deadline`，且 epoch/strategy_generation/config_revision 与请求逐项相等；任一不满足 `reserve`/`begin_send` 返回 `GateClosed`。
- **预算校验（checked_budget）**：quantity>0、`command_digest` 非全零、unit_budget>0、fee_budget>=0、intent!=0、recovery_object!=0；`quantity*unit_budget + fee_budget` 用 checked_mul/checked_add，溢出 `Overflow`。
- **费用尾款占用**：`Order::held()` = 剩余数量×unit_budget +（未释放时 `fee_budget - fees_paid` 的非负部分）；`abort_unsent` 与可信无字节拒绝才置 `fee_released=true` 释放费用尾款，不得在其他路径释放。
- **未知即冻结**：`uncertain`/`not_found` 直接 `freeze()`；`fill`/`fee`/`terminal` 转换失败一律 `freeze()`；`replace()` 中 `spent+held > capital` 也冻结共享池（"Keep even a deficit representable so available() cannot overflow"）。冻结后 `reserve`/`begin_send` 拒绝新单。
- **恢复屏障**：`recover_intent` 把订单置为 `durable=true`、`send=MayHaveSent` 并**强制 frozen=true**；恢复只重放，绝不恢复发送权限（注释："recovery never submits"）。
- **事件去重**：`record_event` 按 Fill 的 `trade_id`、Fee 的 `id` 去重；完全相同重复返回 `Ok(false)`，同 id 不同内容返回 `ConflictingEvent` 并冻结；事件槽满 `EventCapacity`。
- **无字节中止的可信来源**：只有可信传输 adapter 断言"未接受任何可输出字节"（`transport_rejected_without_bytes` / `ExecutionFact::AbortedBeforeSend`），普通超时/网络错误不得走此路径。
- **SyncReplica 不可降级**：`accept_commit` 中 profile 与回执不匹配、`SyncReplica` 缺 `replica_durable` 均 `ReceiptMismatch`/`NotDurable`。

## 7. 改动检查清单

1. `cargo test -p tickwire-execution` 全绿。
2. 改动状态机转换（reserve/commit/begin_send/ack/uncertain/fill/fee/terminal/recover_intent）时，同步转录对应 TLA 层级（L0 提交门 / L1 崩溃回执 / L2 经济事实），跑 `./scripts/check-tla.sh`。
3. 改动 `command_digest` 绑定或请求相等语义时，联动 `../../specs/implementation/command-binding.md` 与 `tickwire-journal-codec` 的 schema 2 键 14。
4. 新增转换必须保持无 I/O、无分配、checked 算术；任何"未知"结果路径必须落到冻结而不是重试。
5. 改 `ports` trait 签名后，检查 `tickwire-mock`（MockJournal/MockExchange/MockSql/MockKv）与 `tickwire-gateway`、`tickwire-business-journal` 的实现同步。
