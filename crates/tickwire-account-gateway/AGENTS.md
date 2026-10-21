# tickwire-account-gateway

## 1. 职责（架构位置：账户风险域 —— 账户发送协调与准入门禁）

一个实例串行持有一个账户、一条有序账户日志、一个协议编码器与一条连接；负责恢复依赖绑定、准入同步、首次输出前的新鲜授权门禁、排队、中止与对账恢复。Spot 与 USD-M 分别构造实例。lib 根文档：`One account owner, one ordered journal, and fresh authorization at first output.`

## 2. 公共 API 入口

- `AccountGateway<S: Backend, A: Authority, E: Encoder>`：`create`、`recover`、`recover_with_reactivation`、`submit(command, evaluated_at, deadline) -> Status`、`poll(now, budget)`、`cancel_pending()`、`terminate_unsent_strategy(strategy, manager) -> bool`、`record_ingress_event`、`record_account_input`、`close_uncreated_algo`、`reconcile_account_inputs`、`associate_income_observations`、`install_income_baseline`、`ingress_account_inputs/ingress_event/ingress_events`、`command(intent)`、`baseline()`、`admission_origin(...)`、`history_frontier()`、`journal_config()`、`freeze_account()`、`writer_epoch()`、`reactivate(binding, connection, verifier) -> ActivationReceipt`、`record_event/record_fact`、`commit_strategy_handover`、`status(intent)`、`committed(intent)`、`account()`、`journal_records_after(...)`、`journal_position()`、`export_durable`、`halted()`、`read(out)`、`output_progress(intent)`、`stop_sending()`、`register/reregister`。
- 策略接口：`Authority`（`prepare/verify/commit_state/current_gate/verify_handover`，无宽松默认）、`ReactivationVerifier`（`verify` 及各收入/未创建/输入闭包的 live/普通核对，默认 `Denied`）、`AdmissionRequest{baseline,record,preceding,binding}`、`Gate{now,lease_until,writer_epoch,strategy_generation,config_revision,owner_isolated,action_permitted,market_valid}`、`Config{journal,writer_epoch,route,max_intents}`、`Status::{AwaitingCommit,Queued,PartiallyAccepted,Flushed,Unknown,Aborted}`、`CommitState::{Pending,Ready}`、`CommittedAdmission{admission,receipt,record_digest}`、`OutputProgress{accepted_bytes,total_bytes}`、`ActivationReceipt{ledger_sequence,history_digest}`、`AuthorityError::{Denied,Unavailable,Recovery}`。
- `pub use tickwire_gateway::{ConnectionConfig, EncodedRequest, Encoder, Route}`；`pub mod websocket`（`WebSocketControl`、`begin_websocket/websocket_ready/websocket_control_pending/queue_websocket_control/poll_websocket`）。
- 私有模块：`binding`（命令/请求/闭包/门禁绑定校验）、`recovery`（实现 `account_journal::RecoveryVerifier`）。无 bin/example。

## 3. workspace 位置

上游（`Cargo.toml` 实际声明）：`tickwire-account`、`tickwire-account-codec`、`tickwire-account-journal`、`tickwire-gateway`、`tickwire-journal`、`tickwire-recovery`、`tickwire-transport`、`tickwire-venue`；dev：`tickwire-binance`、`tickwire-risk`、`rcgen`、`ring`。

下游（grep 命中）：`tickwire-runtime`、`tickwire-runtime-policy`、`tickwire-strategy-host`、`tickwire-strategy-sdk`、`tickwire-binance-account`、`tickwire-binance-execution`、`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/account-gateway.md](../../specs/implementation/account-gateway.md)（13 条：精确重试、先编码后持久、候选准入、依赖绑定、同步后发布、首次输出门禁、零接受中止、正数接受保留责任、事实先同步、冻结不停健康撤单查询、恢复重放冻结、Reactivated 后才提交新意图）
- [../../specs/implementation/account-recovery-binding.md](../../specs/implementation/account-recovery-binding.md)（恢复依赖绑定与精确前缀核对）

## 5. 构建与测试

```sh
cargo build -p tickwire-account-gateway
cargo test -p tickwire-account-gateway
```

无 bin/example。`tests/`：`submission.rs`、`lifecycle.rs`、`ingress.rs`、`income_closure.rs`、`persistence_failures.rs`（追加/同步失败路径）、`recovery_metadata.rs`、`binance_rest.rs`、`websocket.rs`；公共故障后端在 `tests/common/fault_backend.rs`。

## 6. 关键不变量与注意事项

- **精确重试先于一切检查**：同意图同字节命令直接返回已有 `Status`，不重复编码/授权/预留/写日志；同意图不同字节 → `halt()` + `Conflict`（`submit`）。
- **先授权/恢复依赖，再同步意图**：`submit` 先在候选账户 `admit`，构造 `CanonicalRecord`，`authority.prepare` 得到 `VerifiedClosure` 并 `binding::closure` 核对，然后才 `journal.append`；依赖发布失败不写 Admit、不排队。
- **追加失败保留候选预留并停止**：`journal.append` 出错时保留 `candidate`（可能完整尾已存在）并 `halt()`，不能沿未确认结果继续。
- **commit_state 失败降级为 Pending**：本地证据已存在时保留身份/预留，走正常无输出路径，不释放预留。
- **首次输出前新鲜门禁**：`poll` 内 `try_send` 回调里同步采样 `current_gate` 并 `binding::permits` + `account.check_admission`；时间不得早于原准入计算；增险另查行情/账户有效。
- **零接受中止路径**：只有与 pending token 精确匹配、`accepted_bytes==0` 的 `NoBytes` 报告，或明确无应用字节副作用的 queue 失败，才进入 `abort`；`drive` 错误本身不是零接受证据。中止先候选预校验（已有确认/成交/矛盾则失败且不追加 Aborted），成功先同步 `Aborted` 记录再发布候选账户与状态，失败不提前释放。
- **正数接受保留责任**：`MayHaveSent → PartiallyAccepted`；部分接受后取消/失败 → `Unknown` + 冻结 + 关连接；`Flushed` 只是本机缓冲冲刷，不等于交易所确认。
- **冻结后仍允许授权内撤单/查询**：`freeze_account()` 只冻结账户；健康连接的撤单/查询不停，只有网络/WAL 不可用才停止协调器。
- **reactivate 同步后才解冻发布**：先核对基线/历史/scope/ledger，`verifier.verify`，候选 `apply_reactivation`，同步 `Reactivated` 记录，再换连接；旧意图仍返回原状态不重发。
- `halt()` 冻结账户、排空保留接收、关连接、把非 Aborted 意图置 `Unknown`。

## 7. 改动检查清单

1. `cargo build -p tickwire-account-gateway && cargo test -p tickwire-account-gateway` 通过。
2. 发送/中止/恢复联动 [account-gateway.md](../../specs/implementation/account-gateway.md)；依赖绑定联动 [account-recovery-binding.md](../../specs/implementation/account-recovery-binding.md)。
3. 门禁字段（lease/writer/代次/revision/行情）改动同步 `tickwire-runtime-policy` 与 `tickwire-gateway`。
4. 恢复路径改动核对 `tla/execution_models/account_gateway/`（L0_guarded_account_output）。
5. 本 crate 不自行签发授权，授权永远来自注入的 `Authority`。
