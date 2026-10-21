# AGENTS.md — tickwire-binance-account

## 职责
持久化私有事件输入与订单身份注册的账户侧 owner：在输入 WAL（`tickwire-journal`）上保存认证原文与规范决定，把私有 WS 事件/REST 响应绑定到本地创建命令，维护 WS 交易会话、REST 单发 owner，以及只读对账（成交页/账户快照/挂单/条件单/收入/结构/修改历史）。它是命令从 admit 到账户记账之间的持久化与会话责任层，不直接产出市场行情。

## 公共 API 入口
- `AccountIngress<S: Store>`（`lib.rs`）：`create(store, Config)`、`recover(store, Config, required: Option<&PhysicalFrontier>)`、`register_command(command, ids)`、`registered(intent) -> Option<&CanonicalCommand>`、`position()/applied_position()/input_stream()/stopped()/quarantined()`、`stored_events() -> impl Iterator<Item=StoredEvent>`、`account_inputs()`、`close_for_recovery()`、`export_durable(...)`。
- 决定类型：`Outcome::{Applied{input, applied}, Quarantined{input, reason}}`、`QuarantineReason::{UnknownOrder, AmbiguousOrder, UnadmittedOrder, BindingConflict, AccountRejected, AccountReconciliation}`、`StoredDecision::{Bound, AccountBound, Quarantine}`、`StoredEvent<'a>`、`InputKey { stream, sequence }`。
- `rest::RestOwner<S: Store, B: Backend, A: Authority, E: Encoder>`：`from_parts(ingress, gateway, config)`、`submit(command, order_ids, evaluated_at, deadline)`、`poll(now, received_mono_ns, budget) -> Option<RestOutcome>`、`ingest_private(event)`、`source_gap(gap)`、`replay_pending(maximum)`、`register/reregister`、`account()/status(key)/busy()/submission_ready()/latest_applied_input()/applied_position()`；`RestConfig`、`RestDisposition::{Observed, NeedsReconciliation, Rejected, Unknown, NoBytes}`、`RestOutcome`、`StoredRest<'a>`。
- `ws` 模块：`SessionIdentity`、`DurableWsSession`、`WsRequestPlan`、`SignedWire`、`DurableWsBinding`、`BindingEvidence`、`WsRequestId`；再导出 `ControlAction::{Connect, Upgrade, Ping, Pong, Close}`、trait `SessionState`、`WebSocketSettings`、`WsConfig`、`ResponseDecision/StoredWsResponse/WsResponseOutcome`。
- `reconciliation` 模块：`ReconcileConfig`、`TradeQuery`、`ReadOperation::{Connect, TradePage}`、trait `ReadPermit`、`ReconcilePage`；只读 reader `IncomeReader`、`AccountSnapshotReader`、`ReadOnlyOrderReader`、`ReadOnlyAmendmentReader`、`ReadOnlyStructureReader`（各自 `new(ReconcileConfig)` + 泛型 `poll<S, B, A, E>`）；子模块 `activation/amendments/bootstrap/income/income_association/income_closure/income_import/orders/structures/structure_closure`。
- `account_facts::bind_observation(baseline, session, body, event_ns, transaction_ns) -> CanonicalAccountInput`（稀疏 USDM 账户观察绑定，不授予授权）。
- `Config { income_baseline_authority, income_association_authority, physical: journal::Config, writer_epoch, baseline: CanonicalBaseline, account_stream, max_records, max_registrations, max_raw_bytes, max_retained_bytes }`；`MAX_RECORD_BYTES = 60 KiB`；`Error`。

无 binary、无 example。

## 在 workspace 中的位置
- 上游依赖（`[dependencies]` 中的 tickwire-*）：`tickwire-risk`、`tickwire-recovery`、`tickwire-account`、`tickwire-account-codec`、`tickwire-account-gateway`、`tickwire-account-journal`、`tickwire-binance`、`tickwire-binance-private`、`tickwire-journal`、`tickwire-venue`、`tickwire-transport`、`tickwire-clock`；外部 `ring`、`base64`、`serde`、`serde_json`。
- 下游（已在各 `Cargo.toml` 核对）：`tickwire-app`、`tickwire-binance-execution`、`tickwire-binance-quota`、`tickwire-replica`、`tickwire-runtime-policy`、`tickwire-runtime`、`tickwire-strategy-host`、`tickwire-strategy-sdk`。

## 对应规格
- ../../specs/implementation/binance-ws-trading.md
- ../../specs/implementation/spot-amend.md
- 相关补充：../../specs/implementation/usdm-account-facts.md（收入/账户观察、IncomeReader 与基线审批实现位于本 crate 的 reconciliation/income*）

## 构建与测试
```sh
cargo test -p tickwire-binance-account
cargo build -p tickwire-binance-account
cargo clippy -p tickwire-binance-account --all-targets -- -D warnings
cargo fmt -p tickwire-binance-account
```
`tests/`（本地 mock TLS/账本，不访问真实网络，无 OS 条件编译）：`account_observation.rs`、`network_ingress.rs`、`persistence.rs`。

## 关键不变量与注意事项
- **先同步输入再验证账户**：WAL 先持久化认证原文与决定，再在候选账户上验证并同步账户记录，最后发布状态；恢复逐条重验原文与规范正文，失败不把未同步候选发布为现有账本（出处：`recover` 重放逻辑、`usdm-account-facts.md`）。
- **配置校验**：`physical.domain == baseline.snapshot().scope.risk_domain_id`；`account_stream` 非零且不等于 stream_epoch；`writer_epoch` 非零且 < 2^63；`max_records` 2–65536；`max_registrations` ≤ `tickwire-account::MAX_ORDERS`；`max_raw_bytes` ≤ 32 KiB；`max_retained_bytes` 60 KiB–256 MiB；产品仅允许 Spot/LinearFuture/LinearPerpetual（出处：`validate_config`）。
- **注册幂等/冲突即停**：`register_command` 同 IntentKey 同命令同 ids 幂等返回；字节不同或 ids 不同则 `Conflict` 并把 `stopped` 置位；`cursor != records.len()` 时拒绝新注册（`ReplayPending`）。
- **WS 交易绑定**：首次应用输出需实际 `DurableWsBinding` 与缓存帧摘要相等，且实际账户收据与绑定相等；新连接先同步会话代次，旧 owner Drop/撤销使旧资格失效，恢复不重新签发资格（出处：`binance-ws-trading.md`、`ws/owner.rs`）。
- **Spot 修改（amend）门禁**：仅原普通 Spot LIMIT/GTC、非冰山、无结构腿；新总量大于已记成交且严格小于有效总量，client ID 显式沿用；持久 Aborted 只结束本次修改不中止原订单（出处：`spot-amend.md`、`rest/amendment`）。
- **稀疏观察语义**：`bc` 不生成现金流；缺失资产/仓位保持未报告，未解释观察冻结；收入以 `(incomeType, tranId)` 去重，同键正文不同即冲突（出处：`account_facts.rs`、`usdm-account-facts.md`）。
- 本 crate 不持有市场行情；私有事件来自 `tickwire-binance-private`，编码标签/字节契约在 `tickwire-account-codec`。

## 改动检查清单
```sh
cargo test -p tickwire-binance-account
cargo clippy -p tickwire-binance-account --all-targets -- -D warnings
cargo fmt -p tickwire-binance-account
```
改 WAL 记录/恢复跑 `persistence.rs`；改私有事件入账跑 `network_ingress.rs`；改账户观察绑定跑 `account_observation.rs`。WS 交易契约见 `binance-ws-trading.md`，amend 契约见 `spot-amend.md`。
