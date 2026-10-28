# tickwire-runtime 接手说明

## 1. 职责（架构位置：运行时托管 / 本地恢复 authority）

显式驱动的交易节点与本地恢复 authority，三个模块：

- `authority`：`GateProvider`/`CommitPolicy` trait、`LocalAuthority`（实现 account-gateway 的 `Authority`：prepare 装配恢复对象并 `publish`、`verify` 重核闭包、`current_gate` 委托策略）、`RecoveryQueue`（暂存 `RecoveryBundle`）、`verify_handover_objects`（历史策略归属与账户前缀校验，本身不授予实时交易权）。
- `node`：`CoreNode` 显式 reactor（公开行情、私有流、REST owner、共享会话、策略、输出槽），`step() -> Report` 单步驱动、`run`/`run_with_maintenance` 循环；`Health`、`Submission`、`HandoverReadiness`、`InstalledReplay`/`PreparedReplay`（release 重建/安装证据校验）、`Strategy`/`AccountHistory`/`Observer` trait、`Bindings`/`Config`/`Report`。
- `files`：真实文件后端装配（`Configuration`/`Networks`/`Directories`/`OpenMode`/`Services`/`FixedDeliveryBudget`），`open`/`open_with_services` 构造 `FileNode`；`pub use tickwire_market_source as market` 直接转引市场源抽象。

架构位置：把账户网关、Binance 网络客户端、日志、恢复闭包装配成一个可单步驱动的节点；不自己签发交易权利，授权全部来自注入的 `GateProvider`/策略。

## 2. 公共 API 入口

- `authority`：trait `GateProvider { current(command, admission) -> Gate }`、`CommitPolicy { state(admission, receipt) -> CommitState }`；`Dependency`、`RecoveryBundle`、`RecoveryQueue`（`new/stage/remove`）、`LocalAuthority::new/with_commit_policy`、`verify_handover_objects`。
- `node`：`Health{calibrated, market_live, private_receiving, account_frozen, sender_halted}`（`ready()`）、`Submission`、`HandoverReadiness`、`InstalledReplay`/`PreparedReplay`、`RebuildInput`/`InstalledRebuild`/`PreparedRebuild`、`verify_rebuild_ready`、`Strategy` trait（`handover_ready/handover_objects/handover_checkpoint/handover_replay_source/handover_install_replay/handover_rebuild_input/handover_install_rebuild/handover_finish_rebuild`）、`AccountHistory`、`Observer`、`NoObserver`/`NoPublisher`、`CoreNode`（`new/account/set_market_source/strategy/rest/session/local_time/private_status/private_gap/connection_replaced/replay_pending/step/stop/run/run_with_maintenance`）。
- `files`：`Configuration`、`Networks`、`Directories`、`OpenMode{Create, RecoverFrozen{..}}`、`Services`、`SharedRecoveryStore`、`FileNode` 类型别名、`open`/`open_with_services`。

## 3. workspace 位置

- 上游依赖（Cargo.toml）：`tickwire-handover`、`tickwire-release`、`tickwire-account-codec`、`tickwire-account`、`tickwire-account-gateway`、`tickwire-account-journal`、`tickwire-binance`、`tickwire-binance-account`、`tickwire-binance-clock`、`tickwire-binance-execution`、`tickwire-binance-private`、`tickwire-binance-public`、`tickwire-binance-quota`、`tickwire-binance-session`、`tickwire-book`、`tickwire-clock`、`tickwire-journal`、`tickwire-market-source`、`tickwire-recovery`、`tickwire-transport`、`tickwire-venue`；dev-dependency `tickwire-risk`。
- 下游（grep `Cargo.toml` 核对）：`tickwire-app`、`tickwire-runtime-policy`、`tickwire-strategy-host`、`tickwire-strategy-sdk`。

## 4. 对应规格

**无专门实现文档。** 本 crate 是装配/托管层，规格分散在被装配组件中：账户准入见 [../../specs/implementation/account-gateway.md](../../specs/implementation/account-gateway.md)，恢复闭包见 [../../specs/implementation/recovery-closure.md](../../specs/implementation/recovery-closure.md)，策略进程交接证据链见 [../../specs/implementation/strategy-handover.md](../../specs/implementation/strategy-handover.md)（场景由 `tickwire-lab` 集成），对账控制器属于 `tickwire-app` 的 [../../specs/implementation/reconciliation-controller.md](../../specs/implementation/reconciliation-controller.md)。改动前先确认职责落在哪个被装配组件，避免在本层复制业务规则。

## 5. 构建与测试

```sh
cargo test -p tickwire-runtime
```

- `tests/handover_closure.rs`：4 个测试，经 `verify_handover_objects` 校验历史策略归属、固定 S checkpoint 与完整输入闭包（复用 `tickwire-account-codec/tests/common` 夹具）。
- 本 crate 无 bin、无 example；真实节点装配由 `tickwire-app` 完成。

## 6. 关键不变量与注意事项

- **不提供 allow-all 默认门禁**：`GateProvider` 由调用方注入，runtime 自身不补默认放行；`LocalAuthority` 的 `executor_id` 为全零时 `prepare` 直接 `Denied`。
- **prepare 装配固定 kind 集合**：kind 1 准入记录、2/3 由账户 owner 用实际 Admit/ preceding 历史重写（不用 worker 占位）、12 输入、13 完整命令、14 基线、15 历史前缀；对象 edges 为 (0, n)；随后 `Persistence::publish` 成功才返回 `VerifiedClosure`。
- **verify 只重核不重建授权**：`verify` 走 `Persistence::verify` 重读绑定并重检同步，不恢复发送权限。
- **Health 全部满足才 ready**：calibrated && market_live && private_receiving && !account_frozen && !sender_halted；`step` 每轮回报这些标志，调用方据此决定是否放行提交。
- **显式单步 reactor**：`step` 驱动四个网络 owner 与策略，I/O 预算在四个 owner 间均分；`maximum_idle_wait_ns=0` 是忙轮询，正值才限制 reactor sleep。
- **重建证据二次校验**：`InstalledReplay::from_authenticated_child` 先要求子进程 42/37 已认证，再独立核对 release root、install 校验、target 实例/代次与 HandoverSeal 摘要；C 态回滚探针仍须单独成功，本对象不是完整 release preflight。
- **Strategy 不接收可变账户状态**：trait 只给只读回调/checkpoint/已认证重放证据，适配器不得合成缺失证据。
- **schema4 发布阶段**：`RebuildInput`、`InstalledRebuild`、`PreparedRebuild` 通过三个独立 Strategy 方法交付；Host 先认证真实 Capture PID、报告游标及独立 probe 退出，runtime 构造器再核对内容闭包、预算与 v4 READY。当前 READY 必须沿安装检查点的完整 retained 输入前缀继续；最终 Prepared 和账户恢复同时要求固定安装根及回退完成根。内容对象不授予交易权，默认适配器均返回不支持。
- `OpenMode::RecoverFrozen` 要求按给定物理 frontier 恢复，缺 frontier 不得启动为可交易状态。

## 7. 改动检查清单

1. `cargo test -p tickwire-runtime` 全绿。
2. 业务规则（预留、对账、额度、签名）改动落在对应底层 crate，本层只改装配与接线。
3. 改 `LocalAuthority::prepare` 的 kind 装配时，联动 `tickwire-recovery` 的必备集合校验与 account-recovery-binding.md。
4. 改 `CoreNode` 轮询/预算/恢复路径时，核对 node.rs 的 Config 注释（四 owner 均分 I/O、空闲上界）。
5. 新增 Strategy/Observer 适配器必须保持只读、可认证，不合成 handover 证据；检查下游 `tickwire-strategy-host`/`tickwire-app` 接线。
