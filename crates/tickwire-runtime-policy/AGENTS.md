# tickwire-runtime-policy 接手说明

## 1. 职责（架构位置：运行时控制门禁）

显式 supervisor 授权、实时行情依赖与对账私有状态的控制平面：`ControlPlane` 在单 Rc/RefCell owner 内维护 owner 租约、策略授权、管理授权、行情喂料新鲜度、模式（Trading/Stopped）与私有流对账标记；`PolicyGate` 实现 `tickwire-account-gateway::GateProvider`，在每条命令准入前做一次完整门控评估。`bootstrap` 模块提供可序列化部署输入 `Config` 与原生 provider 构造；`lifecycle` 模块提供单次交接（begin/committed/release）门禁与签名收据。

架构位置：`tickwire-runtime::LocalAuthority.current_gate` 的策略后端；账户网关准入、binance 执行、策略宿主共用同一套授权判定。**所有控制变更与当前门禁共享一个 Rc/RefCell owner，不可跨线程共享**（runtime-policy.md 首段）。

## 2. 公共 API 入口

- 错误与模式：`Error`（Denied/Clock/Busy/Identity/Capacity/Unreconciled/Conflict 等）、`Mode`（Trading/Stopped）。
- 授权结构：`ScopeRef`、`Versions`（config_revision/risk_policy_version/risk_domain_version 等绑定版本）、`StrategyGrant`（logical_strategy_id/instance_id/strategy_generation/authorization_epoch/config_revision/valid_until_ns/actions）、`ManagementGrant`（cancel/query 行政动作）、`OwnerLease`（executor_id/owner_generation/writer_epoch/lease_epoch/domain/valid_until_ns/isolation_record）。
- `ControlPlane`：`new`、`set_mode`（带 control_revision 单调校验）、`install_owner_lease`、`replace_writer`、`install_strategy_grant`、`install_management_grant`、`feed_*`（行情喂料安装/标记）、`reconcile`/`complete_reconciliation`、`provider(clock) -> PolicyGate`。
- `PolicyGate<C: PolicyClock>` 实现 `GateProvider`；`policy_clock::PolicyClock` trait（`sample()`）。
- `bootstrap`：`Environment`、`Product`、`Domain`（process_instance+generation→ClockDomain）、`Scope`、`Lease`、`Config`（`build()`/`domain()`/`scope()`/`product()`/`native_provider()`，后者用 `tickwire-clock-sys::SystemClock`）。
- `lifecycle`：`HandoverBinding`、`HandoverGate`、`begin_handover`/`handover_is_current`/`handover_is_released`/`handover_committed`/`release_handover`。

## 3. workspace 位置

- 上游依赖（Cargo.toml 实测）：`tickwire-account`、`tickwire-account-gateway`、`tickwire-binance-account`、`tickwire-binance-clock`、`tickwire-binance-private`、`tickwire-clock`、`tickwire-clock-sys`、`tickwire-runtime`、`tickwire-venue`；serde；dev-dependency `tickwire-risk`。本 crate 不依赖 fencing/recovery/ipc。
- 下游（grep `Cargo.toml` 核对）：`tickwire-app`、`tickwire-lab`、`tickwire-strategy-host`。

## 4. 对应规格

- [../../specs/implementation/runtime-policy.md](../../specs/implementation/runtime-policy.md) — 运行时控制门禁规格（策略授权/行情依赖/租约/GapToken；单 owner Rc/RefCell，故障永久关闭）。

## 5. 构建与测试

```sh
cargo test -p tickwire-runtime-policy
```

- 测试内置于 `src/tests.rs`（经 lib.rs 的 `#[cfg(test)] mod tests;` 接入，13 个 `#[test]`）：覆盖租约过期/换 writer、grant 实例/代次/epoch/config_revision 绑定、管理授权仅限 Cancel/Query、Stopped 拒绝、对账未完成拒绝、交接未 released/私钥吊销不符拒绝、时钟 domain/回退 fault。

## 6. 关键不变量与注意事项

- **风险版本与准入绑定**：evaluate 逐项核对 `admission.command_digest() == command.digest()`、intent epoch/id、scope（risk_domain_id/venue/account/environment）、product、`writer_epoch`、`config_revision`，并核对 grant 的 `risk_policy_version`/`order_config_version` 与命令的 `catalog_version`/`catalog_hash` 等于 `Versions`（install 时 `versions_valid` 要求全部非零、catalog_hash 非全零）；任一不符 `Denied`。
- **私有流对账过滤**：非行政动作要求 `private.reconciled` 存在且 `connected`、connection_epoch 匹配、gap_generation 匹配、account_epoch 匹配、`ledger_sequence <= admission.ledger_sequence()`、未过 `valid_until`，否则 `Unreconciled`；`check_reconciliation` 还校验 scope/product/input_stream/writer_epoch/through_sequence/baseline/history/query_coverage 摘要。
- **时钟失败即永久 fault**：`PolicyGate.current` 中 `clock.sample()` 失败、`now.domain() != config.domain`、或 `now.nanos() < last_mono`（回退）都置 `faulted=true` 并返回 `Denied`，之后不再放行（lib.rs evaluate 段）。
- **租约与 grant 双重截止**：owner `valid_until_ns`、策略 grant `valid_until_ns` 都以 `now < valid_until_ns` 校验，Gate 取两者更早者；deadline 已过、`now < strategy.created_mono_ns`、`Mode::Stopped` 均拒绝。
- **管理授权窄化**：management grant 只放行 Cancel/Query 行政动作，且必须命中已登记的 logical_strategy_id；交易性动作只能走 strategy grant。
- **对账完成才激活**：`complete_reconciliation` 之前私有流视为未对账，依赖它的动作返回 `Unreconciled`；任何 writer 替换/grant 变更都会把 `private.reconciled` 重置为 None（多处 `p.reconciled = None`）。
- **行情喂料新鲜度**：非行政、非降险 Amend 的动作要求 `Mode::Trading` 且对应 feed 在窗口内新鲜；`feed` 缺新标记直接拒绝。
- **交接门禁**：`begin_handover` 记录 binding（instance/generation/私钥吊销）；未 released、`private_revocation` 不符、binding.instance/generation 不符的交接拒绝；`release_handover` 只能在 `handover_committed` 真实回执之后。
- **单线程 owner**：Rc/RefCell 不可 Send/Sync；并发场景由调用方单线程驱动，不要引入 Mutex 包装。

## 7. 改动检查清单

1. `cargo test -p tickwire-runtime-policy` 全绿。
2. 改 evaluate 的字段绑定清单时，对照 runtime-policy.md 与 `Versions` 版本表，逐字段补/删断言并加测试。
3. 新增授权类型（grant/lease/management）必须实现截止校验与 conflict 拒绝，不得放行无截止授权。
4. 改动会重置对账的状态迁移时，确认 `complete_reconciliation` 的重新激活路径有对应测试。
5. 保持 Rc/RefCell 单 owner；跨线程需求由上层接线，不要在本层加锁。
