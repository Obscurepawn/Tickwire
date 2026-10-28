# AGENTS.md — tickwire-app

## 职责

总装层：文件配置装配的实际运行时（`[[bin]]` `src/main.rs`）。把基线账户、校准时钟、共享额度、公共行情、私有会话、账户输入 WAL、签名 REST、恢复对账、复制、故障接管/恢复、生命周期与策略装配进 `tickwire-runtime` 节点并驱动主循环。它是唯一面向部署的可执行进程，本身不实现业务算法，只负责接线、顺序与不变量。

## 公共 API 入口

### 模块（库层，供装配与测试引用）

- `config`：`Deployment { baseline, directories, recover, product, boot_id, clock_generation, owner_id, writer_epoch, initial_writer_epoch, account_stream, input_stream, first_connection_epoch, trading_websocket, instrument_id, book_source, credential, network, clock, tuning, policy, quota, strategy, market_sources, replication, lifecycle, shadow_loader, reconciliation, income_*, control, stop_file, maximum_runtime_ns }`；`ProductName::{Spot, Usdm, UsdmFuture}`（`market()/product()`）；`CredentialConfig/CredentialMaterial/KeyAlgorithm::{Hmac, RsaPkcs8, Ed25519Pkcs8}`；`Network::resolve(connect_ns)`；`ClockConfig`；`Tuning`；`Deployment::read/baseline/domain/credentials/authentication/assemble`；常量 `PUBLIC_BOOK_LEVELS=5000`、`PUBLIC_DEPTH_UPDATES=5000`、`PUBLIC_BUFFERED_EVENTS=64`。
- `control`：`ControlInbox::open(config, policy, quota, plane, clock)`、`poll(endpoint)`、`submit(command, endpoint) -> Applied`；`Config/Binding/Action/Command/Applied`。
- `failover`：`SealConfig::read`、`RestoreConfig::read`、`validate_start(&Deployment) -> Option<(Option<PhysicalFrontier>, PhysicalFrontier)>`、`seal(&SealConfig) -> Value`、`restore(&RestoreConfig) -> Value`、`FenceAuthority`、`RestoreMode`。
- `replication`：`Profile::{LocalDurableAsync{maximum_unreplicated_age_ns}, SyncReplica{mirror_id, assignment_epoch}}`、`Config { stream_file, mirror, mirror_server_name, tls, signing_key_pkcs8, mirror_signing_public_key, profile_version, profile, … }`、`Driver::open/replicate/start/drive`。
- `reconciliation`：`Controller::new(...).with_execution_network(...)`、`drive<P,G,S,O,L,U,B>(node, registry)`；`Config { enabled, reader_id, first_request_epoch, first_execution_connection_epoch, retry_ns, valid_for_ns, fee_maintenance_interval_ns, income_baseline_import, income_account_scan, income_association }`；子模块 `association`、`income`。
- `lifecycle`：`Inbox::open(...)`、`poll(strategy, gateway, now)`；`lifecycle/replay`。
- `launch`：`build(strategy, settings, plane)`、`Config/Request`、`ShadowLoader::open/poll/next_sequence`。
- `market::Sources::connect(...)`（认证直连 MD 订阅；空则选本地公共 client）。
- `hosted::build(config, runtime, policy) -> Box<dyn AppStrategy>`（bootstrap 限定、shadow 开启 handover、远程外部市场分支）。
- `strategy`：`Config/MemberConfig/CommandFile/RecoveryFiles/DependencyFile/FileStrategy/MultiStrategy/new_modes/load(digest)`。
- `replay`：`run(&Deployment)`、`export(&Deployment, destination)`。
- `release_preflight`（`pub(super)` 限定：`qualify/qualify_startup/fixed_ready/unchanged/root/bind/requirements`）。

### 可执行入口（`src/main.rs`，默认 bin 名 `tickwire-app`）

```sh
tickwire-app inspect    --config /absolute/deployment.json
tickwire-app run         --config /absolute/deployment.json
tickwire-app replay      --config /absolute/deployment.json
tickwire-app export      --config /absolute/deployment.json --out /absolute/new-export
tickwire-app seal        --config /absolute/seal.json
tickwire-app restore     --config /absolute/restore.json
```

`run` 按 `ProductName` 分派 `BinanceSpot` / `BinanceUsdM` depth policy；主循环回调依次驱动 replication、control、shadow_loader、lifecycle、reconciliation，停止条件含 `stop_file`、策略失败、`maximum_runtime_ns`。

## 在 workspace 中的位置

上游 `[dependencies]`（近全栈，`Cargo.toml` 共 39 个 tickwire-*）：`tickwire-account`、`tickwire-account-codec`、`tickwire-account-gateway`、`tickwire-account-journal`、`tickwire-binance`、`tickwire-binance-account`、`tickwire-binance-clock`、`tickwire-binance-execution`、`tickwire-binance-private`、`tickwire-binance-public`、`tickwire-binance-quota`、`tickwire-binance-session`、`tickwire-book`、`tickwire-clock`、`tickwire-journal`、`tickwire-quota`、`tickwire-recovery`、`tickwire-runtime`、`tickwire-runtime-policy`、`tickwire-transport`、`tickwire-venue`、`tickwire-strategy-sdk`、`tickwire-release`、`tickwire-handover`、`tickwire-strategy-host`、`tickwire-market-publisher`、`tickwire-market-source`、`tickwire-replica`、`tickwire-lifecycle`、`tickwire-fencing`、`tickwire-supervisor`、`tickwire-control`、`tickwire-control-store`、`tickwire-join`、`tickwire-ipc`、`tickwire-shm`、`tickwire-snapshot`、`tickwire-risk` 等。

下游消费者（全仓 grep `Cargo.toml` 核对）：`tickwire-lab`。

## 对应规格

- [../../specs/implementation/reconciliation-controller.md](../../specs/implementation/reconciliation-controller.md)（订单/成交分页/费用闭合/三类远端快照、终态费用维护触发与限频、收入基线导入与关联、冻结规则）

## 构建与测试

```sh
cargo build -p tickwire-app
cargo test -p tickwire-app
cargo clippy -p tickwire-app --all-targets -- -D warnings
cargo fmt -p tickwire-app
```

`tests/failover_files.rs` 为纯文件/源封口恢复测试（无 OS 围栏，测试控制器的隔离断言是受信输入）。真实私有会话、网络、启动序列在 `./scripts/check-linux.sh` Docker 与集成环境验证。运行时用绝对路径配置；`stop_file` 必须绝对且启动时不存在。

## 关键不变量与注意事项

- **数据面逐单不过 raft**：正常下单/回报走私有会话与执行通道；控制面（`control::ControlInbox`、持久登记 `tickwire-control-store`）只承载控制命令与授权，不把交易数据送 Raft。
- **本地盘口槽**：`Deployment::assemble` 按规范基线完整目录顺序选取本地 `BookKey.instrument`，不窄化外部 `u64 instrument_id`；相同基线的恢复/重连保持槽位不变，venue/source 与外部 InstrumentBinding 保持原身份。槽不能跨 owner 或目录交换；见[归一化市场事件](../../specs/implementation/binance-market-events.md)。`config_instrument_tests.rs` 覆盖高位 ID、槽唯一性、同目录稳定性和 Spot/USD-M 实际 DepthSync 构造。
- **恢复对账顺序**（`run`）：`validate_start` 先核对 failover 目录标记/完成绑定/源目标身份/基线 bootstrap/封口/输入前缀 → `settings.mode = RecoverFrozen` → 打开节点后 `while node.replay_pending(1024)? != 0 {}` 先把持久输入回放完，**才接受新的私有会话输入**，随后才 `replication.start`（`main.rs` 268-273）。
- **reconciliation REST epoch 递增**：`first_execution_connection_epoch` 必须 `> first_connection_epoch`，构造时即拒绝（`main.rs` 215-217）；健康账户仅在 `terminal && !aborted && !fee_reconciled`、REST 空闲且达 `fee_maintenance_interval_ns`（缺省 1s，零拒绝）才启动费用维护。
- **failover 身份绑定**：`validate_start` 校验 started/complete 双标记格式（`tickwire.failover-in-progress.v1` / `tickwire.frozen-failover.v2`）、started_sha256、target binding、ledger_sequence 不小于基线、源 exec 代次/实例/boot 与目标不同、recovery seal 与 fence sha256 非零、完整前缀与输入前缀链一致（`failover.rs::validate_start`）。
- **复制模式固定**：`Profile` 随配置绑定，`LocalDurableAsync` 默认；`SyncReplica` 绑定 mirror_id + assignment_epoch，不在装配处改写提交模式。
- **控制连接与额度**：有 `control` 时走 `ControlInbox::open`（真实 quota provider）；无 control 时用 `MockQuotaStore` 构造 quota coordinator，二者互斥。
- **停止条件**：策略 `failed()`、`stop_file` 出现、超过 `maximum_runtime_ns`（0=不限）任一即停；策略失败向上返回错误并打印 `stopped` 事件。

## 改动检查清单

1. `cargo build -p tickwire-app && cargo test -p tickwire-app` 通过；启动/恢复/私有会话改动在 `./scripts/check-linux.sh` Docker 内验证。
2. 装配顺序（validate_start → RecoverFrozen → replay_pending → replication.start）改动必须同步 `reconciliation-controller.md` 与恢复测试。
3. 控制命令/票据改动联动 `tickwire-control`、`tickwire-control-store` 与 TLA `tla/execution_models/control_requests/`。
4. failover 标记/封口格式改动联动 `tickwire-replica`、`tickwire-handover`、`tickwire-fencing` 与恢复目录布局。
5. 本 crate 只装配与驱动，不在此处实现策略、对账算法或紧急撤单（紧急路径归 `tickwire-guardian`）。
