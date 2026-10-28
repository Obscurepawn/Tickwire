# AGENTS.md — tickwire-app

## 职责

总装层：文件配置装配的实际运行时（`[[bin]]` `src/main.rs`）。把基线账户、校准时钟、共享额度、公共行情、私有会话、账户输入 WAL、签名 REST、恢复对账、复制、故障接管/恢复、生命周期与策略装配进 `tickwire-runtime` 节点并驱动主循环。它是唯一面向部署的可执行进程，本身不实现业务算法，只负责接线、顺序与不变量。

## 公共 API 入口

### 模块（库层，供装配与测试引用）

- `config`：`Deployment { baseline, directories, recover, product, boot_id, clock_generation, owner_id, writer_epoch, initial_writer_epoch, account_stream, input_stream, first_connection_epoch, trading_websocket, instrument_id, book_source, credential, network, clock, tuning, policy, quota, strategy, market_sources, market_transition, replication, lifecycle, shadow_loader, reconciliation, income_*, control, observation, stop_file, maximum_runtime_ns }`；`ProductName::{Spot, Usdm, UsdmFuture}`（`market()/product()`）；`CredentialConfig/CredentialMaterial/KeyAlgorithm::{Hmac, RsaPkcs8, Ed25519Pkcs8}`；`Network::resolve(connect_ns)`；`ClockConfig`；`Tuning`；`Deployment::read/baseline/domain/credentials/authentication/assemble`；常量 `PUBLIC_BOOK_LEVELS=5000`、`PUBLIC_DEPTH_UPDATES=5000`、`PUBLIC_BUFFERED_EVENTS=64`。
- `control`：`ControlInbox::open(config, policy, quota, plane, clock)`、`poll(endpoint)`、`submit(command, endpoint) -> Applied`；`Config/Binding/Action/Command/Applied`；`configure_market(market::Config, Option<Sources>)`、`poll_market(endpoint, strategy)` 连接启动批准的扩容目录。
- `observation`：`ObservationInbox::open/poll/session/error`；`Config/Session/Request/Target/Outcome/Observation/Status`。启动批准的本机目录入口，独立持久请求和观察对象，冷路由实际 Host 的 begin/poll/cancel，不产生配置 Applied。
- `failover`：`SealConfig::read`、`RestoreConfig::read`、`validate_start(&Deployment) -> Option<(Option<PhysicalFrontier>, PhysicalFrontier)>`、`seal(&SealConfig) -> Value`、`restore(&RestoreConfig) -> Value`、`FenceAuthority`、`RestoreMode`。
- `replication`：`Profile::{LocalDurableAsync{maximum_unreplicated_age_ns}, SyncReplica{mirror_id, assignment_epoch}}`、`Config { stream_file, mirror, mirror_server_name, tls, signing_key_pkcs8, mirror_signing_public_key, profile_version, profile, … }`、`Driver::open/replicate/start/drive`。
- `reconciliation`：`Controller::new(...).with_execution_network(...)`、`drive<P,G,S,O,L,U,B>(node, registry)`；`Config { enabled, reader_id, first_request_epoch, first_execution_connection_epoch, retry_ns, valid_for_ns, fee_maintenance_interval_ns, income_baseline_import, income_account_scan, income_association }`；子模块 `association`、`income`。
- `lifecycle`：`Inbox::open(...)`、`poll(strategy, gateway, now)`；`lifecycle/replay`、`lifecycle/rebuild`（固定窗口、实际安装与回退结果的阶段 WAL 和账户前缀门禁）。
- `launch`：`build(strategy, settings, plane)`、`Config/Request`、`ShadowLoader::open/poll/next_sequence`。
- `market::Sources::connect(...)`（认证直连 MD 订阅；空则选本地公共 client）；同线程可克隆冷控制句柄提供 `consumers/validate_arm/arm/commit/receipt/release`，每个成员保留当前精确 ConnectConfig。
- `hosted::build(config, runtime, policy) -> Box<dyn AppStrategy>`（bootstrap 限定、shadow 开启 handover、远程外部市场分支）。
- `HostConfig.resources` 可显式配置 worker、source_replay、rollback 三份资源组计划；设置后传入所有对应 Host 启动入口，错误不会回退到无约束启动。计划省略时保持原有序列化字节，继续适配既有发布闭包；实际组核验由 supervisor/control-sys 执行。该配置不生成全局配置 Applied。
- `HostConfig.projection_launch` 通过 `hosted::profile` 把完整投影中的策略首次启动字段绑定到实际 Host：固定投影对象与启动 scope、明确 OS 名称映射、程序摘要、worker CPU/NUMA/内存。仅普通 registered Active/Shadow 首启，拒绝 capture、preflight 和轮换；worker 资源必填。读取和核验在子进程创建前完成，local 分支使用同一份已核验 Bootstrap，不能重新读取路径换入另一身份。启动后由真实进程和资源观察构造私有 `LaunchBindingProof`；`AppStrategy::projection_launch(target)` 仅提供精确实例的历史启动证明，不代表完整配置应用或当前新鲜观测。
- `HostConfig.projection_parameters` 通过 `hosted::parameters` 核对完整投影范围、参数格式定义摘要及工厂原始配置的严格 JSON 值；全局投影与私有配置的修订、摘要分别保存。真实 worker 启动后，私有查询取得 Host 的当前合并检查点配置，核对成功才交付 intent；失败锁存并清理实际子进程，外部观测不能占用内部查询。`AppStrategy::projection_parameters(target)` 只返回精确实例的历史参数消费证明。它不替代 `projection_launch/resources`，两类投影绑定同时启用时须一致，也不生成完整全局 Applied。规格见 [strategy-parameter-consumption.md](../../specs/implementation/strategy-parameter-consumption.md)。
- `strategy`：`Config/MemberConfig/CommandFile/RecoveryFiles/DependencyFile/FileStrategy/MultiStrategy/new_modes/load(digest)`。
- `replay`：`run(&Deployment)`、`export(&Deployment, destination)`。私有 `ReplayLocks` 持有三个源目录的排他锁，在 load 中途失败或 Recovered 生命周期结束时逐个显式 unlock；源仍只读，fork/exec 副本不能延长已结束 owner。
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

`run` 按 `ProductName` 分派 `BinanceSpot` / `BinanceUsdM` depth policy；主循环回调依次驱动 replication、control、observation、shadow_loader、lifecycle、reconciliation，停止条件含 `stop_file`、策略失败、`maximum_runtime_ns`。

## 在 workspace 中的位置

上游 `[dependencies]`（近全栈，`Cargo.toml` 共 39 个 tickwire-*）：`tickwire-account`、`tickwire-account-codec`、`tickwire-account-gateway`、`tickwire-account-journal`、`tickwire-binance`、`tickwire-binance-account`、`tickwire-binance-clock`、`tickwire-binance-execution`、`tickwire-binance-private`、`tickwire-binance-public`、`tickwire-binance-quota`、`tickwire-binance-session`、`tickwire-book`、`tickwire-clock`、`tickwire-journal`、`tickwire-quota`、`tickwire-recovery`、`tickwire-runtime`、`tickwire-runtime-policy`、`tickwire-transport`、`tickwire-venue`、`tickwire-strategy-sdk`、`tickwire-release`、`tickwire-handover`、`tickwire-strategy-host`、`tickwire-market-publisher`、`tickwire-market-source`、`tickwire-replica`、`tickwire-lifecycle`、`tickwire-fencing`、`tickwire-supervisor`、`tickwire-control`、`tickwire-control-store`、`tickwire-join`、`tickwire-ipc`、`tickwire-shm`、`tickwire-snapshot`、`tickwire-risk` 等。

下游消费者（全仓 grep `Cargo.toml` 核对）：`tickwire-lab`。

## 对应规格

- [投影中的策略启动字段](../../specs/implementation/supervisor-process.md#全局投影中的策略启动字段)（`tickwire-config` 完整投影结构与摘要核对、受信 OS 名称映射、实际资源启动和证明边界）。

- [实际实例观测](../../specs/implementation/runtime-observation.md)（本地受信目录、异步新挑战、真实持有进程、持久结果与恢复边界）。

- [../../specs/implementation/market-expansion.md](../../specs/implementation/market-expansion.md)（启动批准的市场扩容、独立控制决定、实际 JOIN 与双收据门禁）。

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

- **实际实例观测**：观测 provider 仅支持 Linux；非 Linux 打开明确拒绝，相关 /proc 测试按平台隔离。`Deployment.observation` 可省略；启用时显式批准 inbox/outbox/journal 三个不同的 0700 真实绝对目录、固定 UID、stream_epoch 和支持查询模式的真实 worker 镜像摘要白名单（1..128，非零唯一）。输入和输出均为 0600 单链接普通文件，不提供远端网络鉴权。`AppStrategy`/Multi 精确路由逻辑策略、实例和代次，Capturing 与未启动 Host 明确不支持；发出查询前用实际 Registry 内核镜像匹配白名单。
- **观测持久与期限**：完整 Request 在派发前同步到独立 WAL；Host 验证的完整依赖允许不同图路径重复引用相同内容；App 按摘要合并相同字节、拒绝同摘要不同字节，形成唯一有序集合后先写 CAS，再写不超过 256 KiB 的 Outcome CAS 与小 Terminal 引用，最后原子发布 `<32hexid>.json`。opaque 对象上限为恢复对象的 64 MiB，三目录总量也不超过 64 MiB。恢复逐对象核对长度/摘要、有序唯一和 READY 必需根，历史结果保留原时间/期限并标 `historical`，进行中查询转为 Interrupted，绝不恢复成新的 FreshObservation 或重新派发。每次打开同步新 session.json。
- **观测隔离和限额**：请求生命周期最长 30 秒，历史请求 1..1024、同时 pending 1..32、每轮扫描/轮询 1..16。10 ms 冷扫描之间无分配且不触发 proc/闭包 I/O；pending 轮询按 owner 轮转，避免低 ID 永久占用预算。长期冷 I/O 前后重新检查实际单调时钟与 Binding，跨期结果转为失败；消费者仍需核对自己的当前期限。错误构建、目标、忙碌、过期为请求失败，存储/权限破坏关闭观测入口并取消查询，不把策略标为失败。
- **观测重试和单写者**：同 ID 同内容不重新挑战；换内容会取消未完成原查询并永久关闭该 ID，冲突结果绑定新 digest，既有终态仅作为历史保留。WAL 的 LOCK 文件由持有 FD 与当前目录项的设备/inode、UID、0600 和单链接共同绑定；冷检查及所有 WAL 操作前重新核验，路径替换后停止使用原 writer。加锁成功立即建立唯一 Wal guard，在后续初始化失败或正常 Drop 时显式 unlock，Storage 的校验副本仅关闭 FD，fork 至 exec 窗口继承的描述符不能延长已结束 writer 的锁；活跃 owner 仍互斥，旧副本释放也不能解锁后继。目录部署 owner 仍是受信边界。
- **观测回归**：`observation/tests.rs` 和 `observation/tests/durable.rs` 覆盖实际 Registry 子进程镜像白名单、同 ID 冲突、错误实例/Binding/session、WAL 同步跨期、恢复中断、闭包篡改/缺根/顺序、总容量、LOCK 替换、双 owner 轮询公平性与空扫描零分配。真实认证挑战和 App 全进程文件验收由 Host/lab 场景覆盖；存储单测中的 Outcome 仅验证持久协议，不代表运行时观察事实。

- **市场扩容控制**：`Deployment.market_transition` 只在已有 `control` 下启用，明确配置 `consumer_directory/effective_directory`（分离的 0700 真实目录）、`maximum_operations`（1..128）、`maximum_per_poll`（1..32）。仅实际 Sources 和托管远程 Host 的 PID/instance/generation 可接收决定；文件为 0600 的 `<pid>-<32hexinstance>.arm.json/.commit.json/.receipt.json`。完整 MarketDecision 先同步到控制 WAL 旁 `market-objects`，再同步其长度/摘要 WAL 引用；对象与 WAL 共用 64 MiB 预算，外部 command sequence、32 KiB 命令和 64 KiB 物理批次限制保持不变。
- **扩容成功边界**：只有实际 Armed/Quiesced/Activated 能产生阶段收据，Activated 先同步 effective 中完整 Commit+Receipt，再同步共享 receipt，最后放行本地 Sources 的新观察。远程 worker 由自身 durable HEAD 与恢复对象持久化门禁放行。恢复校验所有历史对象，却不重放为活跃授权；无批准 QUIESCE 仍关闭旧订阅。文件扫描至少间隔 10 ms，未到期为无分配常量路径，已处理 Arm 缓存文件身份；冷轮询仍包含有界文件与同步 I/O，不声明硬实时隔离。
- **同步后的期限门禁**：Decision 对象/WAL、effective 和共享 receipt 同步后重新采样实际时钟、复核当前 Binding，再检查批准期限。dispatch 与本地 release 不使用同步前时间；跨期限时保持关闭。已可见 receipt 的 observed 时间仍为真实消费者观察时间，MD 用自身当前时钟拒绝过期历史事实。下一次冷扫描从本次完成后至少间隔 10 ms。
- MarketDecision 的 `at_ns` 保存显式传入的已验证准入时刻；同步后的新采样仅决定是否还能 dispatch。同步跨期仍保留 operation 历史，恢复按准入时刻验证且不重放；不得改用较晚的持久化时刻让已准入记录无法恢复。
- **扩容回归**：`control/market/tests.rs` 覆盖决定先持久、对象缺失、超过 64 KiB 的完整批准、收据失败、恢复不重放与冷扫描零分配；`market/tests.rs` 使用真实 MD Server、memfd 和 SEQPACKET 覆盖批准/未批准切换及 release 前无新观察。仅测试依赖 `tickwire-md`、`tickwire-control-sys`、`sha2` 与安全 `allocation-counter`。
- `control/market/tests/clock_tests.rs` 覆盖 Arm/Commit 决定跨期限、两次实际文件同步后的过期/Binding 变化，并用真实 Sources 重新 JOIN 证明过期收据写出后仍不 release 或发布新观察。

- **数据面逐单不过 raft**：正常下单/回报走私有会话与执行通道；控制面（`control::ControlInbox`、持久登记 `tickwire-control-store`）只承载控制命令与授权，不把交易数据送 Raft。
- **schema4 生命周期接线**：`HostConfig.capture` 明确批准采集 Bootstrap 与私有 publication CAS 目录，仍保留普通 Bootstrap 为目标安装模板。`lifecycle/rebuild` 先重算封口窗口并核对当前账户的规范前缀、完整固定动作集合和双方能力，再关闭 handover gate、同 PID 安装及独立回退；七固定根按阶段同步进生命周期 WAL。当前 READY 要同时绑定安装/回退根并追平实际账户；任何后窗未列入合同的 Admit 都要求新操作。Commit/Release 复用真实账户 StrategyHandover、GroupRelease 和旧进程正常退出门禁。
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

- schema4 在冷图校验/同步/安装/探针之后及 Prepared 签发前，用 fresh 单调时钟复核实际 Account 的冻结状态、valid_until 和 gateway halted；到期立即走既有 Halt 持久路径，不依赖下一主循环的续期/observer 才关闭。
