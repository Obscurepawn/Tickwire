# tickwire-strategy-sdk

## 1. 职责（架构位置：策略进程侧）

策略进程（worker）自己持有的市场副本与规范命令 SPSC 发布库。一个策略进程只跑一个 `Strategy` 实现：通过 JOIN 接入独立 MD 的自有市场视图，把回调产生的 `CanonicalCommand` 经规范对象存储与 1024 字节 SPSC 意图 bank 发布给执行端；同时承担启动恢复（preflight/restore）、旧 binary 来源重放（`replay`）、声明重建（`replay::rebuild`）、交接封口（`handover`）、优雅退休（`retirement`）、检查点密封（`checkpoint`）与报告通道（`reports`）。

本 crate 只提供确定性回调与发布契约，**不授予交易授权**：授权、门禁、账户预留都在执行端（account-gateway / account）。lib 根文档：`Strategy-owned market replica and canonical-command SPSC publication.`

## 2. 公共 API 入口

- 核心 trait：`Strategy`（`src/lib.rs`，回调 `on_market` / `on_report` / `on_timer` / `on_external` / `apply_config` / `apply_model` / `checkpoint` / `prepare_replayed` / `replay_registration`）；内置 `ObserveOnly`（只观察）、`FixedDemo`（单次显式演示命令）。
- 市场侧：`market::{MarketReplica, MarketView, DenseId}`（`last_trade/best_quote/mark_funding/bids/asks/best_bid/live/continuous/instrument/state_age_ns/trigger_at`）；`apply_dense`/`view_dense` 复用同一副本目录的本地索引，接收循环每帧只映射一次，回调与记录仍使用外部稳定 `instrument_id`。
- 命令发布：`outbound::{CommandStore, IntentWriter, StoredCommand}`，辅助 `encode_intent`、`resolve_record`、`resolve_file`；`CommandStore` 把命令按 digest 存进 `BlobStore`，`IntentWriter` 写 SPSC bank。
- 模块（均为 pub mod）：`bootstrap`（`Bootstrap` 受信启动描述）、`checkpoint`（`verify_request/verify_reply` 挑战密封）、`handover`（`AccountMirror`、`verify`、`matches`）、`market`、`outbound`、`preflight`（`Job`、`ProbeJob`、`Completed`、`start`、`run_probe`、`verify_recovery_inputs`、`target`）、`recovery`（`Template`/`RecoveryFiles`/`Descriptor`/`RecoveryWriter`，`CHECKPOINT_SCHEMA=0x545753430001`）、`registered`（静态工厂 `Registration`/`UserFactory`/`FactoryBinding`/`Configured`/`configure`/`unsafe fn run`）、`replay`（`Engine`、`ReplayableStrategy`、`SourceReplayFactory`、`ReplayContext`、`ReplayEffects`、`VerifiedStateAtCut`、`replay_source`；子模块 `account/application/catchup/control/durable/install/live/native/process/rebuild/retained/rollback`）、`reports`（`AccountReport::{Baseline, Record}`、`ReportWriter`、`ReportReceiver`、`ReportIdentity`）、`restore`（`Target`、`RestoreFactory`、`VerifiedRestoreInput`，`OBSERVE_SCHEMA/DEMO_V1_SCHEMA/DEMO_SCHEMA/DEMO_MIGRATION`）、`retirement`（QUIESCE 封装）。
- feature：`default = ["runtime"]`；`runtime = ["dep:tickwire-runtime","dep:tickwire-account-gateway","dep:tickwire-binance-account","dep:tickwire-binance-clock","dep:tickwire-binance-public"]`。`src/runtime.rs`（`RecoveryResolver`、`ResolvedRecovery`、`IntentReceiver`、`RuntimeStrategy`）只在 `#[cfg(feature = "runtime")]` 下编译；`handover::matches` 与 `recovery` 内个别项也受该 feature 门控。`--no-default-features` 编译得到不含执行端接线的纯回放/恢复子集。
- bin：`src/bin/tickwire-strategy-worker.rs`，四种模式：`--bootstrap PATH`（正常启动，可选 preflight_job / FixedDemo / ObserveOnly）、`--source-replay PATH`（旧 binary 来源重放）、`--replay-rollback PATH`（回滚作业）、`--restore-probe PATH`（独立恢复探测）。

## 3. workspace 位置

上游（`Cargo.toml` 实际声明）：`tickwire-release`、`tickwire-handover`、`tickwire-market-source`、`tickwire-account-codec`、`tickwire-ipc`、`tickwire-join`、`tickwire-shm`、`tickwire-shm-sys`、`tickwire-snapshot`、`tickwire-recovery`、`tickwire-venue`、`tickwire-control`、`tickwire-control-sys`、`tickwire-clock`、`tickwire-clock-sys`；optional（runtime）：`tickwire-runtime`、`tickwire-account-gateway`、`tickwire-binance-account`、`tickwire-binance-clock`、`tickwire-binance-public`；dev：`tickwire-risk`。

下游（grep `tickwire-strategy-sdk` 命中）：`tickwire-strategy-host`、`tickwire-strategy-example`、`tickwire-app`、`tickwire-lab`。

## 4. 对应规格

- [../../specs/implementation/strategy-rebuild.md](../../specs/implementation/strategy-rebuild.md)（声明重建与 SDK 预热）
- [../../specs/implementation/strategy-handover.md](../../specs/implementation/strategy-handover.md)（实际进程交接）
- [../../specs/implementation/strategy-replay.md](../../specs/implementation/strategy-replay.md)（S→C 来源重放与 schema3 安装，SDK 侧回调/持久化由本 crate 实现）
- [../../specs/implementation/strategy-graceful-retirement.md](../../specs/implementation/strategy-graceful-retirement.md)（report QUIESCE 退休，`retirement.rs` 封装）
- [../../specs/implementation/release-preflight.md](../../specs/implementation/release-preflight.md)（`preflight`/`restore` 的预检与恢复工厂）
- [../../specs/implementation/strategy-input-consumption.md](../../specs/implementation/strategy-input-consumption.md)（当前持久账户与行情输入视图的只读检查）

## 5. 构建与测试

```sh
cargo build -p tickwire-strategy-sdk
cargo test -p tickwire-strategy-sdk
cargo test -p tickwire-strategy-sdk --no-default-features   # 纯子集编译/测试
```

bin 实际运行（由合格 Host/supervisor 以受信 Bootstrap 启动，单独运行不获授权）：

```sh
./target/debug/tickwire-strategy-worker --bootstrap PATH.json
./target/debug/tickwire-strategy-worker --source-replay PATH.job
./target/debug/tickwire-strategy-worker --replay-rollback PATH.job
./target/debug/tickwire-strategy-worker --restore-probe PATH.job
```

`tests/source_replay.rs` 用 `std::process::Command` 实际拉起 worker 子进程，向量来自 `specs/contracts/ipc-vectors-v1.json`；模块内单元测试分布在 `replay/*/tests.rs`、`preflight_tests.rs`、`restore_tests.rs`、`registered/tests.rs`。

## 6. 关键不变量与注意事项

- **恢复成功不是授权**：`restore.rs` 头注释明确“Successful restoration is not a live grant or READY: the worker must still catch up and seal its output.”；恢复回调只产出状态，不产出可发送命令。
- **重放只返回状态**：`replay.rs` 头注释“The same deterministic callback executor serves live recording and isolated old-binary replay. Replay returns state, never commands eligible for sending.”
- **FixedDemo 不自造凭证**：`lib.rs` 头注释“The complete order and authority context are supplied by the launcher; this strategy never invents credentials.”；命令必须绑定目标实例/动作键，消耗后置空（`self.command = None`，只发一次）。
- **注册不获授权**：`prepare_replayed` 默认返回 `Unsupported`；`replay_registration` 默认 `None` 表示保留旧回调 API，不声称完整尾部重放。
- **启动前先绑定全部恢复输入**：`preflight::verify_recovery_inputs` 在任何恢复回调前核对模板与每个依赖文件摘要；回调完成件先保存，再挂市场/意图（`preflight.rs` 头注释）。
- **实际可执行映像**：Linux 从 `/proc/self/exe` 的实际文件描述符校验完整长度和摘要，支持普通 ELF 与封印 memfd，不依赖启动入口路径仍然存在或与 `Role::Binary` 路径相等。恢复清单内的 `Role::Binary` 仍独立进行原权限、内容和重读检查；启动入口删除不等于允许恢复制品缺失。不得用候选路径的 hash 冒充实际运行 image。
- **恢复闭包与消息预算分开**：worker 的来源/回滚闭包与 `Live::installation_objects` 按恢复单对象 64 MiB 读取 opaque 依赖，再由 release 校验完整闭包；`ControlInput`、检查点和其他类型化 replay 消息仍限制 16 MiB。
- **行情换代先持久后接线**：`Live::supports_market_transition` 用于切换前准入；`transition_market(MarketTransitionRequest, &Client, now)` 绑定受信计划、旧 QUIESCE 后 applied 水位与真实新 JOIN。完整基线/catchup 只替换市场视图，不触发用户回调；提交与 HEAD 同步成功后调用方才能替换 Client。Legacy owner 拒绝，失败停止 recorded owner，typed 16 MiB 预算不扩大。历史重放与 schema3 continuation/install 保留前后 bank 流关系；旧 ID/version 不可删除或改写，旧 catalog hash 的批准关系仍由运行时受信计划核对。
- **持续 worker 换源**：`market_transition` 绑定认证 report 41/42 的 kind6 Arm、kind7 Commit；控制对象至多 2 MiB。`worker::expansion` 在真实旧订阅和 replay 能力检查后返回 Armed；匹配操作派生请求的真实 QUIESCE 才暂停回调，完成 PendingJoin、持久 MarketTransition/HEAD、保留 merged 行与检查点后交换消费对象并返回 Activated。`RecoveryWriter` 保留步骤有界，闭包在既有 prepare/seal 同步；它不冒充额外 fsync。未 Arm 的 QUIESCE 保持退休行为，拒绝 Arm 不替换旧消费源。
- **新 JOIN 等待真实生产者 frontier**：新转换采用 basis1；若 JOIN 的 catchup 为空，worker 保持暂停并逐轮有界读取真实记录，不造 synthetic 输入。等待期间与提交前核对真实会话期限、EOF/QUIESCE、cursor/副本一致及盘口连续/新鲜状态；失败关闭，持久成功与新鲜截止检查之后才交换 Client。旧 basis0 重放保持字节兼容，所有本地 JOIN 证据仍参与完整行哈希。
- **输入采集不执行用户工厂**：`replay::rebuild::capture::CaptureOnly` 只在绑定的本机单调时钟域内接收有界行情与账户输入，错误不推进窗口；`finish` 返回真实输入链与账户摘要的封口，不生成用户检查点或 READY。实际进程通过独立 `--capture-only` 注册入口采集，封口后只有认证 kind9 才能进入实际前向重建和安装。
- **采集封口须完整重算**：`replay::rebuild::capture::VerifiedWindow::verify` 从 identity/Contract/seed/Inputs 重放固定观察器并精确核对整个 Seal；`validate_plan` 只核对内容绑定。该模块的 `store::Attempt::begin` 在采集前创建一次性 0700 目录、同步 ATTEMPT；`finish` 同步六个类型对象后最后同步 CAPTURE_WINDOW.json；`read_back` 重读并重算全闭包，任一对象限制 16 MiB。实际 PID、控制挑战、报告游标、当前期限及发布授权由认证 owner 另行核验。
- **独立 CaptureOnly 入口**：公共 `capture::Bootstrap/SealRequest/Receipt` 与 `registered --capture-only` 只装配真实 MD 与账户 report lane，初始账户前缀精确固定；kind8 封口绑定实际 cursor 与一次性持久窗口，输出同时保留 Bootstrap、SealRequest 和 Receipt 对象。采集与尚未获准安装的封口阶段不创建用户工厂对象、不发 READY/intent。封口后同 PID 等待并校验期限；正常停止复用 report QUIESCE、真实 MD detach 和 report Applied 后退出。新的采集 owner 复用 worker 内已有 qualified SHM 边界，不增加 unsafe 调用点。
- **重建保留真实实例与完整历史**：`replay::rebuild::execute_running` 返回不可反序列化的 `RunningCandidate`，同一预热 Application/Engine 连同 Source、RuntimeState、cut、rows 与预算一起保留；`persist` 同步完成闭包及空 genesis→warm delta 两个 commit。`PersistedRunning` 暴露安装需要的内容根和历史，`Live::from_rebuilt` 同步 HEAD 后承接真实实例；已有 HEAD 或任何同步失败均不返回 live owner，不允许调用 `Live::new` 代替此路径而重置历史。`Completion/Observation` 的纯数据定义由 `tickwire-release::rebuild` 持有，SDK 原路径继续重导出。
- **前向重建保留注册工厂**：`registered::configure_rebuild` 在 configure 前核对目标身份、配置版本、模型文件与全部固定 artifacts；`UserFactory::begin_rebuild/consumed_actions` 默认拒绝。实际返回策略经 `Running<F>` 保存原 Configured 工厂，预热后模型版本/RNG 必须与固定输入一致。此入口只执行计算，认证安装、真实通道和发布授权仍由外层负责。
- **handover 只核对恢复数据**：`AccountMirror` 要求基线恰好一次、记录 position 连续且绑定同一基线 digest，`InputSeal` 逐条累计；`verify` 输出仅供 Host 绑定来源身份/WAL，不代表回调已被执行。
- **检查点挑战不可缓存**：`checkpoint.rs` 头注释“cached READY is not a seal response”；`verify_request` 核对 opcode=39、challenge、report_sequence、实例/代次/逻辑策略、有效期不超过会话。
- **unsafe 边界**：`worker::run/run_restored` 与 `registered::run` 标记 `# Safety`，准入边界是已认证 launcher 的 SHM 策略；修改 worker 生命周期时不得去掉该契约注释。
- **feature 门控**：去掉 `runtime` 后 `IntentReceiver/RuntimeStrategy` 不可用，依赖执行端类型的代码必须保持 cfg 门控。

## 7. 改动检查清单

1. `cargo build -p tickwire-strategy-sdk` 与 `cargo test -p tickwire-strategy-sdk` 通过；`--no-default-features` 仍可编译。
2. 改动回调/恢复/重放语义时联动核对 [strategy-rebuild.md](../../specs/implementation/strategy-rebuild.md)、[strategy-replay.md](../../specs/implementation/strategy-replay.md)、[release-preflight.md](../../specs/implementation/release-preflight.md)；交接退休核对 [strategy-handover.md](../../specs/implementation/strategy-handover.md)、[strategy-graceful-retirement.md](../../specs/implementation/strategy-graceful-retirement.md)。
3. 新增规范命令/事件字段时同步 `tickwire-venue` 编码与 `specs/contracts/ipc-vectors-v1.json` 向量。
4. 改动 SPSC/报告布局时核对 `tickwire-ipc` 布局契约，不破坏既定向量。
5. 不得在本 crate 内实现授权、网络发送或账户预留。

- **同 PID 安装承接**：`capture_worker::Outcome/CapturedSession/Computed` 为私有启动状态，registered 路径配置并预热后把原Client/report/clock与同一个Live实例交给`worker::Attached/run_attached`；不重新JOIN，不调用Live::new重置历史。新的intent lane复用原qualified attach操作。kind9首次回执固定真实C；相同挑战只读原回执，其他请求拒绝。kind10核对独立rollback完整图、持久保留后产生新v4 READY。Host仍核验实际PID、cursor与当前期限，内容成功不是执行授权。
- **安装依赖与模板再验证**：raw安装/回退依赖单64MiB、闭包256MiB，typed消息各decoder仍守原16MiB或更小上限。安装重读RecoveryTemplate先按Manifest固定Object核对长度/摘要后解码，不接受长耗时预热后被替换的合法JSON路径；安装和回退同步后均重新采样期限。`capture_worker::install::tests`覆盖模板替换拒绝与17MiB opaque依赖；实际进程验收在Host测试中。

- `replay::rebuild::probe` 重导出 release 的 Job/Receipt，运行时只在完整部署资格核对后执行旧工厂；历史恢复由 release 的共享 Verified 验证实际 Host Completion 图，不重读原路径。opaque Job.objects 单对象 64 MiB、typed 根 16 MiB；共用16层旧来源递归预算。

## 持续运行的实例查询

`checkpoint::OBSERVE_CURRENT`（mode2）把请求报告位置解释为下界，仍回显原挑战和请求位置；READY 保存真实当前位置。`worker/observation.rs` 独立保存一次查询和单份应答，等待期间不暂停行情、账户或定时器；非阻塞发送背压保留同一检查点，截止后仅丢查询。周期 READY 使用单槽最新公告，在查询应答与安装/回退应答队列排空之后尝试非阻塞发送；新检查点清除较旧待发公告，避免迟到序号回退。查询也保留同一新 READY 的普通公告，使最后消费位置在没有后续账户输入时仍能公布。只有身份与结构已核验的过期 mode2 请求按请求级失败处理，旧模式 0/1 保留原精确封口行为。调用方须显式批准支持 mode2 的 SDK 构建。

`checkpoint` 单测覆盖下界、旧模式精确水位、应答回显、旧 READY 与过期错误分类；worker 单测覆盖未追平、背压单次执行和同步跨期。真实 registered 进程与 Host 前缀、游标及内核核验见 [../../specs/implementation/runtime-observation.md](../../specs/implementation/runtime-observation.md)。

`replay::application::observed::inspect(&Checkpoint)` 严格检查当前 SDK 保存视图，返回字段私有的 `InputViews/AccountInput/MarketInput`。账户基线和有序记录重建实际位置与 InputSeal；市场恢复实际连续视图、完整稳定标的 ID/版本及 ResourceHeader；两个完整列表与当前 RuntimeState 的 stream/sequence 逐项匹配。该接口不调用策略工厂、策略恢复或用户回调，不把内容检查当作活进程证明。`ReportWriter::header/identity` 只读提供实际 publisher 描述符与已绑定报告身份。单测覆盖当前账户/生产者变化、完整集合、畸形保存视图及水位错配。
