# tickwire-strategy-host

## 1. 职责（架构位置：策略进程侧 —— 父进程托管层）

父进程侧持有的策略进程管理器：负责真实 JOIN、SPSC 命令摄入、就绪/预检证明（READY seal）、独立回滚 probe、远程托管（`RemoteHostedStrategy`）与轮换（rotation），以及只持有 report lane 的独立 CaptureOnly 进程。Host 自己不跑策略回调，它认证子进程、保管报告写入、核对恢复完成回执，并在交接/退休时取得真实子进程退出证据。lib 根文档：`Parent-owned strategy process, real JOIN and SPSC command intake.`

## 2. 公共 API 入口

- `pub mod capture`（`Owner/Launch/Captured`，启动、报告发布、封口、轮询与真实正常退休）；`pub use remote::{RemoteHostedStrategy, RemoteLaunch}`；`pub mod rebuild_probe`（`Completed`、`run(spec, directory, job, deadline)`）。
- `HostedStrategy<R, const M: usize>`（`src/lib.rs`，含 `new/notice/error/failed/set_shadow/instance/child/publisher/stop/set_rotation_source`）、`Started<R, M>`（`publisher: SharedPublisher<M>`、`strategy: RuntimeStrategy<R, MarketCallback>`、`child: ChildHandles`）、`Launch`、`ChildHandles`（`market/intent/report: AuthenticatedChild`）、`SharedPublisher<M>`、`PublicSink<M>`、`RotationPlan<R, M>` 与 trait `RotationSource<R, M>`、`MarketCallback`。
- 内部模块（非 pub，仅 crate 内）：`preflight`（Host 侧完成件/回执装订）、`readiness`（`ReadyProof`，子模块 `cache/capture/install`）、`remote`、`source_replay`。
- 无 `[bin]`、无 examples；本 crate 是被 `tickwire-app` 装配的库。

## 3. workspace 位置

上游（`Cargo.toml` 实际声明）：`tickwire-strategy-sdk`、`tickwire-supervisor`、`tickwire-control`、`tickwire-control-sys`、`tickwire-control-store`、`tickwire-market-publisher`、`tickwire-runtime`、`tickwire-runtime-policy`、`tickwire-journal`、`tickwire-join`、`tickwire-ipc`、`tickwire-shm`、`tickwire-recovery`、`tickwire-clock`、`tickwire-clock-sys`、`tickwire-binance-public`、`tickwire-binance-clock`、`tickwire-binance-account`、`tickwire-account`、`tickwire-account-gateway`、`tickwire-account-codec`、`tickwire-venue`、`tickwire-market-source`、`tickwire-md`、`tickwire-release`。

下游（grep 命中）：`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/strategy-handover.md](../../specs/implementation/strategy-handover.md)（必须取得真实 `ExitObservation`，旧实例退休失败不激活新实例）
- [../../specs/implementation/strategy-graceful-retirement.md](../../specs/implementation/strategy-graceful-retirement.md)（先发认证 report QUIESCE，等 worker DETACH/释放 cursor，再等 Child 退出；超时失败、新实例不激活）
- [../../specs/implementation/release-preflight.md](../../specs/implementation/release-preflight.md)（Host 核对真实 PID/UID/GID、binary hash、Bootstrap、来源与同步输出，再跑独立 rollback probe）

## 5. 构建与测试

```sh
cargo build -p tickwire-strategy-host
cargo test -p tickwire-strategy-host
```

无独立 bin/example；业务进程场景由 `tickwire-app` / `tickwire-lab` 装配驱动，`tests/capture_process.rs` 另提供显式运行的两个 Linux CaptureOnly 独立 worker 验收。模块内有少量单元测试（如 `retirement_requires_an_actual_owned_child_exit`，验证空 Registry 的 terminate 错误不会被吞成交接成功）。

## 6. 关键不变量与注意事项

- **退出必须有真实证据**：`retire_owned_child` 调用 `Registry::terminate(instance)`，错误（含未知实例）直接返回 `Error::Supervisor`，禁止把失败当成功（对应 strategy-handover.md 修复点）。
- **远程直连 MD**：`remote.rs` 头注释“Strategy connects directly to independent MD; execution owns only intent/report lanes.”Host 不代跑行情。
- **持续行情扩展**：`RemoteHostedStrategy::market_consumer/arm_market/commit_market/market_transition/market_rejection` 使用真实持有的子进程身份与现有报告 CAS。调用方先接受可信运行控制决定，Host 经认证 report 41/42 有界发送并统一分发回执；实际 PID、审批摘要、期限、Armed→Quiesced→Activated 顺序和最终 bank 摘要逐项核对。kind6/7 的拒绝状态保留旧消费者，操作拒绝后不再接纳成功回执。文件对象有效本身不建立授权。
- **完成件只装订 Host 派生字段**：`preflight.rs` 头注释“Finalize only fields derived by Host. The signed Bootstrap template is checked before any child is created; an unbound template cannot become a runtime job.”
- **轮换串行**：`RotationPlan`/`RotationSource` 在旧 Active 成功停止、新实例 READY 前不激活；交接不重复发单、不释放资金（资金释放由账户日志/网关负责，Host 不触碰）。
- **退休顺序**：先发认证 report QUIESCE → 等 MD DETACH 与 cursor 释放 → 等直接持有的 Child 成功退出；单纯连接关闭不是 QUIESCED 证据。
- **probe 独立**：rollback probe 只连认证控制 socket，以目标刚输出的状态执行回滚工厂，没有行情/账户/意图 FD；Host 等其成功退出才保存两份回执与 Completion。
- **probe 依赖预算**：回滚完成闭包可含旧二进制等 opaque 依赖，`read_probe_object` 按恢复单对象 64 MiB 读取并核对长度与摘要；类型化 replay 控制请求仍限制 16 MiB，完整闭包预算由 release 核验。

## 7. 改动检查清单

1. `cargo build -p tickwire-strategy-host && cargo test -p tickwire-strategy-host` 通过。
2. 交接/退休改动联动 [strategy-handover.md](../../specs/implementation/strategy-handover.md)、[strategy-graceful-retirement.md](../../specs/implementation/strategy-graceful-retirement.md)；预检/回执改动联动 [release-preflight.md](../../specs/implementation/release-preflight.md)。
3. 子进程认证、SHM 资源与 cursor 契约依赖 `tickwire-supervisor`/`tickwire-join`/`tickwire-shm`，改动时同步核对其规格。
4. 不得在 Host 内实现策略回调、授权发送或账户资金释放。

## CaptureOnly 进程入口

`capture::Owner` 独立持有已批准 registered 子进程、真实 MD Admission 和唯一 report lane；不创建 intent，不构造业务 READY。`seal` 固定当前实际账户报告位置，`poll` 认证 kind8 成功并重读、重算完整窗口，随后复核真实 PID、期限和 mapped report cursor。失败持久关闭并收尸；成功保持子进程存活，正常 stop 执行 report QUIESCE→真实 MD detach→report Applied→成功 Child exit；Drop 只提供异常 kill/wait 兜底，不证明 MD detach。规格见 `strategy-rebuild.md` 的 Host 采集 owner。

Capture 模式沿用 Host 已有 qualified-SHM launcher 的调用者安全契约，`Owner::launch` 显式为 unsafe；它不把任意 LaunchSpec 当作安全凭证。报告 FD/cursor 建立复用从 remote launcher 提取的原接缝，没有新增底层映射访问。

Linux 独立注册进程验收（普通 `cargo test` 明确忽略；本命令必须另行执行）：

```sh
cargo build -p tickwire-strategy-example --bin tickwire-counting-worker
TICKWIRE_CAPTURE_WORKER="$(pwd)/target/debug/tickwire-counting-worker" cargo test -p tickwire-strategy-host --test capture_process -- --ignored --nocapture
```

设置 `CARGO_TARGET_DIR` 时相应调整 `TICKWIRE_CAPTURE_WORKER` 的绝对路径。验收使用实际 MD JOIN server/Publisher 父线程及独立 counting worker；不能表述为独立 MD 进程。无效 counting 配置内容仍由 Contract 正确绑定，采集成功证明未执行其 configure/create；同时核验实际报告位置、封口内容、PID 保持、正常 MD detach、期限/内容错误关闭与 Drop 收尸。

Capture 测试依赖 `tickwire-binance/book/snapshot/risk` 仅在 dev-dependencies 中；不进入生产默认路径。`Captured::objects` 包含落盘读取的 Bootstrap/SealRequest、窗口六对象及 Receipt，共九对象。真实负例包含完成文件被替换、Bootstrap/SealRequest 任一 CAS 根缺失；错误不得短暂暴露 Captured。

## 同 PID 声明重建提升

`rebuild::Hosted/Launch` 是 qualified Capture launcher 的持续模式，启动时保存原普通 Bootstrap，使用 `capture-seal.json` 冷控制选择实际账户 cut，再由真实 Captured 与受信 `objects.json`/forward-files.json/rollback-files.json 构造固定输入。实现 runtime 的三段 rebuild 接口；kind9 只在同 Registry 真实 PID 的第二 intent 连接成功且完整安装回执/READY通过后提升为 RemoteHostedStrategy。独立 probe 成功退出后 kind10 回写完整 rollback 依赖并取得新 READY。普通行情、报告、监督、策略权限、退休沿原 Remote owner；采集事实本身仍不获交易权限。

新增 `Hosted::launch` 显式 unsafe 签名保留既有 qualified-SHM 调用者契约。`capture::Owner::launch_mode` 是私有共享启动模式；`remote::connect_intent` 从原 Remote launcher 提取已有 IntentReceiver::attach 接缝，实际底层不安全访问没有新增。提升方法只能由上述已合格构造的对象调用。

`tests/capture_process.rs` 复用同一 qualified 启动接缝，并挂载 `tests/rebuild_process/` 的显式 Linux 验收：同 PID Capture→真实 Counting v2 factory→kind9/真实 intent 连接→C 后真实行情与账户→独立 probe 成功退出→kind10/v4 READY→正常 MD detach。逐字节核对 C→D 原始输入和实际业务计数，真实产物变异覆盖错误 PID 与替换 probe 根。命令关闭，验收不声称提交新订单；完整交易发布另由 lab 运行。使用 release worker 的独立冻结副本设置 `TICKWIRE_CAPTURE_WORKER`，可用 `TICKWIRE_REBUILD_EVIDENCE` 导出全部对象与结果 JSON。


## 显式进程资源计划

`ProcessResources { worker, source_replay, rollback }` 与 `GroupResources { cgroup, cpus, numa_node, memory_max }` 是可序列化的严格启动配置值，`validate` 对全部三组路径、用途分离和系统 ResourceLimits 边界做纯值校验。`Launch`、`RemoteLaunch`、`capture::Launch` 新增可选 `resources`，Rust 调用者在兼容模式显式填 `None`；App JSON 缺省仍为 None。未知、重复或缺少组字段拒绝。

所有 worker 启动以及 `source_replay`、restore-probe、replay-rollback、rebuild-rollback 统一经 `resources::spawn` 按用途选择组。三组先做纯值校验，仅本次用途组实际 acquire 并核对内核限制与空成员，未使用组不提前宣称内核资格。Some 计划不得回退普通 spawn；ReadyProof/preflight 保留计划到后续 helper，恢复历史完成件不会产生当前资源证明。`rebuild_probe::run` 保留旧入口，显式资源计划使用 `run_with_resources`。同 PID Capture→Live 继续持有原 Registry Entry 的资源 lease，不再次启动 worker。

`ChildHandles`、`HostedStrategy`、`RemoteHostedStrategy`、`capture::Owner`、`rebuild::Hosted` 提供冷 `observe_resources`，直接复用监督器的真实内核/镜像/进程采样，不在正常 poll 增加 `/proc` I/O，也不产生全局配置 Applied。退出不证明 cgroup 释放，下一次 acquire 必须重新核验专用空组。

`resources/tests.rs` 使用真实 ELF 验证三类资源失败均不执行目标、正确用途的目录被实际访问、未使用组非法也拒绝、严格 JSON/边界与 legacy 真实启动。普通文件系统伪组会被内核边界拒绝。本机没有可写委派 cgroup，成功组内启动与完整资源约束未在 Host 实测；有条件的正向验收依赖 supervisor/sys 的显式 ignored 测试。规格见 `specs/implementation/supervisor-process.md` 的 Host 段落。

## 新鲜运行实例观测

`ObservationRequest { request_id, deadline, require_resources }` 与字段私有的 `FreshObservation` 供本地 `HostedStrategy`、远程 `RemoteHostedStrategy` 以及完成提升后的 `rebuild::Hosted` 使用。`observe_process` 读取真实持有进程；`begin_observation/poll_observation/cancel_observation` 复用报告会话的 mode 2 检查点挑战。调用方必须显式批准实际镜像支持该 SDK 模式；纯 CaptureOnly 拒绝，旧模式 0/1 不改变。

普通维护只分发和保存固定查询响应，周期 READY 在查询期间仅保留最新水位。显式冷 `poll_observation` 才读取完整恢复闭包，核对执行端实际报告前缀、共享 cursor、前后 Registry 镜像/启动 ticks/身份与配置资源组，并在 I/O 后重新检查原始截止点。周期 READY 可以因发送背压先于较早查询响应到达；查询按发起时固定水位核对，不与后来周期水位混淆。没有待完成查询时 `poll_observation` 不读取恢复对象或 `/proc`。

挑战域同时绑定实际内核 host boot、PID、启动 ticks、实例、共享封口序号、会话 epoch 与外部请求 ID，避免不同进程 incarnation 复用挑战。每个 owner 至多接纳 4096 个不重复的请求 ID；前置资格拒绝不消耗 ID，接纳后即使派发失败也不能重用。每次查询预分配 4096 项真实 `(账户位置, InputSeal 摘要)`，报告发布后只追加，不增加热路径分配；超限仅失败查询。最多保留 16 个已发送但尚无应答的关闭挑战，原期限内仍占用控制通道，超期后只释放互斥而不遗忘迟到证据；容量满拒绝后续查询，正常交易不受影响。当前版本不依据超时或 FIFO 自动回收无应答墓碑，只有认证匹配响应会排空对应项。

查询与封口、安装、换源、退休串行。取消没有 worker 取消 opcode，因此提前取消仍 Busy 至匹配响应或原截止；所有已有报告接收入口会吸收自己的已关闭挑战。查询到期、取消、前缀容量不足不暂停回调、不终止子进程、不改变交易授权。结果只证明本次采样，不能产生全局配置 Applied。规格见 `specs/implementation/runtime-observation.md`。单元测试覆盖前缀保留/预算、旧挑战身份/epoch/最低水位、取消排空、周期与查询应答乱序及请求容量；真实进程验证由 `tests/rebuild_process/fresh.rs` 提供，沿同 PID 安装后的实际 owner 运行。

慢接收真实进程用例在控制 socket 背压下仍消费 176 条账户记录，核对旧排队查询的完整闭包与实际业务计数；长流末端新查询触及既有 typed 16 MiB 上限时明确拒绝，保留 worker 存活并完成真实 MD/report 退休，不把容量拒绝计作 Fresh 成功。

`HostedStrategy::prepare_start(now)` 复用原 `next` 的错误检查、维护与 lazy 启动顺序，返回是否已启动，不读取 intent；false 通过 `failed()` 区分等待与失败。`next` 也调用此接口，行情尚未 Live 时不提前启动。App 可在启动后、首次交付 intent 前调用冷 `observe_resources` 完成投影启动字段核验；未启动返回 NotReady。该顺序见 [supervisor-process.md](../../specs/implementation/supervisor-process.md)，新接口不生成 Applied 或改变原启动条件。
