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
