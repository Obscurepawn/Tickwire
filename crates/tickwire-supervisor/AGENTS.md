# AGENTS.md — tickwire-supervisor

## 职责

监督层：作为可信 launcher 持有直接派生的子进程活句柄，并在子进程 exec 后通过控制通道完成认证握手、登记 `Session`，观察退出与终止子进程。它只管理"本 Registry 直接拥有的子进程"，不承担紧急撤单/隔离执行（那是 `tickwire-guardian` 的独立路径），也不释放子进程后代可能持有的共享内存资源。

## 公共 API 入口

- `LaunchSpec { identity: Identity, executable, arguments, working_directory, environment, uid, gid, inherit_output }`（`src/lib.rs`）。
- `Registry { new(capacity) }`，容量 1..=4096：
  - `spawn(spec) -> ProcessIdentity`：校验路径/参数/环境、将单次打开的ELF复制并封印，对固定镜像做SHA-256并与`identity.build_hash`比对后从固定FD执行。
  - `process(instance) -> ProcessIdentity`：先 `try_wait`，已退出返回 `Exited`；保持轻量路径。
  - `observe(instance) -> LiveChildObservation`：显式冷采集实际boot/PID启动ticks/UID/GID/固定镜像与连接epoch，字段私有，不证明配置已应用。
  - `accept(listener, policy, now, deadline) -> AuthenticatedChild`：内核凭据比对 + `Session::accept` 握手后发布连接。
  - `disconnected(instance, epoch)`：标记断连（重连要求更晚 epoch，不续交易授权）。
  - `observe_exits() -> Vec<ExitObservation>`：扫描已退出的直接子进程。
  - `terminate(instance) -> ExitObservation`：仅对本 Registry 拥有的子进程 `kill/wait`。
- `AuthenticatedChild { connection: tickwire_control_sys::Connection, session: Session, process: ProcessIdentity }`。
- `ExitObservation { instance(), generation(), pid(), status() }`：只证明直接子进程退出。
- `Error::{Invalid, Conflict, Capacity, Exited, Identity, Busy, Io, Control, Transport}`。

## 在 workspace 中的位置

上游 `[dependencies]`：`tickwire-control`、`tickwire-control-sys`、`tickwire-ipc`；第三方 `sha2`。

下游消费者（全仓 grep `Cargo.toml` 核对）：`tickwire-strategy-host`、`tickwire-app`。

## 对应规格

执行镜像与观察来自 [../../specs/implementation/supervisor-process.md](../../specs/implementation/supervisor-process.md)。会话握手与凭据规则来自 [../../specs/implementation/control-session.md](../../specs/implementation/control-session.md)；退出观察语义不代表后代资源释放，由调用方对账。

## 构建与测试

```sh
cargo build -p tickwire-supervisor
cargo test -p tickwire-supervisor
cargo clippy -p tickwire-supervisor --all-targets -- -D warnings
cargo fmt -p tickwire-supervisor
```

`tests/observation.rs`含4项实际owned-child观察回归；`tests/authenticated_image.rs`含3项实际Registry认证/再次exec/错误摘要回归。各ignored child入口由父测试真实执行；观察解析器另有3项单测。spawn/SEQPACKET/memfd依赖Linux内核，非Linux镜像入口明确拒绝。

## 关键不变量与注意事项

- **活句柄防 PID 重用**：`Registry` 持有 `Child` 句柄直到 drop；`process()` 每次先 `try_wait`，已退出实例返回 `Exited`，绝不把一个已回收的裸 PID 当作存活子进程（`lib.rs::process`）。
- **内核凭据即身份证据**：`accept` 不用父端创建的 socketpair 冒充子进程；`listener.accept` 后读取 `peer_credentials()`，要求与仍拥有的 `ProcessIdentity`（pid/uid/gid）一致，Hello 帧不携带 fds，再交给 `Session::accept`（`lib.rs::accept`）。
- **认证中途退出不发布会话**：握手前后核对实际`/proc/<pid>/exe`与条目持有的封印镜像dev/inode/长度/seals；握手完成后再次`try_wait`，若进程已退出则返回`Exited`，不把 `AuthenticatedChild` 交给调用方（`lib.rs::accept` 尾部）。
- **重连不续授权**：`disconnected` 仅清 `connected` 标志；`accept` 要求 `policy.control_epoch > last_control_epoch` 且 `policy.peer == entry.identity`，重复连接/旧 epoch 返回 `Conflict`。
- **可执行文件指纹**：`spawn`只哈希已封印镜像，必须等于`identity.build_hash`才执行；按原路径设置argv[0]。系统动态解释器/共享库仍属宿主资格，不自动改写`$ORIGIN`。参数≤128、环境≤128、禁NUL、绝对路径（`lib.rs::spawn`）。
- **terminate 只杀自己拥有的子进程**：不从裸 PID 重建句柄；`Drop` 时对未退出子进程 `kill/wait`（`lib.rs::terminate/Drop`）。
- **supervisor 不替代 guardian 紧急路径**：紧急查询/撤单由独立的 `tickwire-guardian` owner 执行，本层不发起任何交易所动作。

## 改动检查清单

1. `cargo build -p tickwire-supervisor && cargo test -p tickwire-supervisor` 通过；涉及 spawn/凭据的改动在 `./scripts/check-linux.sh` Docker 内验证。
2. 握手/epoch/租约规则改动联动 `tickwire-control` 与 `control-session.md`；通道布局联动 `tickwire-control-sys`/`tickwire-ipc`。
3. 新增退出/重连语义需同步 `observe_exits`/`terminate` 的状态推进，避免把已退出实例当存活。
4. 本 crate 不做紧急撤单、不释放后代共享内存、不解释业务结果。

## 实际内核身份观察

`Registry::observe(instance)` 返回字段私有的 `LiveChildObservation`，包含批准的 Identity、实际 PID/UID/GID、内核 host boot/start ticks、固定镜像摘要与长度、已连接 epoch 和采样起始 Instant。它在读取前后检查同一个 owned Child 与实际执行镜像；`process()` 保持原轻量退出检查。该观察不证明新的 challenge 响应，也不包含配置 Applied 状态。`tests/observation.rs` 用实际子进程覆盖异常 comm、退出、路径替换和再次 exec；Linux 解析模块另验证严格边界。
