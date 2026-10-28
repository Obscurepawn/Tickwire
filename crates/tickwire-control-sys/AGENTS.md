# tickwire-control-sys

## 1. 职责

有界 Linux 控制报文的 OS 边界：pathname `SOCK_SEQPACKET` 面向数据报、`SCM_RIGHTS` 描述符传递、CLOEXEC、peer credentials 与私有目录管理。它是控制面认证会话（`tickwire-control`）与 JOIN/MD 等进程间 FD 交付的内核接缝；报文体继续使用 `tickwire-ipc::ControlFrame`，本 crate 不定义第二套 wire 布局。审计过的 unsafe 边界（`#![deny(unsafe_op_in_unsafe_fn)]`），workspace 的 `unsafe_code=forbid` 在此 crate 按注释说明显式放行。

- `src/linux.rs`：`Connection`、`ControlListener`、`PrivateDirectory`、`monotonic_ns`。
- `src/unsupported.rs`：非 Linux 桩（全部返回 `UnsupportedPlatform`）。
- `src/executable.rs`：有界 ELF 复制、memfd 不可变封印、固定 FD 执行与实际进程镜像比对。
- `src/process_resources.rs` 与子模块：预配置 cgroup v2 专用 leaf 的排他 lease、执行前迁入和 affinity 校验、启动后的实际线程资源观察。

## 2. 公共 API 入口

- 常量：`MAX_FRAME_BYTES = 65_536`、`MAX_FDS = 16`。
- `Connection`：`connect(path, deadline)`、`send(bytes, fds: &[BorrowedFd], deadline)`、`try_send(bytes,fds) -> bool`、`recv(deadline) -> ReceivedFrame`、`try_recv() -> Option<ReceivedFrame>`、`peer_credentials() -> PeerCredentials`。
- `PendingConnection::new(path)` / `poll() -> Option<Connection>`：无等待的 pathname 建连，成功转移一次 Connection，Drop 取消。
- `ControlListener`：`bind(path)`、`bind_with_mode(path, mode)`、`accept(deadline) -> Connection`、`try_accept() -> Option<Connection>`。
- `PrivateDirectory`：`create_in(parent, prefix)`（0700 私有目录）、`path()`。
- `ExecutableImage::{open(path,max_bytes),len,is_empty,reader,spawn,matches_process}`：独立只读 reader 从偏移零读取封印后的字节；spawn 保留原 argv[0]，配置回调不能替换执行程序；matches_process 只比对调用时实际 `/proc/<pid>/exe`，不代替 owned Child 生命周期证明。
- `ResourceLimits { cpus, numa_node, memory_max }` / `validate()`；`ResourceDomain::{acquire(path,limits),limits,spawn(&mut self,image,configure),observe(pid)}`。固定真实 cgroupfs 目录及文件 FD，目录 inode 上 `flock` 阻止协作 owner 重复占用；同一 lease 只允许一次启动尝试。
- `ResourceObservation` 的字段私有，getter 返回 pid、limits/cpus/numa_node/memory_max、thread_ids、group_device/group_inode 与 sampled_at；调用方还须持有并复核实际 Child/镜像/starttime，不产生配置 Applied。
- `monotonic_ns() -> Result<u64>`；`PeerCredentials{pid, uid, gid}`；`ReceivedFrame`（`bytes()`/`fds()`/`into_parts()`）；`Error`/`Result`。

## 3. 在 workspace 中的位置

- 上游依赖：`libc`、`tickwire-ipc`（`ControlFrame` 体校验）。
- 下游消费者（已核对 Cargo.toml）：`tickwire-control`、`tickwire-join`、`tickwire-market-source`、`tickwire-md`、`tickwire-strategy-host`、`tickwire-strategy-sdk`、`tickwire-supervisor`、`tickwire-snapshot`（仅 Linux dev-dependency）。

## 4. 对应规格

- [../../specs/implementation/control-transport.md](../../specs/implementation/control-transport.md)（接收所有权、发送/等待/身份、try_* 单次语义）。
- [../../specs/implementation/supervisor-process.md](../../specs/implementation/supervisor-process.md)（封印执行镜像及实际内核身份边界）。

## 5. 构建与测试

```sh
cargo build -p tickwire-control-sys     # 历史 macOS 编译通过；本阶段未复测非 Linux
cargo test -p tickwire-control-sys      # 真实报文测试全部 Linux-only
cargo clippy -p tickwire-control-sys --all-targets -- -D warnings
cargo fmt --check -p tickwire-control-sys
```

- `tests/transport.rs`（21 项）带 `#![cfg(target_os = "linux")]`：SEQPACKET 一包一帧、SCM_RIGHTS FD 回收、peer credentials、截断/FD 数量校验、截止点与 EINTR。
- `tests/platform.rs`（1 项）覆盖非 Linux 拒绝路径。
- 真实内核行为（memfd/seals、SEQPACKET/cmsg/PEERCRED）仅在 Linux 验证；完整平台回归应使用 [../../scripts/check-linux.sh](../../scripts/check-linux.sh) 在固定 Docker 镜像内运行；macOS 不跑真实 socket 测试。
- `tests/executable.rs`：6 项实际 ELF 回归，另1个 ignored child入口由父测试实际执行；覆盖改名/删除/原inode覆盖、写入/增长/截断封印、镜像身份、脚本/上限、动态ELF/argv0与CLOEXEC。模块单元测试另覆盖source metadata之后增长的有界拒绝。
- 无 bench。
- `process_resources` 单元测试覆盖严格 CPU/内存/进程/线程解析（Name 允许非 UTF-8）、组配置变异、伪文件系统与符号链接拒绝、有界读取、真实 cgroup 目录锁排他、真实子进程 affinity 及全部线程继承、执行前 syscall/设置校验失败不执行目标。`tests/process_resources.rs` 的正向组测试显式 ignored；需要 `TICKWIRE_RESOURCE_{CGROUP,CPUS,NUMA,MEMORY_MAX}` 指定实际预配置委派 leaf，本机无权限，尚未执行。实际 affinity 子进程验证不替代完整组验收。
- `transport.rs` 追加实际 UNIX backlog 耗尽后重试、try_send 队列背压、0/1/16 FD 完整帧与所有权测试。

## 6. 关键不变量与注意事项

- **执行镜像不可变**：源文件只打开一次；copy累计长度同时受打开长度与512MiB硬上限约束。先封印再供上层核摘要，执行只用固定描述符；原路径和原inode后续变化不能换掉执行字节。MFD_EXEC仅在EINVAL时尝试旧内核兼容flags，权限/策略拒绝不回退；F_SEAL_EXEC为旧内核可选，WRITE/GROW/SHRINK/SEAL始终强制。只读FD取代原可写description后才exec，避免ETXTBSY。
- **FD 唯一所有权**：socket/accept/目录/inode pin 的 FD 全由 `OwnedFd` 持有；创建与 accept 原子设置 CLOEXEC+NONBLOCK；不提供接管任意裸 FD 的公开构造函数。
- **资源限制与失败边界**：仅接纳 domain、无子组且初始无进程的实际 cgroup v2；effective CPU 集合/单 NUMA 节点、有限 memory.max 与 swap=0 必须精确匹配。不修改 controller、不创建/删除组；取得 lease 后的私有 guard 在构造失败和最终 owner 销毁时显式解锁，临时 fork FD 副本不延长 lease；Drop 只释放 FD/lease。pre_exec 使用父进程预开的迁移 FD，只有有界 raw read/write/affinity 系统调用，权限或读回失败阻止 exec；postexec 观察失败则回收本次直接创建的 Child，目标可能已经执行。不得杀未知组成员。
- **资源证据范围**：memory.max 是组 charged memory，父创建的映像/SHM 另计父 owner 预算；允许 CPU 集合不证明物理隔离或独占。外部部署管理者必须尊重 advisory lease；采样不保证未来设置恒定，不是授权。新 unsafe 仅在本 sys crate 的文件/affinity/pre_exec 边界，均保留 SAFETY 依据，SDK 不增加 unsafe。
- **接收**：每次 `recvmsg` 提供 65,536 字节 payload 与可容纳 16 个 FD、按 usize 对齐的辅助缓冲区，`MSG_DONTWAIT | MSG_CMSG_CLOEXEC`；成功后先把每个 SCM_RIGHTS 整数转 `OwnedFd`，再处理截断/零长/帧/FD 数量校验；任一步失败由局部对象析构关闭已交付 FD；`MSG_CTRUNC` 时内核关闭未交付的多余 FD。
- **发送**：先结构校验与实际 FD 数核对，才 `sendmsg(MSG_DONTWAIT | MSG_NOSIGNAL)`；FD 仅借用到调用结束，发送者保留句柄；完整数据报交给内核即返回成功（接收/owner 应用是另外两个完成点）；部分发送按截断错误返回，**不重发剩余字节**。
- **截止点**：connect/accept/send/recv 共用同一绝对 `Instant`；EINTR 不更新截止点；EAGAIN 进入剩余时间有界 poll；Unix connect 因 backlog 满返回 EAGAIN 时重试 connect，不当作已建连。
- **try_* 单次语义**：`try_accept` 一次非阻塞 accept4 并核验 SO_PEERCRED；`try_recv` 一次 `MSG_DONTWAIT` recvmsg；EAGAIN/EWOULDBLOCK 返回 `Ok(None)`，EINTR 原样作为 `Error::System`，不等待、不内部重试、不读 `Instant`。
- `try_send` 一次 sendmsg，EAGAIN 返回 false，EINTR 原样返回，调用者保留整包与借用 FD；原 send 与其共用单次实现。`PendingConnection` 的 AF_UNIX EAGAIN 保持未建连，下次重试 connect；EINPROGRESS/EALREADY 另用零超时就绪与 SO_ERROR 检查，不能把 backlog EAGAIN 当成 TCP 异步完成。EINTR 保留 pending FD，其他错误关闭它。
- 未知 cmsg 类型拒绝整帧，但继续收集其后的 SCM_RIGHTS，避免提前遗失所有权。
- 本层只保证报文边界与 FD 所有权；帧体语义、授权、去重由 `tickwire-control` 与上层完成。

## 7. 改动检查清单

1. 改 cmsg/SEQPACKET/PEERCRED 逻辑前先读 control-transport.md 接收与所有权小节。
2. macOS 仅验证编译与拒绝桩；本阶段未执行非 Linux 检查。真实行为回归走 `./scripts/check-linux.sh` Docker 全量。
3. 新增 unsafe 保持 `unsafe_op_in_unsafe_fn` 与 SAFETY 注释；clippy `-D warnings`、`cargo fmt --check`。
4. 报文体变化联动 `tickwire-ipc::ControlFrame::decode`，不要在本层引入 JSON 或第二套布局。
