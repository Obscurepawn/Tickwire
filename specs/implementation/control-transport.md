# Linux 控制 FD 传输实现规格

实现范围是 [控制传输接口](../../docs/implementation/control-transport-interface.md) 中的 pathname `SOCK_SEQPACKET`、`SCM_RIGHTS` 和内核连接身份。报文继续使用 [IPC ABI](../../docs/design/ipc-abi.md) 与 `tickwire-ipc::ControlFrame`，没有第二套 wire 布局或 JSON 运行时解析。

## 接收与所有权

1. socket、accept、目录与 inode pin 的 FD 均由 `OwnedFd` 唯一持有。socket 创建和 accept 原子设置 CLOEXEC、NONBLOCK；不提供接管任意裸 FD 的公开构造函数。
2. 每次 `recvmsg` 提供 65,536 字节 payload 和可容纳 16 个 FD 的、按 `usize` 对齐的辅助缓冲区，设置 `MSG_DONTWAIT | MSG_CMSG_CLOEXEC`。
3. 成功的系统调用返回后，先把每个 `SCM_RIGHTS` 整数转成 `OwnedFd`，再处理截断标志、零长报文、完整帧与 FD 数量/索引校验。任一步失败均由局部对象析构关闭已交付 FD；`MSG_CTRUNC` 时内核负责关闭未交付的多余 FD。
4. `ReceivedFrame` 私有保存 bytes 与 FD 数组。`into_parts` 显式转移二者的所有权；借用访问不会复制 FD。

辅助数据解析依赖 Linux 内核返回合法 cmsg 边界与新安装、互不重复的描述符编号；发送者不能直接构造接收缓冲区中的这些内核输出。实现仍检查所有长度及对齐运算。未知 cmsg 类型拒绝整帧，但继续收集其后的 SCM_RIGHTS，避免提前遗失所有权。

## 发送、等待和身份

发送先完成结构校验与实际 FD 数核对，才执行 `sendmsg(MSG_DONTWAIT | MSG_NOSIGNAL)`。FD 仅借用到调用结束，发送者保留其句柄。完整数据报成功交给内核后返回成功；接收和 owner 应用是另外两个完成点。异常的部分发送作为截断错误返回，不重发剩余字节。

connect、accept、send、recv 均使用同一绝对 `Instant` 截止点。EINTR 不更新截止点；EAGAIN 进入剩余时间有界的 poll。Unix connect 因 backlog 满返回 EAGAIN 时重试 connect；不将它当成连接已经建立。

每个连接缓存建连后的 `SO_PEERCRED`。独立子进程必须 exec 后主动连接 pathname，监督器核对未 reap 的真实 Child PID、UID、GID。父进程建立后继承给子进程的 socketpair 仍报告创建者身份，不能用于证明当前子进程身份。HELLO、身份登记、会话 epoch、nonce、feature、租约、业务权限和 FD 对应的资源头/访问权限由上层验证。

`monotonic_ns` 使用 `clock_gettime(CLOCK_MONOTONIC)`，检查 timespec 合法范围与 `u64` 换算溢出。跨进程比较要求同主机启动和同 Linux time namespace；该 API 不自行认证时钟 namespace。

## 目录与清理

`PrivateDirectory::create_in` 使用 getrandom 的 128 位随机后缀及 mkdirat(0700)。父目录归当前有效 UID 或 root 所有，且不可被外部写入，或具备 sticky 位。监听父目录只允许当前有效 UID 所有、组与其他用户不可写的目录。

bind 使用已打开父目录的 `/proc/self/fd/<fd>/<name>`，拒绝已有路径，不自动删除遗留 socket。文件权限只允许 0600 或 0660，不修改进程全局 umask。创建出的 inode 以 O_PATH FD 固定，chmod 操作该 inode，Drop 比较当前名字与固定 inode 后才 unlinkat；非空目录保留。launcher 串行管理同 UID 的目录改名，比较和删除之间没有原子性保证。

## unsafe 边界与平台

unsafe 仅用于 libc 调用、C 结构输出、辅助数据读写、以及新建/接收 FD 转移到 `OwnedFd`。每处保留 Safety 注释；所有指针都指向调用期间仍存活的缓冲区。运行库不借用内核继续异步访问的内存，也不将报文字节转型为 Rust wire struct。根 workspace 的 unsafe 禁止未放宽，本 crate 是独立低层边界。

Linux 以外生产入口返回 `UnsupportedPlatform`。本轮运行环境与真实 red/green 证据保存在 [验证目录](../../docs/validation/control-transport/README.md)。本轮 Linux 测试覆盖 aarch64；x86_64 上的实际运行资格、跨 UID/GID 部署、pidfd/holder registry 与远端 TLS 由后续集成完成。
