# Linux 控制传输接口

`tickwire-control-sys` 用本机 Unix `SOCK_SEQPACKET` 传输一个完整 ABI 控制帧及其 `SCM_RIGHTS`。每帧最多 65,536 字节和 16 个 FD，envelope 固定 128 字节。成功接收只表示帧结构与 FD 编号关系有效；会话身份、协商 feature、HELLO/WELCOME、租约、幂等与授权由上层控制会话验证。

```rust
pub struct PeerCredentials { pub pid: u32, pub uid: u32, pub gid: u32 }
pub fn monotonic_ns() -> Result<u64>;

impl PrivateDirectory {
    pub fn create_in(parent: &Path, prefix: &str) -> Result<Self>;
    pub fn path(&self) -> &Path;
}
impl ControlListener {
    pub fn bind(path: &Path) -> Result<Self>;
    pub fn bind_with_mode(path: &Path, mode: u32) -> Result<Self>;
    pub fn accept(&self, deadline: Instant) -> Result<Connection>;
}
impl Connection {
    pub fn connect(path: &Path, deadline: Instant) -> Result<Self>;
    pub fn peer_credentials(&self) -> PeerCredentials;
    pub fn send(&mut self, bytes: &[u8], fds: &[BorrowedFd<'_>],
                deadline: Instant) -> Result<()>;
    pub fn recv(&mut self, deadline: Instant) -> Result<ReceivedFrame>;
}
impl ReceivedFrame {
    pub fn bytes(&self) -> &[u8];
    pub fn fds(&self) -> &[OwnedFd];
    pub fn into_parts(self) -> (Vec<u8>, Vec<OwnedFd>);
}
```

发送前调用现有 `ControlFrame::decode`，并核对实际 FD 数。发送借用 FD，发送者始终保留自己的描述符；内核为接收方建立独立描述符。接收使用 `MSG_CMSG_CLOEXEC`，先将内核交付的全部 FD 纳入 `OwnedFd`，再检查数据/辅助数据截断、cmsg 类型和长度、CRC、完整 body、FD 数及索引唯一性。失败丢弃整帧，RAII 关闭该帧全部已交付 FD；内核关闭因辅助缓冲不足而未交付的多余 FD。`ReceivedFrame` 只有私有字段，不提供可绕过校验的构造器。[Linux unix(7)](https://man7.org/linux/man-pages/man7/unix.7.html)、[recvmsg(2)](https://man7.org/linux/man-pages/man2/recvmsg.2.html)

socket 和 accept 原子设置 CLOEXEC/NONBLOCK；收发再使用非阻塞标志，`poll` 根据同一个绝对 `Instant` 截止点等待。EINTR、EAGAIN 均重新计算剩余时间，不续期。发送使用 `MSG_NOSIGNAL`；成功意味着该完整数据报已交给本机内核，不表示接收方已应用。[accept4(2)](https://man7.org/linux/man-pages/man2/accept.2.html)

`monotonic_ns` 直接读取 `CLOCK_MONOTONIC`，供同主机、同 Linux time namespace 的进程比较应用 deadline；`Instant` 只用于本进程阻塞预算。launcher 保持这些进程在同一个 time namespace，因为该 namespace 可为单调时钟设置不同偏移。该时钟不计系统 suspend 时间，部署中的休眠处理由上层健康检查关闭相关风险门。[clock_gettime(2)](https://man7.org/linux/man-pages/man2/clock_gettime.2.html)、[time_namespaces(7)](https://man7.org/linux/man-pages/man7/time_namespaces.7.html)

`PrivateDirectory` 在指定父目录中创建随机名称、0700 权限的目录，保存目录 FD 与 inode 身份。父目录须由当前有效 UID 或 root 拥有，且组/其他用户不可写；如允许外部写入，则必须有 sticky 位，例如通常的 `/tmp`。socket 始终位于创建出的私有目录或部署方提供的受限目录内。监听默认 socket 权限 0600；`bind_with_mode` 另允许 0660，用于已由部署方设置获权组和目录遍历权限的部署。监听父目录必须归当前有效 UID 所有，且组/其他用户不可写；绑定通过已打开目录的 `/proc/self/fd` 路径完成，已有路径一律失败，不先 unlink。

同 UID 的 launcher 和其他受信进程必须串行管理这些目录项。清理通过 pinned inode 与当前名称的 device/inode 比较后执行 `unlinkat`，二者不是原子 compare-and-unlink；该接口不允许同 UID 进程在创建、绑定或清理期间并发改名。已在清理前被替换的路径会保留，非空目录不会递归删除。目录权限隔离其他 UID，同 UID 恶意进程不在该命名空间管理契约内。[unlinkat(2)](https://man7.org/linux/man-pages/man2/unlink.2.html)

`peer_credentials` 是 `SO_PEERCRED` 的连接建立时身份快照。继承父进程创建的 socketpair 后，即使子进程 exec 并发送消息，peer PID 仍可能是创建 socketpair 的父进程；因此独立进程先 exec，再主动连接受控 pathname，由父进程持有未 reap 的 `Child` 并核对实际 PID。这里不把可复用的裸 PID 当作长期退出证据，pidfd/holder registry 属于后续控制面。[SO_PEERCRED](https://man7.org/linux/man-pages/man7/unix.7.html)

Linux 之外返回 `UnsupportedPlatform`。传输层不修改现有 codec，也不授予映射或交易权限。
