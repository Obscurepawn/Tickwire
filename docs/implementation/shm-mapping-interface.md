# 共享映射与原子访问接口

`tickwire-shm-sys` 管理 Linux memfd、映射、文件封印及 64 位原子访问。它为上层 ring 提供检查过范围的内存原语；槽位所有权、发布前缀、读者释放、资源身份和控制握手由上层维护。普通数据与原子位置的并发访问规则无法由通用 offset 推断，因此访问方法保留 `unsafe`，每个调用点说明同步依据。

生产入口只接受 Linux、小端 x86_64/aarch64、原生 64 位原子、4096 字节页。ABI 的 128 字节隔离仍要求部署检查实际最大缓存行；容器没有提供完整 cache line 拓扑时不据此宣称完成资格验证。macOS 返回 `UnsupportedPlatform`，不提供伪装成生产的文件后端。

```rust,ignore
WriterMapping::create(name: &str, len: usize) -> Result<WriterMapping>
WriterMapping::seal_for_readers(&mut self) -> Result<()>
WriterMapping::duplicate_fd(&self) -> Result<OwnedFd>
ReaderMapping::map(fd: OwnedFd, expected_len: usize) -> Result<ReaderMapping>

// 两类映射均提供：
len(&self) -> usize
is_empty(&self) -> bool
unsafe read_bytes(&self, offset: usize, len: usize) -> Result<&[u8]>
unsafe load_u64_acquire(&self, offset: usize) -> Result<u64>

// 仅唯一写者提供：
unsafe write_bytes(&mut self, offset: usize, bytes: &[u8]) -> Result<()>
unsafe store_u64_release(&mut self, offset: usize, value: u64) -> Result<()>
```

`create` 创建带 `MFD_CLOEXEC | MFD_ALLOW_SEALING` 的 memfd，设置固定长度，建立唯一 RW `MAP_SHARED` 映射，并设置 `MADV_DONTFORK`。新文件的零页用于初始化；映射和 FD 的生命周期由 RAII 管理。名称只用于诊断，不是资源身份。[共享映射](https://man7.org/linux/man-pages/man2/mmap.2.html)、[MADV_DONTFORK](https://man7.org/linux/man-pages/man2/madvise.2.html)

创建者同时是这版接口中的唯一写者。它先填好头部及初始原子值，再调用 `seal_for_readers` 设置并读回 `F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE | F_SEAL_SEAL`，即 `0x17`。只有成功封印后 `duplicate_fd` 才交付可供读者使用的 FD。已有 RW 映射继续有效，内核拒绝后续共享 RW 映射、文件写入、缩短及增长。[Linux memfd](https://man7.org/linux/man-pages/man2/memfd_create.2.html)、[文件封印](https://man7.org/linux/man-pages/man2/F_ADD_SEALS.2const.html)

`ReaderMapping::map` 消费收到的 FD，检查普通文件、精确长度、封印集合与平台，再建立 `PROT_READ` 映射。失败时关闭所消费的 FD。上层在发出 ReadReady 前继续验证 ResourceHeader、hash、身份、代次及 offer；此 crate 不发送 MapWriter/ReadReady，也不实现 `SCM_RIGHTS`、peer credentials 或跨进程写者授权。

映射不实现 `Clone`、`Send` 或 `Sync`，由单一线程持有，不暴露裸指针或 `AtomicU64` 引用。所有访问先检查加法溢出及范围；原子访问还检查 8 字节对齐。`load_u64_acquire` 只执行 64 位 Relaxed load，再执行 Acquire fence；写端执行 Release store。只读映射没有 RMW 或直接 Acquire load。[Rust 只读原子规则](https://doc.rust-lang.org/std/sync/atomic/index.html#atomic-accesses-to-read-only-memory)、[fence 同步条件](https://doc.rust-lang.org/std/sync/atomic/fn.fence.html)

调用者保证每个原子位置始终使用相同的 64 位原子访问，初始值在资源发布前设置。普通字节读取须已观察覆盖该记录的发布，且借用存续期间写者不能复用该槽位；普通写入须已取得排他的槽位写权限。复制到本地缓冲不会消除复制期间的并发改写风险。`&mut WriterMapping` 只限制本进程的 Rust 调用，跨进程排他性仍由 ring 的释放协议保证。

此接口不提供持久化、快照的 `0x0f` 不可变封印模式、内存锁定或故障后资源回收。Linux 容器测试覆盖当前内核与架构的实际系统调用行为；另一架构及目标生产机还需独立验证。
