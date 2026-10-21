# 共享映射实现规格与验证

`tickwire-shm-sys` 已实现单写 memfd 的 Linux 映射和原子访问原语。接口先于测试与实现确定；业务 ring 另行维护发布、槽位借用和释放。`unsafe` 只位于这个低层 crate 及上层 SDK 明确允许的私有边界，根 workspace 的禁止规则没有放宽。

创建顺序是 memfd → 定长零页 → RW `MAP_SHARED` → `MADV_DONTFORK` → 初始化 → 设置并读回 `0x17` seals → 允许导出 FD。读者验证普通文件、精确长度及完整单写 seal profile 后只建立 RO 映射。创建者就是此接口的唯一写者，跨进程委派写者的 MapWriter ACK 尚未实现。后续已加入[资源头身份校验](shm-ring.md)、[认证控制会话](control-session.md)与 [SCM_RIGHTS 控制传输](control-transport.md)；它们各自保留独立测试证据。

所有内存访问方法检查整数溢出与范围，原子访问另检查 8 字节对齐。普通读写和原子访问均标记为 `unsafe`：正确 offset 无法证明其他进程遵守槽位所有权或同宽原子协议。借用不能越过释放水位；复制操作也要求复制期间没有并发改写。映射不实现 Clone/Send/Sync，Drop 先 munmap，再由 OwnedFd 关闭描述符。

## 测试与实机证据

首次 Linux 测试在只有接口和测试的状态下失败：`Error/WriterMapping/ReaderMapping/SINGLE_WRITER_SEALS` 未定义，编译器报告 E0432。实现之后在真实 Linux 内核运行系统调用测试，没有用 macOS 文件后端替代 seals。

验证环境是 Docker/Colima Linux aarch64，内核 `6.8.0-117-generic`，guest 页大小 4096；Rust `1.99.1 (58cf8f9c3 2026-10-13)`。镜像 `rust:1.99.1-bookworm` 的已拉取 digest 为 `sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29`。

```sh
docker run --rm \
  --mount "type=bind,src=$PWD,dst=/work" \
  -w /work -e CARGO_TARGET_DIR=/work/target/linux \
  -e RUSTUP_TOOLCHAIN=1.99.1 \
  rust:1.99.1-bookworm cargo test -p tickwire-shm-sys

cargo clippy -p tickwire-shm-sys --all-targets -- -D warnings
cargo rustc -p tickwire-shm-sys --release -- --emit=asm
```

该阶段后两条也已在同一 Linux 镜像内执行。初始单写映射阶段 Cargo 报告 11 项通过：2 个 OS 单元测试、7 个映射集成测试、1 个独立进程测试及其 1 个子进程入口。子入口在常规测试枚举中不执行工作，由父测试通过独立 exec 启动后承担实际读者职责。

| 检查 | 已观察结果 |
|---|---|
| 创建与权限顺序 | 封印前不能导出 FD；封印后原 RW 映射继续发布，RO 读者看见更新 |
| 内核封印 | 新建共享 RW mmap、pwrite、增长、缩短、再加 seal 均失败 |
| 读者权限 | 新 RO 映射不能通过 mprotect 升级为 RW |
| 映射属性 | `/proc/self/smaps` 显示写者 `rw-s`、读者 `r--s`，两者 VmFlags 都含 `dc` |
| 长度与偏移 | 零长、非页倍数、usize 溢出、越界和未对齐原子地址均在访问前拒绝 |
| 资源输入 | 非普通文件、长度不匹配、未封印、逐个缺失所需 seal、误用不可变 `0x0f` profile 均拒绝 |
| FD 与初始化 | 新页全零；重复封印幂等；导出与收到的 FD 均设置 CLOEXEC |
| 实际多进程 | 子进程先完成 RO 映射并通知父进程；父写入 60 KiB 后 release 发布，子 acquire 后逐字节核验 |

多进程测试用 `/proc/<parent>/fd/<fd>` 完成测试专用 FD 交付；它没有验证生产控制传输或身份授权。Release 构建的 aarch64 汇编中，`ReaderMapping::load_u64_acquire` 的共享地址访问为 `ldr x9, [x9, x1]` 后接 `dmb ishld`；写端为 `stlr`，没有只读页 RMW。

macOS aarch64 单独验证生产创建入口返回 `UnsupportedPlatform`，并通过 clippy。Linux 与 macOS 的 clippy 均使用 `-D warnings`。

验证时 `src/lib.rs` SHA-256 为 `9f93a8bbc2b13127a8172f659f01375a66e07500bb83ed609c01d20a5aeb764b`；`tests/mapping.rs` 为 `b4f728da5da873c7f7f93ae3b43f1cfa192598d1eaa684664cd8f3fef28e82b3`；`tests/process.rs` 为 `a4ace7478b5fa27e84588c265b3f2c6495c36d2dde5b62a41efd0013163be4f5`。

## 尚未完成的资格与功能

当前结果覆盖上述 Linux guest 的映射、权限和发布可见性。Linux x86_64 尚未实机执行；目标生产机的缓存行、CPU/NUMA 隔离、锁页、首次缺页和延迟分位数尚未验收。Colima guest 没有导出 cache `coherency_line_size`；aarch64 glibc 的 L1 查询使用 CTR_EL0 最小行宽，因此本次不据此宣称已验证整个缓存层次的最大行宽。[glibc 实现](https://github.com/bminor/glibc/blob/master/sysdeps/unix/sysv/linux/aarch64/sysconf.c)

不可变对象的 `0x0f` 封印已在[不可变资源模块](immutable-resource.md)实现；消息截断和 FD 数量校验由[控制传输](control-transport.md)验证。跨进程写者交接、pidfd 生命周期和恶意写者隔离仍待接入。[固定记录 ring SDK](shm-ring.md)已提供 guard 生命周期与实际 cursor 验证，完整生产进程仍需连接上述组件。memfd 没有持久化承诺，映射测试不替代 WAL/复制恢复验收。
