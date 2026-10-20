# Linux 不可变快照跨进程交付

[独立集成测试](../../../crates/tickwire-snapshot/tests/immutable_linux.rs) 在固定 Linux aarch64 容器中通过，日志为 [linux-integration.log](linux-integration.log)。两个测试入口包含一个主场景和一个子进程 helper；主场景确实 exec 新进程执行 helper。`tickwire-snapshot --all-targets` 的 clippy `-D warnings` 同时通过，输出见 [linux-clippy.log](linux-clippy.log)。

父进程在 0700 私有目录中监听 Unix pathname SOCK_SEQPACKET，持有尚未回收的 Child，并对 accept 后的 SO_PEERCRED 核验精确 PID、UID 和 GID。子进程独立 connect，反向核对父 PID。资源经 SCM_RIGHTS 传递；父关闭本次资源的全部映射和 FD 后发送 barrier，子收到后才映射并验证，因而成功读取不依赖父仍持有源引用。

子进程在接收前，从编译进测试的基准数据固定 expected ResourceHeader、JOIN hash、目录 hash 和标的能力集合。入站描述符与这些预期核对；文件内容不会被用于自填 expected identity/hash。有效资源依次经过 `ImmutableMapping::map`、`validate_resource`、`tickwire_snapshot::validate` 和 `check_readiness`，最终核对完整状态行、深度、共同截面及覆盖标志。

同一真实连接还交付两个已经正确封印的坏文件：一个保留资源头但改动子对象内容，另一个修改资源 ID。前者返回 Checksum，后者返回 Identity。每轮结束时，子进程的 FD 数回到基线，`/proc/self/maps` 中也没有该资源映射；成功和失败分支均覆盖回收。

本次集成第一次运行即通过，没有为已有组件人为制造 Red。不可变资源自身的真实 Red→Green 记录在[底层验证归档](../immutable-resource/README.md)。独立二审已完整阅读新测试，确认真实 exec、SCM_RIGHTS、关闭源引用的 barrier、受信 fixture 来源和回收断言与上述说明一致，未发现阻断项。

测试使用 fixture 的固定时间 `1_500_000_000` 校验快照期限，socket 操作另用实际 Instant 绝对期限限制等待。它验证封印资源交付与业务接收校验；HELLO/WELCOME 会话、已认证 offer 登记、owner pending gate、实际 cursor ReadReady、JOIN_READY 和增险准入仍由后续业务协调器集成。测试中的 HELLO 格式包仅充当传输 barrier，未将它视作会话认证结果。

两个日志均保留实际命令、镜像完整摘要和原始输出。镜像为 `rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29`，构建目录为 `target/linux-snapshot`。受测测试文件 SHA-256 为 `ec59d466c83cec7e9770f0da4f9289ec4f075d50284e317b19e826ab105e42f0`；其他源码与最终全仓验收由本目录主记录统一归档。
