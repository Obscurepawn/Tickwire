# 不可变资源验证记录

Linux aarch64 上 `tickwire-shm-sys` 共 21 个测试入口通过：原有 11 个与新增 10 个。新增部分包含 9 个主场景、1 个由独立进程实际运行的拒绝 helper。依赖它的 `tickwire-shm` 24 个测试和 1 个 compile-fail doctest 同时通过；两个 crate 的 clippy 与 sys 的 fmt 检查通过。完整输出在 [linux-final.log](linux-final.log)。

macOS arm64 的两个生产入口拒绝测试通过，见 [native-green.log](native-green.log)；native clippy 使用 `-D warnings`，见 [native-clippy.log](native-clippy.log)。

## TDD 证据

| 阶段 | 实际结果 |
|---|---|
| API/测试先行，占位入口 | [linux-red.log](linux-red.log)：9 个场景失败，1 个常规枚举的 helper 通过；失败均到达真实 Linux 测试代码，入口返回 UnsupportedPlatform。初始源码摘要见 [red-source.sha256](red-source.sha256) |
| 首轮实现 | [linux-first-green.log](linux-first-green.log)：sys 21 项通过 |
| epoch 上限补查 | [linux-sequence-red.log](linux-sequence-red.log)：将正确头的 channel_epoch 改为 `2^63`，重新计算头与对象 hash，错误地被接受，case 7 失败 |
| 最终修复回归 | ResourceHeader 的身份计数统一检查 `<2^63`；[linux-final.log](linux-final.log) 全部通过 |

测试单独让现存 RW MAP_SHARED 导致 F_SEAL_WRITE 返回 EBUSY，munmap 后才成功添加 `0x0f`。另将导出的只读 FD 重新打开为 O_RDWR 再尝试 pwrite、扩缩容和 RW mmap，确认失败来自内核 seals，而非仅由 FD 访问模式阻止。safe 字节读取、导出 FD 的 O_RDONLY/CLOEXEC、接收 O_RDWR 后降为只读、越界与溢出、错误类型/长度/seals、头部身份/摘要、整文件摘要、零页尾及资源上限均有断言。

隔离进程重复 64 次三类失败：错误 seal、长度不符、资源验证失败。每次 `/proc/self/fd` 数回到基线；资源验证失败后对应 `memfd:immutable-rejection` 映射也从 `/proc/self/maps` 消失。此检查同时覆盖成功 mmap 后再拒绝的清理路径。

独立二审已阅读 `immutable.rs`、`resource.rs`、复用的 Mapping/平台方法及测试，未发现阻断项。审阅核对了完整 seal 锁定后长度二查、O_RDWR→O_RDONLY 转换期间原 FD 存活、pwrite 短写/EINTR/零写、安全切片借用期，以及 content_end/page-align 不越界。测试结果来自上述执行日志，二审本身是源码检查。

## 复现与资格

```sh
bash docs/validation/immutable-resource/run-linux.sh
```

[runner](run-linux.sh) 固定镜像 `rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29`，使用独立 `target/linux-immutable`，执行环境输出、fmt、sys/ring 全部测试及 clippy。Linux 内核为 `6.8.0-117-generic`、aarch64、4096 页，Rust 为 `1.99.1 (58cf8f9c3 2026-10-13)`。

环境与受测源码摘要见 [environment.json](environment.json) 和 [source.sha256](source.sha256)。本轮未测 x86_64、跨 UID/GID 部署或延迟分位数；资源内容被封印也不构成 JOIN Ready 或磁盘持久化证据。层间职责见[接口](../../implementation/immutable-resource-interface.md)与[实现规格](../../../specs/implementation/immutable-resource.md)。
