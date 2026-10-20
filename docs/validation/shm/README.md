# 共享内存验证证据

`./scripts/check-rust.sh` 在 macOS aarch64 通过 202 项测试；`./scripts/check-linux.sh` 在 Linux aarch64 通过 223 项测试。两个数字均包含 1 项引用生命周期编译失败 doctest；Linux 数字包含两个子进程测试辅助入口。格式检查和 Clippy 均通过。

| 文件 | 证据 |
|---|---|
| [protocol-red.log](protocol-red.log) | `tickwire-shm` 状态接口实现前的编译失败 |
| [mapped-detach-red.log](mapped-detach-red.log) | 伪造纯状态 Quiesced 证明可绕过实际 cursor 的真实失败 |
| [features-red.log](features-red.log) | 必选特性或代次边界未被拒绝的真实失败 |
| [quiesce-red.log](quiesce-red.log) | 重复 Quiesce 将已完成 cursor 降回 Quiescing 的真实失败 |
| [linux-workspace-green.log](linux-workspace-green.log) | 当前全部 Linux Rust 测试、doctest、Clippy 结果及内核/工具链信息 |
| [../rust-tests.log](../rust-tests.log) | 当前 macOS 全 workspace 验证输出 |
| [environment.json](environment.json) | 容器摘要、命令、测试计数及 SHM 源文件 SHA-256 |

Linux `tickwire-shm-sys` 有 11 项测试（其中 1 项子进程辅助入口），检查封印、只读权限、精确大小、对齐/边界、映射标记和真实跨进程原子发布。`tickwire-shm` 有 13 项纯协议测试、11 项映射测试（其中 1 项子进程辅助入口），另有 1 项 doctest。广播集成在一个测试中依次运行 1、2、4、8、16、32 个独立消费者，每个校验 1,024 条完整记录。

sys 初次 TDD 的失败/通过输出在会话工具结果中，未单独保存为日志；本目录没有补写或重构该历史输出。当前 Linux green 是重新完整执行所得。这里的真实进程测试检查传输与回收正确性，不作为延迟或生产资格数据。

独立审阅还确认 guard 字段保存 `&[u8]` 的调用期别名约束问题，已改为 `NonNull<[u8]>` 与按方法借用生成的短引用。该修复依据源码及 Rust 语言契约，未得到实际误编译或 Miri 反例；本目录 green 对应修复后的源码，TLC 覆盖发布/释放协议而非编译器别名规则。依据与具体调用场景见[ring 规格](../../../specs/implementation/shm-ring.md)。
