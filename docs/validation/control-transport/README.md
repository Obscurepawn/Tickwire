# 控制传输验证记录

`tickwire-control-sys` 在真实 Linux aarch64 内核上完成 15 个测试入口：11 个主场景、4 个由独立进程实际执行的 helper。最终格式检查、测试和 `clippy -D warnings` 均通过，完整输出见 [linux-final.log](linux-final.log)。macOS arm64 完成 1 个生产入口返回 `UnsupportedPlatform` 的测试及 clippy，见 [native-green.log](native-green.log) 与 [native-clippy.log](native-clippy.log)。

## Red → Green

| 阶段 | 实际结果与证据 |
|---|---|
| 接口后、实现前 | [linux-red.log](linux-red.log)：4 项均失败，入口返回 `UnsupportedPlatform`；当时源码摘要在 [red-source.sha256](red-source.sha256) |
| 初版实现 | [linux-first-green.log](linux-first-green.log)：4 项通过 |
| 进程与异常报文扩展 | [linux-extended.log](linux-extended.log)：10 个入口通过，含 fork/exec、独立 connect 和拒绝后的 FD 计数 |
| 独立源码审阅 | 发现 `create_in` 接受非 sticky、可被其他 UID 写入的父目录，创建与 pin 之间可能被换名；[linux-directory-red.log](linux-directory-red.log) 真实复现该准入缺口 |
| 修复与完整回归 | 父目录限制为当前 UID/root 所有，且不可外写或有 sticky 位；[linux-final.log](linux-final.log) 中 15 项、fmt、clippy 全部通过 |

独立审阅还核对了 cmsg 长度与对齐、内核交付 FD 的唯一所有权、截断后关闭顺序、SO_PEERCRED 快照、绝对 deadline，以及父目录修复。审阅者未运行本目录测试；执行结果来自上述实际日志。`linux-green.log` 是完整回归的中间记录；`linux-clippy.log` 记录初次发现镜像尚未安装 clippy，最终 runner 已包含安装步骤。

## 覆盖的行为

- 0、1、16 个 FD 的整帧收发，CLOEXEC/NONBLOCK 标志、发送者保留句柄、接收对象析构关闭句柄，以及收到的 FD 在 exec 后确实消失。
- 9 类错误各重复 32 次：短帧、错误 CRC、缺 FD、多 FD、重复索引、索引越界、payload 截断、辅助数据截断、零长且携带 FD。隔离进程逐次比较 `/proc/self/fd` 数量，288 次拒绝后均回到基线；每次错误后还能收到下一条完整合法报文。辅助截断实际发送 32 个 FD，覆盖内核关闭未交付 FD 与运行库关闭已交付 FD。
- 独立 exec 子进程主动连接 pathname，父进程读取的 peer PID 精确等于未 reap 的 Child PID；子进程读取到父进程 PID。双方 CLOCK_MONOTONIC 值处于父进程测得的前后边界。
- 真实 `fork` 后 `execve` 继承 socketpair，消息确实来自子进程，但父端 SO_PEERCRED 仍是创建者 PID。这是部署边界的正向证据，不通过修改 payload 身份规避。
- accept/recv 超时、发送队列满、EOF、反复 SIGUSR1 打断 poll 不延长 deadline、受限目录/socket 权限、拒绝已有路径、保留被替换路径与非空目录。

Linux 内核为 `6.8.0-117-generic`，页大小 4096，Rust 为 `1.99.1 (58cf8f9c3 2026-10-13)`。固定镜像、主机信息及受测源码摘要见 [environment.json](environment.json)、[source.sha256](source.sha256)。这是系统调用、协议边界和所有权测试，没有测量延迟收益，也没有授予 x86_64、跨 UID/GID 部署或 pidfd 生命周期的运行资格。

## 复现

在仓库根目录运行：

```sh
bash docs/validation/control-transport/run-linux.sh
```

[runner](run-linux.sh) 固定容器镜像 digest，独立使用 `target/linux-control`，运行 fmt、整个 crate 的测试及 clippy。它沿用仓库 Linux 检查的 registry cache 与工具链安装方式。接口与实现边界分别见 [控制传输接口](../../implementation/control-transport-interface.md) 和 [实现规格](../../../specs/implementation/control-transport.md)。
