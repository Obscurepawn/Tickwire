# 控制通道单次非阻塞轮询

MD、Host 和 SDK 原先用 `recv(now + 1µs)` 检查控制包。源码在分配 64 KiB 缓冲后才核对期限，因此该写法可能在调用 recvmsg 前就返回 Timeout。新 `try_recv` / `try_accept` 不经过期限检查，单次尝试内核队列；调用方迁移由相应模块完成。

Linux 实测使用固定镜像 `rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29`，Linux 6.8.0-117-generic aarch64，Rust 1.99.1。为避开同期全仓构建，复制 control-sys 和 IPC 两个 crate 到独立 workspace，使用 `target/linux-control-try`，容器限制一个 CPU。源码清单见 `source-hashes.json`，依赖锁见 `isolated-Cargo.lock`。

- `linux-red.log`：编译通过，三个新测试全部失败。Red 接口暂委托原 deadline API，使用 `Instant::now()`；`red-api.patch` 可从 Green 还原该版本。这验证了新接口需要真正尝试队列，不把这份测试说成 F 场景每次超时的原因证明。
- `linux-green.log`：18 项控制传输测试通过，含三个新测试。排队连接、0/1/16 FD、CLOEXEC、空队列、EOF 前排空，以及九种畸形报文各 32 轮的逐次 FD 数检查均通过。原阻塞期限、EINTR、fork/exec 身份与路径清理测试一并通过。
- `linux-clippy.log`：本 crate all-targets Clippy `-D warnings` 通过。第一次容器没有 Clippy 组件，失败信息单独保留；安装组件后重跑。
- `native-green.log`：macOS 的 UnsupportedPlatform 测试通过。`cargo fmt -p tickwire-control-sys -- --check` 通过。

容器内执行的测试与检查命令：

```sh
cargo test --offline -p tickwire-control-sys --test transport try_ -- --nocapture
cargo test --locked --offline -p tickwire-control-sys -- --nocapture
rustup component add clippy
cargo clippy --locked --offline -p tickwire-control-sys --all-targets -- -D warnings
```

新接口的 EINTR 分支返回系统错误，不在内部循环；本次没有用信号时序强行触发单次非阻塞调用的 EINTR。原有阻塞接口的信号中断期限测试继续通过。此目录只记录底层行为，完整 preflight-v2 切换由后续独立进程场景复验。
