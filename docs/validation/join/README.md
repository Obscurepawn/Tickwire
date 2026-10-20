# JOIN 验证记录

Linux 使用固定镜像 `rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29`，独立 target 为 `target/linux-join`。以下均为实际执行记录，没有把预期结果写成日志。

| 阶段 | 原始日志 | 结果 |
| --- | --- | --- |
| SDK pending 接口 stub | [shm-pending-red.log](shm-pending-red.log) | 编译通过，6 项行为测试失败 |
| SDK 实现及既有回归 | [shm-pending-green.log](shm-pending-green.log) | 新增 pending 与既有 mapped/protocol 通过 |
| owner stub | [owner-red.log](owner-red.log) | 4 项中 2 失败、2 通过 |
| owner 首轮实现 | [owner-first-green.log](owner-first-green.log) | 4 项通过 |
| flow 重复结果 | [flow-red.log](flow-red.log) | 5 项中 2 失败：激活后重试重新追平、冲突重复 ACK 被接受 |
| flow 修复 | [flow-green.log](flow-green.log) | 5 项通过；后来增加另一订阅 membership/gate 用例 |
| 三进程首次运行 | [process-first-run.log](process-first-run.log) | 测试误用 JOIN_READY 响应 opcode；IPC 按契约拒绝，随后测试改用 STATUS |
| 完整 JOIN Green | [linux-green.log](linux-green.log) | 11 项顶层测试通过；另实际执行两次独立 exec helper，均通过 |
| 最终 Linux 回归与 Clippy | [linux-final.log](linux-final.log) | JOIN、SHM 及借用 compile_fail 测试、Clippy 通过 |
| native Clippy | [native-clippy.log](native-clippy.log) | macOS 检查通过；真实 Linux 测试由条件编译排除 |

`shm-red-source.sha256`、`owner-red-source.sha256` 和 `flow-red-source.sha256` 记录取得 Red 时的源码。后两项重试缺陷由源码审阅和 Rust 测试发现，TLC 没有参与发现。`linux-clippy.log` 保留此前对测试 chunks API 的 Clippy 提示；最终通过记录在 linux-final.log。

复现最终 Linux 检查（从仓库根目录）：

```sh
docker run --rm --init \
  --mount "type=bind,source=$PWD,target=/work" \
  --mount "type=bind,source=$PWD/.tools/linux-cargo-registry,target=/usr/local/cargo/registry" \
  -w /work -e CARGO_TARGET_DIR=/work/target/linux-join \
  -e RUSTUP_TOOLCHAIN=1.99.1 \
  rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29 \
  bash -c 'set -eu; rustup component add clippy --toolchain 1.99.1; cargo test -p tickwire-join -p tickwire-shm; cargo clippy -p tickwire-join -p tickwire-shm --all-targets -- -D warnings'
```

root 独立审阅新增 SDK pending/activate/detach 接口。risk_core 独立只读审阅完整 JOIN owner/consumer 及三进程测试，确认了身份绑定、两项 READ_READY、原 membership V、READY 历史结果、真实 cursor 证据和 M 个累计登记上限，未发现阻断项；reviewer 未重复运行本目录测试。身份认证和远端遵守 SDK 内存协议分别是控制与 unsafe 合同的前提。

三进程测试中的 consumer 预期 header/JoinBody 由可信 launcher 提供，未实现常驻 Supervisor 分发服务；RequestStore 使用 MockRequestStore。测试证明实际 Linux 共享资源和控制接线的行为，未把这些测试替代完整生产 JOIN 控制面。

最终源码及接口依赖的 SHA-256 见 [source.sha256](source.sha256)，运行配置见 [environment.json](environment.json)。JOIN 有限模型四配置结果及范围见 [L0 输出](../../../tla/execution_models/join_owner/L0_pending_activation/05_output.md)。
