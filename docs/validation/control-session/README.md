# 控制会话验证

实现与规格：[control-session.md](../../../specs/implementation/control-session.md)。

- `session-red.log`：先有 8 项测试和空模块，真实编译失败；随后实现会话。
- `registry-red.log`：加入请求存储与执行票据测试后的真实编译失败。
- `completion-red.log`：准确 StatusBody 与外部保存版本不匹配仍被接受的行为 Red；随后校验对应 opcode 与保存版本。
- `lease-bound-red.log`：期限算术耗尽错误分类的行为 Red；改为 Bounds，与实际到期的永久关闭区分。
- `native-green.log`：`cargo test -p tickwire-control`，13 passed；macOS 不执行 Linux integration。
- `linux-green.log`：固定 Rust 1.99.1 bookworm 镜像执行 `cargo test --locked -p tickwire-control`，13 项逻辑测试和 1 项实际 exec 子进程测试通过；1 个 helper 在主测试列表 ignored，由父进程明确启动并通过。
- `anchors-update.log`、`anchors-check.log`：L0/L1 共 384 个源码行锚点，检查无漂移。

Linux 环境和镜像与 [check-linux.sh](../../../scripts/check-linux.sh) 相同。综合测试通过 pathname socket 独立 connect，父进程始终保留 Child 句柄；实际 kernel PID/UID/GID 与 launcher 输入比较后才完成握手。两个进程继承同一 Linux time namespace。测试发布文件哈希是固定 fixture，生产 launcher 的签名/资格登记仍需集成。

父进程只登记并交付一次执行票据，保存版本 1 的完整响应；owner 当前版本改成 7 后重试，仍返回原有响应。MockRequestStore 在父进程内存中，测试不包含 owner 重启或真实持久后端。

L0 三配置与 L1 三配置都实际完成 TLC；动作覆盖、计数、源码和模型 SHA-256 见 [模型目录](../../../tla/execution_models/control_requests/ROADMAP.md)。ipc_codec 已独立审核源码与模型，未发现开放候选。Linux 传输和 WAL 有各自的 syscall/进程测试；这里没有将控制会话续租视为交易授权。
