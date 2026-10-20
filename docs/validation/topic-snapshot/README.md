# Topic 快照接收验证

接收校验按[接口](../../implementation/topic-snapshot-interface.md)先编写测试与可编译占位 API，再实现。初始 [Red](red.log) 为 6 项行为失败、1 项截断拒绝通过；失败原因是占位入口返回 `UnsupportedOperation`，不是编译错误。

多标的用例随后发现两个不同成员可以使用相同 snapshot ID；全部结构和摘要都合法时，初版包装仍返回成功。[重复身份 Red](member-identity-red.log) 保留该结果。修复后用有界 ID 集合检查非零及文件内唯一性，顶层也参与冲突检查。

[Green](green.log) 记录 12 项纯 Rust 回归，[Clippy](checks.log) 使用 `-D warnings`。测试包括原 12 KiB 完整基准样本、两个成员的 16 KiB 合法文件、资源/JOIN/顶层身份冲突、每个成员的期限、记录来源与 FeedState 一致性、缺少必需状态仍保留成员，以及全部 0–12287 字节截断输入。身份冲突和时间冲突用例先重新计算所有相关 hash，并单独通过原 IPC 结构校验，再断言业务包装拒绝。

独立审阅已对照 IPC 的完整集合/S/hash/连续 section 校验，检查包装的 r/s/j 身份、记录 topic/producer generation、最早截止时间和缺状态保留行为，没有发现阻断项。根据审阅补充了带有效标志的未来 ingress/publish 时间，以及必需 Quote 自身非 Live 的负测。

```sh
cargo test -p tickwire-snapshot
cargo clippy -p tickwire-snapshot --all-targets -- -D warnings
cargo fmt -p tickwire-snapshot -- --check
```

Linux 另有[真实 SCM_RIGHTS 与独立 exec 集成](linux-integration.md)：父进程关闭源映射/FD 后，子进程仍成功映射和验证；两个坏资源正确拒绝，每轮 FD 与映射恢复基线。该集成具有 2 个测试入口，其中 1 个为父测试实际运行的子进程 helper。最终全仓 macOS/Linux 检查见[阶段验证](../snapshot-journal-stage/README.md)。

纯 Rust 校验结果只证明字节与指定上下文一致。OS seals、受信上下文来源、实际 cursor、资源会话及交易准入分别由所属组件检查；完整 JOIN 协调器仍在后续范围中。
