# L0 输出：单段批同步与恢复 — PASS

primary、batch、scaling 分别探索 **1,816 / 2,695 / 45,883** 个不同状态，完整深度 18/20/28；每组全部 16 个动作生成后继，8 项不变量通过。无开放 candidate。原始结果见 [运行记录](03_run_log.md)。

单 writer 在完整批同步之后返回物理收据。写入/同步失败后关闭旧 writer；收到精确可信前缀时，恢复先验证该前缀，再丢弃最后未封口尾部并重新同步。模型保留实际 payload 身份与完成同步历史，因此收据性质核对内容，而非仅比较计数器。校验或可信前缀失败时保留原始证据。

普通崩溃性质建立在成功同步字节保留的存储契约上。额外媒体故障删除完整尾部时，有可信前缀可检出；没有前缀时完整较短历史与尾丢失无法区分。没有给出无条件耐掉电或 LocalCommitProof 的结论。

## Fidelity & coverage

- Fidelity: 16 transitions modeled, all cited file:line; 0 UNCLEAR/TODO；具体抽象见 [01](01_reasoning.md)。root 完成模型独审并确认 write/sync/receipt、poison、前缀验证和恢复同步与源码一致。
- Code coverage: modeled 8 of 8 本层选定分支：追加输入检查、顺序写入、写后同步、收据发布、poison 拒绝、恢复验证、尾部截断、恢复同步。分母是固定段内的这些决策分支，不是全部文件系统实现或二进制行覆盖。
- Model coverage: TLC --coverage — all actions fired in all three configurations；计数由原日志解析。

每批记录数固定为 1 或 2，内容以完整帧 token 为粒度，最多 3 次调用和 2 次崩溃。CorruptFrame 只在已封口批内注入完整帧损坏；字节级 torn header、padding、CRC/hash、序号洞和 epoch 回退由 Rust 测试验证。段切换、manifest 目录事务留给 L1；文件锁、密码学抗碰撞和设备掉电不由本模型证明。

本层向 L1 导出：固定段内的成功收据覆盖完整同步批；已验证的最后未封口尾部可截断；外部前缀失败不修改日志。L1 仍需独立验证新段建立、目录同步和 manifest rename 的崩溃组合。
