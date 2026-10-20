# L0 输出（已冻结）

五个配置全部 PASS。物理容量均为 64，最大配置穷举 930,364 个不同状态、深度 33；三个可达满环前缀配置覆盖写入 65/66 时的槽位复用，包含两个读者共同限制回收的场景。没有开放 candidate。

验证的安全条件包括：发布时整批 payload 完整；活跃借用和未读记录的实际槽序号保持正确；cached min 始终保守；pending JOIN 仍参与 gate；released 和 applied 各自连续推进；Freeze 不跨未完成 claim；移除成员前真实已登记 cursor 已 Quiesced，水位证据精确匹配且无活跃借用。

结论适用于 [reasoning](01_reasoning.md) 列出的固定身份、有限输入和原子可见性契约。停顿和遗失 guard 保留回收阻塞，本级不声明最终进度。身份代次、FD 认证、进程退出、bank 轮换、arena、生产数值耗尽以及 CPU 弱内存行为未在本级穷举。Rust/Linux 多进程实测属于独立证据。

## Fidelity & coverage

- Fidelity: 17 transitions modeled, all cited file:line; 0 UNCLEAR/TODO within this level; abstractions listed in [01_reasoning.md](01_reasoning.md)。其中 RejectForged 是错误返回的历史见证；claim serial 和真实 Release/Acquire 按文档契约抽象。
- Code coverage: modeled 14 of 14 selected protocol mutation methods in `crates/tickwire-shm/src/lib.rs`：join、join_ready、observe_release、claim、publish、abort、prepare_freeze、freeze、detach、borrow_next、release、apply、start_quiesce、quiesced；并转录 SDK register_cursor、publish、detach 的关键内存/证据边界。分母是指定方法，属于设计行为覆盖，不是 Rust 行覆盖，也不包括构造器、身份错误分支或完整控制面。
- Model coverage: TLC --coverage — all actions fired。primary/two_readers 各 17 个转换，pending_wrap 16 个，wrap/wrap_two 各 15 个；前缀已完成的 JOIN/ReadReady 使用专门 Next 排除。全部配置九个 invariant 通过，逐动作计数与原始日志见 [03_run_log.md](03_run_log.md)。

risk_core 已独立核对模型、源码 guard 与抽象范围；未发现安全反例或漏掉的关键 guard。

## 向后续级输出

后续级可以在本级同等身份与内存顺序前提下沿用 `NoOverwriteBorrow`、`UnreadProtected`、`ConservativeCache`、`MemberGate`、`CompleteBeforePublication`、`DetachedSafe` 和 `FreezeBoundary`。扩展到身份重用、旧 cursor 或进程退出时，需要重新检查这些前提是否由控制面维持。
