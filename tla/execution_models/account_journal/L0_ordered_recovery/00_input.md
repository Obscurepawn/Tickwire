# 输入与范围

目标是冻结的 `tickwire-account-journal` 四个核心文件，采用 faithful fault injection。一个账户日志实例独占可变调用，文件后端持有单写锁；创建、追加和恢复不会与另一个 API 调用交错，进程崩溃可以发生在内部阶段之间。

源码入口：`crates/tickwire-account-journal/src/lib.rs:125-151` 创建、`:153-243` 恢复、`:245-290` 追加；`crates/tickwire-account-journal/src/backend.rs:38-66` 文件发布；`crates/tickwire-account-journal/src/identity.rs:36-139` 身份和代次；`crates/tickwire-account-journal/src/history.rs:17-59` 前缀重建。底层 `Store` 的完成义务见 `crates/tickwire-journal/src/store.rs:6-18`。

每条模型记录保留 kind、原意图 ID/代次、writer 代次、日志序号、基线/账户绑定、完整输入的内容选择和 AdmissionImage 的账本序号。新 Admit 属于 writer 1；接管后 writer 可为 1 或 2，但恢复日志只追加引用旧代次的 Aborted。相同 Admit 重试不追加；重复 Aborted 仍保存在 WAL，而账户只中止一次。

有限账户实例为 Spot 双资产、每笔限价单数量与价格均为 1、费用为 0、报价余额为 Capital、策略与毛敞口上限充分大。每个意图使用独立 client ID，期限有效；所有订单未收到交易所 ACK 或成交，故首次正确 Aborted 可释放 1 单位本金。模型只用这两类真实恢复输入，不把其它六类记录的金额语义列为本级覆盖。

模型容忍写入/同步错误、同步成功后回执丢失、进程退出和未完成尾部。成功同步的历史不会在后续崩溃中丢失。另从预先存在的错误记录开始检查恢复拒绝：错误顺序、代次回退、未来原意图、重复 Admit 和身份错配。这些是恢复输入校验场景，不是成功提交后篡改介质的故障声明。

不覆盖物理字节编码、SHA-256 碰撞、分段切换、manifest 目录事务、目录创建的父级同步、设备掉电、真实镜像、授权配置或 Gateway 发送。完整记录来自 WAL，闭包只提供对应命令与历史前缀的可信绑定。
