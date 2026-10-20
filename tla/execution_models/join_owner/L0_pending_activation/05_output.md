# 输出

PASS：固定已认证身份、同一 bank、容量 64、一个活借用窗口和共同 JOIN 截止时刻的有限模型。主/两读者/wrap/wrap_two 四配置分别遍历 2181、321256、172、7652 个不同状态；发布窗口及可达前缀见 [抽象说明](01_reasoning.md)。

检查的性质是：pending 按原 S 保留门禁；双 READ_READY、真实 cursor 和已应用到当前 published 同时成立才激活；其他订阅改变 membership 不替换已有登记版本；活借用和未读记录保留原槽位序号；失败和超时不 detach；回收依赖已登记实际 cursor 的 Quiesced 状态及精确水位；历史 READY/Quiesce 成功重试不重新修改发布状态。

## 忠实度与覆盖

选定状态面由 23 个动作族构成。主配置和两读者配置包含其中 22 个，wrap 配置从明确可达前缀开始，包含 16 个；四配置的并集覆盖全部 23 个动作族。每个配置列入 Next 的动作均生成后继，完整计数见 [运行记录](03_run_log.md)。没有未解析源码行为混入动作；scope 外行为列在下段。1221 条源码行锚点覆盖所引用的 owner/consumer 与 SDK 范围，完整文件哈希同时记录。

生产操作以 `JoinOwner` 的 9 个流程入口和 `JoinConsumer` 的 8 个流程入口为分母：本级显式转录 14/17 个状态操作；cursor_offer、read_ready_frames、join_ready_frame 的无状态编码部分归约为已验证控制输入，真实字节、FD 和进程身份由 Linux 测试覆盖。以下仍未纳入：独立读者截止时刻、持续新登记、owner 崩溃恢复、代次重用、持久请求存储、pidfd/全部 holder 死亡回收、签名授权及 CPU 弱内存。底层大借用窗口缺口和缓存门禁来自既有 ring L0 的限定结果；本级不宣称完成形式细化或持续发布下的无条件活性。

root 独立审阅 SDK 新接口，risk_core 独立审阅 JOIN 源码和模型，确认 pending cursor、实际 Quiesced、历史结果以及共同 deadline 的边界。qseq==read 在本级从合格 SDK 发布 life5 前写入 qseq、之后停止 Borrow 导出；源码仍对实际映射逐字段检查。
