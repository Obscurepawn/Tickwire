# L0 结果

三个配置全部 PASS，没有开放的源码候选。不同状态数分别为 52,700、748,060、125,574；完整统计和执行命令见 [03_run_log.md](03_run_log.md)。每个配置检查 16 项不变量，Init 与 42 个 Next 动作全部触达。

在固定基线、一个日志所有者和成功同步不丢失的存储契约下，本模型检查了以下行为：

- 基线、bootstrap 和物理日志按依赖顺序建立；追加成功收据引用已同步的完整批次。
- 追加失败封闭当前日志对象，公开位置与历史前缀保持不变。相同 Admit 重试不增加记录，冲突重试会 poison。
- 未返回收据的完整尾批可以被恢复；末尾不完整批次被丢弃。required 精确前缀和物理 writer 检查均先于截尾。
- 恢复先检查完整结构，再为每条 Admit 核验该记录与其前序历史，最后重放账户。闭包对象没有提供日志副本。
- AdmissionImage 错误、Aborted 命令摘要错误或缺失闭包均阻止恢复对象返回。重复 Aborted 增加 WAL 序号，但不重复释放本金或增加账户账本序号。
- 返回账户始终冻结，未中止订单标记为 unknown；恢复日志拒绝所有 Admit，允许继续记录引用原订单代次的 Aborted。

追加收据本身不检查账户计算和授权。模型允许调用者写入可编码但不能重放的记录，并检查恢复拒绝；发送协调由后续 AccountGateway 模型承担。

## Fidelity & coverage

- Fidelity: 42 transitions modeled, all cited file:line; 0 UNCLEAR/TODO within the selected scope; abstractions listed in [01_reasoning.md](01_reasoning.md).
- Code coverage: modeled 13 of 13 selected API branch groups in `crates/tickwire-account-journal`, as listed below; this is design-surface coverage, not executed Rust line coverage.
- Model coverage: TLC --coverage — all 42 Next actions and Init fired in all three configurations.

13 组检查面为：创建时拒绝已有文件；依赖文件发布与失败；新记录追加；精确 Admit 重试；冲突 Admit；poison/恢复态/容量拒绝；写入、同步及收据发布；恢复配置和物理入口；required 与截尾重同步；完整结构校验；逐条闭包及错误；账户 image/Abort 重放；冻结后返回。

有限记录只有 Admit 和 Aborted，原订单属于一代，恢复 writer 可提升到下一代。账户为明确的 Spot 数量 1、限价 1、零费用实例。SHA-256 用无碰撞符号表示；物理批次接口是本级输入契约，未重新证明字节编码、分段切换、manifest 事务或设备掉电行为。

root 与 ipc_codec 已独立对照冻结源码审阅模型，无开放候选。初模的目录同步反例被源码否定，归因为模型遗漏，记录在 [04_counterexamples.md](04_counterexamples.md)。582 个源码行锚点校验零漂移，最终文件哈希保存在 [results.json](results.json)。
