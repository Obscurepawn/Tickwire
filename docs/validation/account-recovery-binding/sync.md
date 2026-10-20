# 恢复闭包模型同步审查

`recovery_closure/L0_publish_verify` 的持久化顺序仍与当前源码一致。此次只更新源码锚点、模型注释与范围说明，没有运行 TLC，也没有修改状态、动作、性质或配置。

## 比较与范围

原 SHA256SUMS 中的 `lib.rs` 对应提交 `868980474de3e77708dd4b9fce0bcab26ff2eac3`。从该版本到当前源码，除本次种类 14/15 外，还包括此前种类 13 的完整命令扩展。

| 变化 | 源码结果 | 本级模型范围 |
| --- | --- | --- |
| 新增命令、账户基线及账户历史种类 | 13 最多一个；14/15 成对各一个并要求 13 | shape 检查只有成功/拒绝出口，未证明具体编号及数量规则 |
| VerifiedClosure 新摘要字段/getter | 从已验证且同步的 manifest 对象描述符取得摘要 | Deliver 仍只表示完成同步后交付能力，未表示字段内容或账户语义 |
| publish/verify/check | 逐对象读取、校验及重新同步，再同步 manifest、binding，最后返回 | 原顺序、错误出口和单所有者拓扑保持不变 |
| FileBlobStore | 与原源码逐字相同 | 文件同步、rename、目录同步、失败及崩溃边界保持不变 |

账户基线、完整命令和准确历史前缀的跨字段绑定由实际 Rust 接口/回归验证，不能由本级历史 PASS 推导。新增配对规则也不等于每个授权提供者都正确验证业务内容。

## 原件与当前证据

更新前完整目录、anchors、运行结果和日志保存在 [pre-sync](pre-sync/recovery_closure/L0_publish_verify/00_input.md)。原源码为 [lib.rs.txt](pre-sync/lib.rs.txt) 和 [files.rs.txt](pre-sync/files.rs.txt)，归档内容摘要见 [archive-hashes.json](pre-sync/archive-hashes.json)。归档 SHA256SUMS 原样保留，不用于冒充当前源码哈希。

[旧锁检查](anchors-before.log) 报告 98 行变化；人工按函数与错误出口复核后，使用独立临时工作区更新本级锁，未改动其他阶梯。[当前锁检查](anchors-check.log) 覆盖 214 行，零漂移。更新记录见 [anchors-update.log](anchors-update.log)。

[机械比对](sync-results.json) 确认：去除注释后的 TLA 文本完全相同，三个 cfg 逐字相同，历史 results.json 与三份 TLC 日志逐字相同。历史状态数仍为 2,345 / 15,097 / 3,898，不同状态对应主配置、两 root 竞争和两依赖配置；每组 Init 加 22 个 Next 动作均有生成状态。这些是此前实际运行结果，本次仅沿用相同公式下的证据。

当前源码与模型摘要记录在本阶梯 [SHA256SUMS](../../../tla/execution_models/recovery_closure/SHA256SUMS)。公共 MEMORY/ROADMAP 由主代理维护，本次未修改。
