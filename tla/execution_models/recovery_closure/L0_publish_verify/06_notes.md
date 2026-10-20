# 故障模型修正依据

在一次 rename 后目录同步返回错误时，调用方会返回但固定文件仍可见。后续 API 可以开始另一个 key 的保存，因此同一目录可积累多个未确认的 rename。仅选择整份 disk 或整份 cache 会遗漏“一个存活、另一个丢失”的内容见证。

当前 `Dirty` 取 cache 与 disk 不同的 key，`Survivors` 从其所有子集构造存活状态；已相同的 key 始终原样保留。每个被 rename 的新文件此前已经 file sync，成功目录同步因而提交整个 cache。失败目录同步只选择部分完成状态，其他阶段错误保持已确认 disk；崩溃再独立决定未确认项的命运。

这是 risk_core 独立审阅发现并由 market_core 修正的模型边界，未修改恢复闭包 Rust 实现。root 负责其业务接口、源码修复和顶层索引集成。

## 账户依赖扩展同步

同步以原 SHA 对应的提交 `868980474de3e77708dd4b9fce0bcab26ff2eac3` 为比较起点，包含随后新增种类 13 与本轮种类 14/15 两次扩展。新增 shape 分支和返回字段没有改变本级所描述的完成点、错误返回顺序或单所有者拓扑。已逐段复核源码后更新锚点；未修改公式或运行配置，也未重新运行 TLC。

更新前原件位于 [pre-sync](../../../../docs/validation/account-recovery-binding/pre-sync/recovery_closure/L0_publish_verify/06_notes.md)。具体种类检查仍由 Rust 测试验证，当前历史结论限于发布/复验持久化抽象。
