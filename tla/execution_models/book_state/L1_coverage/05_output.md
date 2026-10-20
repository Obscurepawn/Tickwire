# L1 输出：覆盖知识有限验证

结论为 **PASS**，Finding: **none**。Bid、Ask 主配置与两组扩大配置共四组完整穷举通过，准确计数见 [运行记录](03_run_log.md)。00 输入与本输出在本次关闭后冻结。

在快照内容和 complete 标识准确、接受事件描述完整版本变化的输入契约下，Known quantity 与对应水位的完整内容一致，Known best 是该完整侧真正的最优价。范围外未存价位的 Unknown quantity 同时存在数量为 0 和 1 的相容完整内容。`ObserveHiddenBest` 可达，说明模型实际探索了本地未知、更优价隐藏在外侧内容中的状态。覆盖范围在连续更新期间不扩大；范围外正数量被删除后，不以零值记录制造已知性。

15 个 Next 动作均有可核对的源码效果或只读分支含义，四组每个动作 generated>0，0 个未确认源码行为。新增语义的分母是 `SideCoverage::contains`、`coverage`、`BookView::quantity`、`BookView::best` 四个 helper 的相关返回分支；五个生命周期方法以 L0 投影保留。单侧扩展函数按最终数量进行容量检查，数组排序与具体批次验证由 Rust 测试承担；本层没有重复证明双侧 spread 与同步策略。

root 已独立对照 coverage、quantity、best 审阅，通过源码对应与隐藏 world 前提检查，无阻断项。world 表示最新已接受水位，真实行情适配器对 snapshot/complete/事件完整性的保证仍是接口前提。

使用方只可把成功 view 解释为创建时 Live 且不过期。Unknown best 在有效 view 内可以出现；是否允许增险仍由真实调用方另行处理。本层没有创建仓库尚不存在的风险门控，也没有证明长时间持有 view 的自动时效。
