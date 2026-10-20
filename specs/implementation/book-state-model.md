# Book 状态有限验证契约

单所有者 Book 的生命周期与覆盖知识模型已完成两层有限检查，八组配置全部 PASS，没有开放源码候选。源码 SHA-256 为 `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`。

| 层级/配置 | generated | distinct | depth |
|---|---:|---:|---:|
| L0_BookState | 111,457 | 1,392 | 6 |
| L0_SpotScaling | 3,042,646 | 8,499 | 6 |
| L0_UsdM | 115,809 | 1,880 | 6 |
| L0_UsdMScaling | 2,220,496 | 10,524 | 6 |
| L1_BookCoverage | 45,523 | 1,471 | 6 |
| L1_Ask | 45,523 | 1,471 | 6 |
| L1_AskScaling | 585,796 | 8,244 | 6 |
| L1_BidScaling | 519,202 | 7,050 | 6 |

L0 具体转录 Spot/USD-M 的范围、stale、pu/bridge 连续性，以及 generation、失败原子性、重建和 view 创建时的 freshness。四组的 19 个 Next 动作全部生成后继；[L0 输出](../../tla/execution_models/book_state/L0_lifecycle/05_output.md)给出精确范围。

L1 以最新已接受水位的完整内容为隐藏见证，检查 Known 数量/最佳价与 world 一致、Unknown 数量允许两个相容 world，以及容量失败、重建与覆盖边界。四组的 15 个动作全部生成后继；[L1 输出](../../tla/execution_models/book_state/L1_coverage/05_output.md)说明快照真实性与完整事件的输入契约。

root 已分别完成独立源码对应审阅，两层均无阻断项。425 条源码行锚点检查通过；完整日志、配置、机器可读计数和摘要保存在 [book_state 阶梯](../../tla/execution_models/book_state/PLAN.md)。仓库 `scripts/check-tla.sh` 可复跑。

返回 view 只在创建时检查时效，缓存 view 不自动追踪时间；Live 内可以出现 Unknown best。当前尚无生产适配器把这个知识结果连接到执行风险开关。数组算法、真实数据接入、u64 全域、自定义 policy 和跨层 refinement 证明均未纳入本有限检查；数组与性能证据继续由对应 Rust 测试和既有基准承担。
