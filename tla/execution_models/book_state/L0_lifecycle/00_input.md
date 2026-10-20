# L0 输入：生命周期与序列策略

本层为阶梯起点，没有上层假定。源文件为 `crates/tickwire-book/src/lib.rs`，SHA-256 `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`。源码保持冻结。

| 已核对源码 | 模型对象 |
|---|---|
| crates/tickwire-book/src/lib.rs:182-194 | Spot：先检查 U≤u，再处理 stale 和 U≤L+1 |
| crates/tickwire-book/src/lib.rs:198-224 | USD-M：桥接 U≤L≤u，连续 pu=L；桥接要求 pu 存在 |
| crates/tickwire-book/src/lib.rs:590-605 | 初始 AwaitingSnapshot、空内容与空水位 |
| crates/tickwire-book/src/lib.rs:620-642 | 快照身份、状态、整体校验、双侧安装及 Bridging |
| crates/tickwire-book/src/lib.rs:644-710 | 增量身份、状态、策略、时钟、整体计划、双侧提交 |
| crates/tickwire-book/src/lib.rs:712-725 | generation 严格递增的重建 |
| crates/tickwire-book/src/lib.rs:729-735 | 同 generation 的显式失效 |
| crates/tickwire-book/src/lib.rs:737-761 | 创建 view 时的 Live 和时效检查 |
| crates/tickwire-book/src/lib.rs:771-779 | 身份检查无写入 |
| crates/tickwire-book/src/lib.rs:781-790 | 失败只把状态改为 Invalid |

已从当前代码确认每次 `&mut Book` 调用完成后才允许下一次修改。`BookView` 持有 slice 借用；本模型只记录查询返回时刻，调用者使用结束后才进入下一动作。当前 mock 示例的 `market_valid` 是独立填写字段，模型没有把它连接成真实增险门控。
