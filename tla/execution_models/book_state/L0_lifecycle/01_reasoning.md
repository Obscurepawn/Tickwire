# L0 推理与抽象

本层检查一个 Book 在快照、桥接、连续更新、拒绝与重建之间转换时，是否会暴露失效内容或半写数据。采用源码转录；错误输入、旧消息、时钟倒退和显式失效都是公共 API 已处理的输入。

| 保留的语义 | 抽象及作用 |
|---|---|
| Spot/USD-M 判定的分支顺序及 U/u/pu 比较 | key 固定，错误身份折叠为不相等；`None` 用 -1 表示 |
| generation、四个状态、水位、最后接受时间 | Invalid 的四种 reason 合并，因为后续生命周期处理相同 |
| 快照和增量的双侧提交、失败保持原内容 | bid、ask 为独立内容见证；成功校验可给任意有限内容，失败走源码共享 fail 分支 |
| view 的 Live、负 age、超龄拒绝 | `viewNow` 是最后一次成功调用的时间参数；不把返回 view 当作自动刷新缓存 |
| 重建清空内容和时间、水位 | Book 固定一份，无数据竞争：每个动作是一整个 safe Rust 方法调用 |

内容见证没有价格或数组布局。它允许校验成功后的任意内容替换，因而可以检查错误返回是否改变任意一侧，但不验证排序、去重、净容量或 merge 算法。计数超限、价格/数量错误、重复、容量不足及 crossed book 合并为 `PayloadError`，快照校验失败合并为 `SnapshotError`。对两种内建 policy，`apply` 中第二次范围/不降水位防御检查不会产生额外可达分支；自定义 policy 不在本层。

TLC 主配置使用 generation 0..1、ID 0..2、时间 0..2、两种侧内容和最大 age 1；这些值已允许重建后的旧 generation、stale、缺 pu、gap、倒退时钟和超龄 view。扩大配置使用 generation 0..2、ID/时间 0..3、三种侧内容，并把 USD-M 的 age 改为 0。数值是边界关系代表，不是 u64 全域证明；整数溢出不在范围内。

19 个 Next 动作覆盖五个有状态公共方法的成功与生命周期相关失败组。生成错误身份调用的输入被存在量化消去：始终存在不同 BookKey；`InvalidateWrongGeneration` 则明确要求有限域中存在不同 generation。每个动作均必须有 generated>0。

六个 action safety 性质直接比较调用前后数据：失败不改数据、错误身份不改 Book、stale 不刷新、generation 不回退、Invalid 只能由重建退出、成功增量水位不回退。快照只进入 Bridging；桥接中重装快照允许较低 ID，故没有宣称整代 ID 单调。成功 view 必须在创建时满足 Live 与时效。没有 fairness 或恢复最终成功的活性假定。

初始 generation 固定为 0，是有限起点；源码 `new` 接受任意 u64。配置中的重建仍覆盖新代次及旧回调。u64::MAX 与自定义 policy 未由有限配置证明。
