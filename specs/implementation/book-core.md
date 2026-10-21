# 订单簿 reducer 实现契约

实现：[tickwire-book](../../crates/tickwire-book/src/lib.rs)。接口设计：有界订单簿。此契约覆盖标准化事件进入单所有者 reducer 后的状态，不包含网络和解码器。

## 状态与输入

`Book<P, N, M>` 固定一个 `BookKey`、一个 generation、两边各 `N` 个价位，以及一套序号策略。`P` 通过公共 `SequencePolicy` 扩展，`BinanceSpot` 和 `BinanceUsdM` 实现当前币安规则。存储内核不选择端点，也不按交易所名字分支。

状态转换：

```text
new / begin_rebuild(larger_generation)
  -> AwaitingSnapshot
install_snapshot(valid)
  -> Bridging
apply(first bridge)
  -> Live
current-generation gap / malformed / capacity / explicit invalidate
  -> Invalid
```

`Invalid` 只接受更大 generation 的重建。`Bridging` 可以替换快照；`Live` 拒绝直接覆盖快照。`WrongBook`、`WrongGeneration`、`GenerationNotIncreasing`、`SnapshotWhileLive` 以及状态不匹配错误均不改变簿。`invalidate(generation, reason)` 供解码失败、增量缓存丢失和连接故障调用，旧 generation 的失效回调被拒绝。

价格单位为 instrument tick，数量单位为 lot；这两个整数单位在一个实例生命周期内固定。所有已存价位满足 `price > 0`、`quantity > 0`，同侧价格唯一；买价降序、卖价升序。输入 `quantity=0` 删除价位，删除不存在的价位成功。最终已知最优买价严格小于最终已知最优卖价；双边同一批次整体移动时，仅比较最终结果。

`U/u` 分别是事件首尾更新号，`L` 是本地水位。范围 `U>u` 被拒绝。Spot 忽略 `u<=L`；其余事件要求 `U<=L+1`。Futures 在 Bridging 忽略 `u<L`，要求首个事件 `U<=L<=u` 且携带 `pu`；Live 忽略 `u<=L`，其余事件要求 `pu=L`。Futures 首包的 `pu` 不要求匹配快照号。旧包忽略不改变价位、水位和新鲜度。Spot 在 `L=u64::MAX` 时全部合法范围事件都被视为旧包，避免下一号溢出。

任何实际应用批次都先检查以下条件，再修改数组：

1. 双边更新总数不超过 `M`，价格/数量有效，同侧没有重复价格。
2. 两侧分别计算最终存活数，最终值不超过 `N`；同批删除释放的空间可以供新增价位使用。
3. 最终已知最优买卖价不交叉，接收时间不早于上一次已应用事件。

全部成立后提交两侧价位，再提交水位、接收时间和 `Live`。更新工作区缓存旧位置：纯改量按缓存位置直接写入；插删将受影响区间与排序更新合并到固定数组，忽略零数量，并在净长度变化时一次搬移后方价位。调用过程由 `&mut Book` 单独拥有状态，不向外暴露中间数据。校验失败仅改变有效状态，保留上次提交的两边价位、水位和时间。`diagnostic_levels` 明确属于诊断接口；策略读取使用 `view`。

每侧最多 32 项时先识别顺序，正序保留、反序反转；正数量循环有序批次按唯一断点旋转后进入一般计划。剩余正数量无序批次可直接定位已有价格：全部存在且唯一时保留旧长度与最优价格，否则回到排序计划并重算所有索引。旧位置低 6 位的标记只筛选碰撞，碰撞后逐项精确比较价格，最多额外比较 496 次。一般路径复用相邻旧位置，二分回退使用固定全簿边界。工作区和两个分支都不改变覆盖、状态或序号策略；只有两侧验证完整成功后才写有效价位。

快照每边输入条数上限也是 `N`，包括零数量条目；快照允许无序输入并在装入时排序。大于该预算时返回容量错误。增量的重复价格被拒绝而非静默选择一个值，其他交易所的适配器可以按其协议预先合并。

不完整快照建立一个固定覆盖边界：买侧最差价及其以上、卖侧最差价及其以下全部已知。后续连续增量使该覆盖区持续有效。边界外明确收到的正数量价位可以查询；其他价位返回 `Unknown`，包括边界外删除后未保存零数量墓碑的价位。这个保守结果避免为已删除的远端价格单独消耗容量。

`known_bids/known_asks` 返回已知非零价位，调用方可以同时读取 `coverage(side)`。如果覆盖区内全部价位已被删除，`best_bid/best_ask` 返回 `Unknown`，即使数组里仍有更远的已知价位。只有适配器确认完整快照并传 `complete=true` 时，空一侧返回 `Empty`，全价格域内不存在的合法价格返回数量零。

`view(now_ns, maximum_age_ns)` 要求状态为 Live 且 `now_ns >= received_at_ns`、`now_ns-received_at_ns <= maximum_age_ns`。首包使用其真实缓存接收时间，避免重放旧包被视为刚收到的行情。`BookView.age_ns` 是创建 view 时的年龄；长期缓存 view 不会自动重新计时。策略应在决策处获取 view，并把必要时间和 generation 传到执行端再次检查。

## TDD 与当前证据

2026-10-20 按接口设计先写 15 个功能测试，建立可编译的占位 API。`cargo test -p tickwire-book` 退出 101，15 个测试均因快照接口尚未实现失败。实现后同一组全部通过，再补充 7 个功能测试和 1 个分配计数测试：

| 验证 | 覆盖 |
| --- | --- |
| 22 个功能测试 | 两产品边界、重叠/重复/缺口、generation、快照替换、外部失效、非法数据、双边原子性、最终容量、排序、零容量、完整/未知深度、时间、新策略接入 |
| 独立 BTreeMap oracle | 4 个固定随机种子，共 8,000 批；每批比较双边完整排序数组及 120 个价位数量查询 |
| 零分配测试 | 构造、快照、1,000 次双边更新与有效 view、重建；测试线程记录的 alloc/alloc_zeroed/realloc 均为 0 |
| 代码约束 | 无依赖、`no_std`、生产库 `forbid(unsafe_code)`，存储不使用堆容器 |

复验命令：

```sh
cargo test -p tickwire-book
cargo clippy -p tickwire-book --all-targets -- -D warnings
```

批量优化增加 3 个确定性工作量测试、2 个批量边界测试、1 个大批量零分配测试，并保留独立审阅者的 40,000 批差分程序为回归测试，该检查点合计 30 项。修改前的真实失败为：512 项改量在插桩处记录 273,920 次价格比较，128 项插删仅 `copy_within` 就搬移 126,912 个价位。优化后分别记录 6,188 次比较、128 个合并区与盘口价位写入；单项改量记录 15 次比较、写入 1 个价位。计数器只存在于 `cfg(test)`，用于发现重复查找与整尾搬移，不是 CPU 指令或全部内存写入计数；计数范围、完整 Red/Green、负载和 release 对照见优化证据。

该轮两组 release 对照均完成五轮，每轮、每负载、每实现采样 200,000 次，分别覆盖相邻和散布价位。结构变化受益的同时，4096 档、16/64 项乱序散布改量的五轮 p50 中位数分别从 667/2,833 ns 增至 958/4,042 ns；256 项散布插删从 147,625 ns 降至 33,333 ns。这些历史结果保留在原报告，随后小批优化继续使用全部相邻、散布、大批与插删负载。

小批优化先增加工作量测试，实际 Red 为 32 项无序改量记录 636 次比较，超过 480 的预算。实现后第一次完整五轮又发现循环有序相邻批次的尾部退化；新增跨末尾测试，实际记录 418 次比较，超过 250 的预算。最终两项分别记录 422 次、111–140 次比较。新增 3 项批次边界和 1 项分配测试后，总数为 36；原 40,000 批差分、双侧原子性、完整/未知覆盖和独立策略回归保留。无序与循环分支均通过零分配测试。过程、被拒绝候选的完整结果、源码摘要与最终对照见小批优化证据。

以 `df8bd2d` 为固定基线，最终两套 200,000 次 × 5 轮对照共保留 260 行统计。4096 档、16/64 项散布改量的 p50 从 958/4000 降至 542/2000 ns，p99.9 从 3000/15375 降至 1625/7834 ns；全部相邻改量的 p50 中位数保持不变。512 档、64 项相邻的 p99 从 667 降至 500 ns，跨末尾退化消失。512 档、512 项散布改量仍记录约 0.5% 的 p50 增长（8750→8792 ns），两实现的各轮范围重叠。运行来自未绑核的 Apple M1 Pro，计时覆盖 `apply + view + best_bid + best_ask`；完整 p50/p99/p99.9 与运行间范围保留在报告和 CSV。

当前增量每侧最坏成本为 `O(K log K + K log L)`，有插删时再加 `O(L+K)`，其中 `K<=M`、`L<=N`。实例空间为 `O(N+M)`；当前 64 位字段载荷为 `48N+32M` 字节，另加元数据。快照仍沿用校验和插入基线，最坏 `O(N²)`，本轮没有优化或测量快照安装。分配证据覆盖 reducer，不包含传输层、TLS、JSON/SBE 解码和 IPC 发布。当前没有实盘录包、Linux 延迟分位数或本模块 TLC 运行结果；首次缓存快照重试、shadow book 的原子切换、FIX/SBE 的独立字段解码仍需在上层实现并验证。

协议来源：[Spot 官方规则](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly)、[USDⓈ-M 官方规则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)。Spot 初始化文字与应用步骤的边界差异记录于接口设计，测试固定了主设计 §6 选择的 `L+1` 判定。
