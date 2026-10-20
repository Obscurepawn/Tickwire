# 本地 L2 订单簿：开源实现与 Tickwire 优化选择

Tickwire 可以先优化批量更新，再比较固定价格窗口与稀疏价格索引。当前有序数组已经具备连续内存、无动态分配和常数时间读取最优价的优势；最需要测量的是大批更新的重复价检查，以及新增、删除价位时的数组搬移。订单簿重建和依赖深度的特征计算作为首批性能分析对象，是否占据交易路径主要 CPU 时间由本机分段测量确定。

这里研究消费交易所行情、重建价格档总数量的 L2 本地订单簿。HftBacktest 和 NautilusTrader 的相应代码也支持其他用途，比较只取市场深度维护部分。

## 固定源码与数据结构

实际读取的源码版本为 HftBacktest `5f3ec40b2afb764e0fea112f941ed85523ef4e88`、NautilusTrader `96ec148a8249eda918c9ee3219c7030755cabcf0`。两者均从原始仓库浅克隆，引用文件已通过 `git cat-file -e <commit>:<path>` 验证。表中的复杂度是根据这些源码推导的操作数量，具体耗时交给同机基准。

设每侧有效价位数为 `L`，一个事件修改 `K` 个不同价位，固定窗口覆盖 `W` 个价格 tick，删除最优价后到下一个有效价的距离为 `G` 个 tick。

| 实现 | 实际存储与最优价维护 | 对本项目有用的取舍 |
|---|---|---|
| HftBacktest HashMap | 买卖各 `HashMap<i64, f64>`，单独缓存 best/最远价。更新经 `entry` 查找；删除 best 后，`depth_below/above` 逐 tick 查询 HashMap | 稀疏存储按有效价位付费，但删除 best 的工作量随 `G` 增长。单次哈希更新的平均成本不能代表整次事件。[源码](https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/depth/hashmapmarketdepth.rs#L21-L193) |
| HftBacktest ROI vector | 买卖各一个连续 `Vec<f64>`，`index=price_tick-roi_lb`，初始化固定上下界；窗口内直接改一个元素，删除 best 后扫描数组 | Region of Interest（ROI，关注价格区间）适合密集窗口和连续深度特征。空间随 `W` 增长，空价格也占位置；源码对窗外更新直接返回，窗口迁移不是此更新函数的职责。[源码](https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/depth/roivectormarketdepth.rs#L15-L201) |
| HftBacktest BTree | 买卖各 `BTreeMap<i64, f64>`，插入/删除后从价格有序索引取得端点 | 适合跨度大、有效价位稀疏的簿，更新按树高增长，无须为相邻价格之间的大量空 tick 扫描。树节点、分配和指针访问成本仍需测量。[源码](https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/depth/btreemarketdepth.rs#L24-L115) |
| NautilusTrader | 每侧 `BTreeMap<BookPrice, BookLevel>`，另有订单 ID 到价格的 `HashMap`；每个 level 用 `IndexMap<OrderId, BookOrder>` 保存订单，best 取树首项 | 同一结构承载 L1/L2/L3。L2 预处理把价格映射成合成订单 ID，因而仍经过订单索引与 level 容器。可参考职责拆分和有序索引，纯 Binance L2 不必复制完整逐订单结构。[ladder](https://github.com/nautechsystems/nautilus_trader/blob/96ec148a8249eda918c9ee3219c7030755cabcf0/crates/model/src/orderbook/ladder.rs#L119-L136)、[level](https://github.com/nautechsystems/nautilus_trader/blob/96ec148a8249eda918c9ee3219c7030755cabcf0/crates/model/src/orderbook/level.rs#L38-L71)、[L2 归一化](https://github.com/nautechsystems/nautilus_trader/blob/96ec148a8249eda918c9ee3219c7030755cabcf0/crates/model/src/orderbook/aggregation.rs#L55-L88) |

HftBacktest 三个后端的 L2 接口接收浮点价格和数量，再按 tick/lot 取整。Tickwire 已在入口使用 `PriceTicks(i64)`、`QtyLots(i64)`，后端优化继续保持整数单位。HftBacktest HashMap 还会在交叉时调整对侧 best，其源码注释讨论了缺失行情后的自然刷新；Tickwire 的 Binance 序号断裂直接进入 `Invalid` 并重建，这条恢复规则继续由 `SequencePolicy` 执行。

Nautilus 的公开基准本身也分用途：`book_iai.rs` 构建 L3 订单簿，测 add/update/delete 等函数；`book_analysis_criterion.rs` 在 10/100 层、每层 4 单的既有簿上测均价、最差成交价和 exposure。这些代码有助于设计工作负载；Tickwire 的 L2 更新尾延迟需要独立测量。[操作基准](https://github.com/nautechsystems/nautilus_trader/blob/96ec148a8249eda918c9ee3219c7030755cabcf0/crates/model/benches/book_iai.rs)、[分析基准](https://github.com/nautechsystems/nautilus_trader/blob/96ec148a8249eda918c9ee3219c7030755cabcf0/crates/model/benches/book_analysis_criterion.rs)

## 当前 Tickwire 的成本与语义

研究时的 [`crates/tickwire-book/src/lib.rs`](../../crates/tickwire-book/src/lib.rs) SHA-256 为 `efc82ab8f4c924fef82c45611985372f7f05ca4c21b2974199c6c80ce9bb8d87`。每侧是排序后的 `[Level; N]` 加有效长度，`Level` 只有价格和数量两个 i64。两侧数组的字段载荷是 `32N` 字节，另加元数据；例如每侧 1024 档时数组载荷为 32 KiB。

`Book::apply` 先判断身份、序号和时钟，完整验证事件，再修改买卖两侧。按现有函数实现，主要工作量为：

- `validate_levels` 对每个更新扫描此前更新，重复价格检查为 `O(K²)`；快照同样会执行这一检查。
- `validate_capacity` 每个价位二分查找一次，为 `O(K log L)`，写入阶段还会再次查找。
- `final_best` 从旧簿顶部寻找未被更新覆盖的首档，每次检查都扫描更新列表。在已保证更新价格唯一时，上界为 `O(min(L,K+1)×K)`，再扫描新数量为正的更新。
- `apply_validated` 先删除再写入。已有档位改量无需搬移；每次新增或删除价位可能搬移 `O(L)` 个元素，一批最坏为 `O(KL)`。

有序数组的稀疏成本取决于价位数量，不随价差扩大。更换容器的基准一起计入交易资格与覆盖检查。

`SideCoverage::Through` 表示截断快照已经证明边界以内的价格范围。范围内未存储的价位可以返回 `Known(0)`；覆盖外无条目返回 `Unknown`。所有已知近端价位删除后，最优价也可能是 `Unknown`。现有外侧单点更新只增加该点的知识，不填补它与原边界之间的缺失深度。

这直接约束 ROI：数组内的零值需要由覆盖范围或已知标记解释；“没有存储”不能统一视为零。窗口之外收到更新时，可以保存在有界稀疏后备区，或使订阅失效并获取新的完整基线。单纯移动数组窗口、把新区域清零，会把未知深度变成错误的已知空簿。任一后端也保留容量溢出后整事件失败、原数据不部分更新的行为。

## 优化实验顺序

第一项实验保留当前结构，给排序且无重复的规范化批次增加明确入口。若解码器保证每侧有序，入口线性检查顺序和相邻重复即可；通用乱序入口则先复制到固定工作区并排序。

随后比较两种提交方式：小批次继续二分查找和原地修改；大批次把旧有序簿与有序更新双指针合并到预分配工作区，复杂度为 `O(L+K)`，完成两侧容量、交叉和覆盖检查后再切换。后者多占一份工作区，还会扫描未修改的价位，所以阈值由批大小、插删比例和 `L` 的实测决定。

第二项实验实现整数 ROI，加占用位图查找下一个有效价，减少逐 tick 空洞扫描。每侧数量数组占 `8W` 字节，两侧 `W=65536` 时合计 1 MiB，尚未计位图与知识状态；多标的并存时要同时测缓存容量和内存预算。若一个位图字中的占用全部为零，分层摘要位图可以跳过整块；窗口移动和后备区合并作为独立慢路径测量。

第三项实验才比较有界稀疏索引，例如预分配 HashMap 加有序价格索引，或固定节点池的树。哈希表旁的有序索引负责查询“删除 best 后的下一价”，其维护成本一起计时。容器替换仍保留同一个 `BookView`、`SequencePolicy`、`Unknown` 语义和现有故障测试。

## 本机基准方案

第一层以已归一化事件为输入，计时从调用 `Book::apply` 前开始，到调用成功/拒绝后取出 best bid/ask 并形成可观察校验值结束。事件生成、文件读取、初始化和最终结果输出放在区间外。该指标是本地 book 服务时间；JSON/二进制解码、跨核排队和策略特征分别增加计时层。

| 维度 | 首轮取值 |
|---|---|
| 每侧有效档位 `L` | 64、256、1024、4096；另设接近容量上限 |
| 每事件更新数 `K` | 1、8、64、512；两侧均匀与单侧集中分别测 |
| 更新类型 | 纯改量；最佳价删除后补入；深处插删；整批删除后新增；快照安装 |
| 稀疏程度 | 相邻有效价格间隔 1、10、1000 ticks；ROI 另设固定 `W` 与越界事件 |
| 输入顺序 | 已排序、反序、固定种子乱序 |
| 工作集 | 单标的热缓存；64 标的轮转；足以越过末级缓存的工作集 |
| 异常 | 重复价、crossed book、序号缺口、容量超限、generation 不符；单独报告拒绝耗时 |

每个组合先预热，再至少重复 5 次，每次有效事件数建议不少于一百万；如此 p99.9 附近仍有约一千个尾部样本。逐事件记录 p50/p99/p99.9/max，附运行间波动、分配次数和总处理率。不同事件类别分别报告，避免大量便宜的纯改量掩盖最佳价删除和插入尾部。

计时器自身成本用空区间测量并报告；保留原始观测值，不逐样本减掉一个均值。再用不逐事件读取时钟的批量运行测吞吐，检验观测开销。Linux 目标机记录 CPU、内核、CPU affinity、频率策略、编译器、优化参数与源码 SHA；多核或回放队列实验额外记录入队到完成的等待时间，开环固定到达率下统计积压和拒绝。

当前工作树已运行最终复核轮 `cargo bench -p tickwire-book --bench depth_apply`。Apple M1 Pro、macOS 15.7、Rust 1.99.1，每组预热一万次后逐事件计时一百万次；计时边界是 `apply + view + best_bid + best_ask`。以下数值直接取自[原始 CSV](../validation/book-baseline-macos-arm64.csv)，[环境记录](../validation/book-baseline-environment.json)保存了源码摘要与命令。

| 每侧 4096 档的工作负载 | p50 | p99 | p99.9 |
|---|---:|---:|---:|
| 双侧已有价位改量，共 2 项更新 | 84 ns | 125 ns | 250 ns |
| 已有价位改量，共 64 项更新 | 2750 ns | 3584 ns | 15000 ns |
| 删除最佳价并插入新价，共 4 项更新 | 3709 ns | 4292 ns | 25500 ns |

同一次运行中，512 档的 4 项 top churn 中位数是 625 ns，4096 档为 3709 ns。规模增长与当前数组搬移逻辑相符，支持优先实验批量合并或减少搬移；还需要分段计时或 profile 区分搬移、二分查找和预验证的各自贡献。

这轮未绑核，开发任务与 TLC 并行运行。空计时区间 p50 为 0 ns、p99 为 42 ns，短事件受约 41–42 ns 的计时量化影响；尾部也包含调度干扰。该轮建立了开发机基线，下一轮按上述矩阵重复测量并记录独立运行分布，生产延迟目标仍在目标 Linux 主机验收。

所有候选先重放同一批事件，与简单参考模型逐事件比较两侧有效价位、best、覆盖与 `Unknown`、序号、错误状态，并运行现有无分配测试。需要衡量策略计算时，另加相同 top-N imbalance、microprice 或深度区间求和，再报告 `apply → feature_ready` 的分位数。本次没有复测上游性能或给出容器替换后的加速倍数。
