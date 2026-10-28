# 离散标的 ID 与连续索引

`tickwire-instrument-index` 将完整 `u64` 标的 ID 映射到进程内连续 `DenseId`，不依赖其他 workspace crate，不处理交易授权、目录版本或 IPC 编码。`DenseId` 只暴露 `index()`；不同索引实例的值不能混用，调用方负责绑定所属目录。

## 固定集合

`FixedIndex::new(&[u64])` 按输入顺序分配 `0..len`，重复键返回 `DuplicateKey`。空集合、零和 `u64::MAX` 均合法。`lookup` 返回已知键的 ID，`key` 对范围内 ID 返回原键，越界返回 `None`。

构建在冷路径执行：从有变化的位开始，按高位到低位贪心删除仍能保持键唯一的 mask 位。软件 bit gather 预计算至多六级移动 mask，把保留位压到低位后查 slot→dense 表；最后始终比较完整原键，防止未知 ID 投影到已知槽。该方法不保证最小位数。

掩码筛选参考 [qlibs/mph](https://github.com/qlibs/mph) 的位筛选思路。目录来自运行配置，因此在加载时计算并缓存索引；查询使用可移植 Rust 软件 gather。

位表只在容量不超过键数四倍且不超过 4,194,304 槽时采用。病态集合可能需要全部 64 位，不能直接按 `2^64` 分配；此时自动使用负载不超过 1/2 的 flat 开放寻址表。fallback 同样存储 dense 序号并精确核对原键，不因 mask 稀疏拒绝资源上限内的合法集合。

## 追加集合

`AppendIndex::with_capacity(max)` 预分配键数组和 flat 表。`insert` 对新键分配当前 `len`，已存在键返回原 ID；满容量时只拒绝新键，原有映射不变。没有删除、复用或排序入口。`lookup`、`key` 与预留容量内的 `insert` 无堆分配、锁或 I/O。

`reserve(new_capacity)` 在冷路径把总容量至少扩到给定值；较小值不缩容。先构建全部候选存储，再替换旧表，因此分配或容量错误保持旧 ID、长度和映射。扩容完成后所有既有 `DenseId` 保持稳定。

## 行情入口与本地盘口身份

MD 启动时从受信 feed 配置创建一个 `FeedIndex`，按外部标的 ID 首次出现的顺序建立固定索引。同一标的出现在多个 feed/topic 时复用本地编号；零外部 ID、空配置、超过 32 个 feed、未知查询拒绝。`BookKey.instrument` 使用索引返回的零起连续编号，不再把外部 `u64` ID 转成 `u32`。网络深度 owner 与各 topic 的发布器都从同一索引取得 key；topic 自己的排序和发布行槽位可以不同，不能用 topic 的行号代替源盘口 key。

`Feed::build` 显式接收该进程的 `FeedIndex`，在创建网络 owner 前完成绑定；行情循环仍使用启动时预绑定的 topic 槽位，没有新增逐消息查表。外部 ID 的全部 64 位保留在 `InstrumentBinding`、目录、IPC、快照和恢复记录中。`BookKey` 只在本地解码与盘口链路使用，不作为交易标的或持久身份。来源 `stream_id` 的语义不变，本轮仍要求深度源可无损表示为 `BookKey.source` 的 `u32`；事件源继续保留完整 `u64 source_id`。

这一接线适用于当前固定配置。运行中的目录增加还需要配置发布、源预热、资源代次切换及已有消费者重新 JOIN；不能把 `AppendIndex::insert` 或本地编号分配称为已完成运行时扩容。

运行时扩展见 [market-expansion.md](market-expansion.md)。MD 的 `FeedIndex::expanded` 按完整目标配置构建独立候选，保留原 ID→槽位关系，要求全部旧 ID 仍在目标集合，新 ID 按首次出现顺序追加。候选构建失败时活动索引不变；标的经济规格和授权由扩展计划另行核对。

## 边界与验证

两个实现最多接纳 `MAX_KEYS = 1,048,576` 个键；超限返回 `Capacity`，可报告的预分配失败返回 `Allocation`。所有槽数计算检查溢出，探测最多扫描整表，槽使用 `u32` dense 序号与私有 `u32::MAX` sentinel。哈希只决定探测起点，不代替身份比较。`memory_stats()` 返回键数组和槽数组的实际预留字节；不包含索引结构本身和分配器元数据。

回归覆盖输入顺序、重复键、完整 u64 边界、mask 别名拒绝、必须 fallback 的集合、哈希冲突、追加及扩容稳定性、失败原子性和随机差分。软件 gather 与逐位提取比较；热操作验证预留存储地址和容量不变。整个 crate 禁止 unsafe，不使用 CPU 特定指令。

`lookup` 基准为 harness=false，可比较固定/追加索引、BTreeMap、HashMap 和排序数组 binary_search，使用 32/256/4096 个键、命中与未知查询、可压缩和分散两类 ID。预先构造查询和容器，计时仅覆盖 black_box 查询循环；追加插入单独计时。每组 4096 次查询，输出批次平均每次耗时的 p50/p99/max，以及 `# memory` 行中的键/槽预留字节；这些分位数不是逐次查询尾延迟。输出为本机合成负载数据，不代表交易全链路延迟。

2026-10-28 在 Ryzen 7 8845H / WSL2 Linux x86_64、Rust 1.99.1 release 上独立执行五轮，固定 CPU 2，测量期间没有并行编译或测试。每组 200 个批次；下表为各轮批次平均耗时 p50 的中位数，单位 ns/query：

| 4096 键命中查询 | 固定索引 | 追加索引 | BTreeMap | HashMap | 排序数组二分 |
|---|---:|---:|---:|---:|---:|
| 可压缩 ID | 1.903 | 1.993 | 10.085 | 6.861 | 10.885 |
| 分散 ID | 2.089 | 1.993 | 10.447 | 6.859 | 12.754 |

这是重复访问同一索引的合成负载，允许独立查询流水执行；没有计入实际行情解码、IPC、策略或持久化。完整尺度、未知查询与插入结果保存在[五轮汇总](../evidence/20261028/index-bench-summary.json)及[第一轮原始 CSV](../evidence/20261028/index-bench-1.csv)（同目录包含其余四轮），[环境与参数](../evidence/20261028/index-benchmark-settings.json)一并保存。
