# 精确行情事件实现规格

实现入口见 接口。本阶段已接通以下七种输入，各自经过真实 TLS/WebSocket、类型解码、规范发布、共享内存、MarketReplica 和异构 JOIN 快照：Spot JSON Trade/BBA、Spot SBE Trade/BBA、USD-M JSON 聚合成交/BBA/MarkFunding。

## 协议和边界

- 价格与数量按可信 InstrumentBinding 的 tick/step 精确换算；非整倍数和整数溢出拒绝。源 symbol、product、catalog、source/connection 身份全部保留。
- 参考价格和资金费率使用 DecimalWire：`coefficient: [u8; 16]`、`exponent: i8`、七字节零填充，24 字节、8 字节对齐。尾数是有符号小端整数，指数 −38…38；零与可约去的尾零采用唯一表示。
- 市场消息 264/265/266 使用公共 128 字节头及 384 字节 MarketExactBody。旧消息 257–263 不变。资源、控制协商、SHM、snapshot builder/validator 和 Source 全部要求 feature bit 9。
- JSON Spot BBA 无交易所时间，保留缺失；SBE 时间是微秒并检查转纳秒乘法。聚合成交保留 aggregate ID、原 trade 范围、总数量和可选 non-RPI 数量。交割资金费空字段与数值零分别表示。
- 接收整帧 SBE 后先验证全部 rows；回调预算不足时保留帧内位置。未知模板、错误 schema/version、截断和 group 边界由固定 SBE codec 拒绝，不以 JSON 中转。
- 单 owner 固定产品/标的/类别。Connect、Upgrade、Subscribe、Pong 使用现有 PublicPermit；零接受重试重新申请。TLS 主机验证开启；SBE 只允许 SpotProduction、使用 Upgrade API key，不使用私钥。
- ExplicitSubscribe 以完整发送后的精确 null ACK 确认。EOF 与完整 ACK/数据同批时先失效，再交付已完整收到但 `usable=false` 的旧数据。I/O、消息解析、SBE rows、控制队列、接收时间片和通知数均有边界。

## 发布、时间与恢复

每标的保存 Quote、Trade、MarkFunding、FeedState 四个类别；旧/新编码同类不能重复。独立 BBA 配置替代深度派生 Quote，但不替换深度 levels 或 coverage。聚合 FeedState 要求配置的类别已可用，仍由真实 DepthSync 确定深度连续性。

每类事件保留自己的接收时间。FeedState 使用最近深度观测时间，其他事件不得刷新深度 age。MarketReplica 按全 topic 连续序号应用，身份/代次/序号或容量错误使视图失效；JOIN 仍从完整快照 S 接收 S+1，SBE depth20 不代替完整快照。

`TWMR0001` 检查点保存实际物化的全标的视图与 blocked 状态，最多 4096 标的、64 MiB，恢复要求同一最大深度容量。恢复校验版本、长度、尾 SHA256、所有状态记录语义、身份、排序、精确 coverage 和容量。快照内 state sequence 是 1 起行号，live state sequence 是原 topic 序号；`last_event` 单独保存最近 topic 事件位置。上层恢复闭包负责对象授权。

MD 默认 Feed.events 为空；显式事件配置绑定独立 owner/source identity，并同步扩展 InstrumentSpec.required_states 与 bank feature。策略实际消费的规范记录和 arena 输入沿既有 SDK 输入 WAL 保存，MD 不新增同步原始网络日志。

## 标的索引与存储局部性

标的目录中的稳定 `instrument_id` 在每个 topic 的行情边界映射为从零连续的本地索引；映射规则见 [标的索引](instrument-index.md)。固定目录在装配时构造位掩码索引，病态离散集合使用有界开放寻址后备。MD 为独立 feed 预先绑定发布槽位，通知直接访问连续的 `Vec<Row>`，每次仍核对槽位对应的稳定 ID；不同 topic 的槽位不能绕过原身份与代次检查。bank 轮换沿既有规则保持目录和行顺序，新增标的必须重新完成目录版本与 JOIN 准入，不从未知网络消息自动注册。

动态目录使用只追加索引，受信注册或冷路径扩容不改变已分配索引。IPC、Trigger、快照和 WAL 继续携带稳定 ID；`BookKey` 使用 owner 预绑定的本地身份，本地稠密索引不是全局身份或授权凭据。策略端同样在消费入口解析索引后访问连续盘口元数据，检查点仍按稳定 ID 排序且保持原编码。

`MarketReplica` 的 Quote/Trade/MarkFunding/FeedState 分别占固定状态槽，类别值仍为 1/2/4/8。深度更新先验证 bid 严格递减、ask 严格递增和完整载荷，再流式合并到 topic 共用的两侧连续工作区；两侧容量和最终非交叉盘口全部通过后才提交。工作区各预留 `maximum_levels` 项，在 10000 档上限时合计最多 320000 字节。盘口本身保留已有 `Vec` 容量，新增价位超过已有容量才扩容；已预留容量内的更新、固定类别事件和查询均不分配。失败仍保持原盘口并关闭可见性，覆盖范围外的 `Unknown` 不变。稠密索引和工作区不进入 `TWMR0001`，既有检查点按稳定 ID、类别顺序逐字节兼容。

价位数量查询按买侧降序、卖侧升序在连续数组上二分查找；未找到价位时仍按覆盖范围区分 `Some(0)` 与 `None`，不得把未知范围当作零量。

app 的本地公共行情 owner 在装配时按 `CanonicalBaseline` 的完整规范目录顺序分配 `BookKey.instrument`，目录最多 64 个标的；匹配仍使用配置的完整 `u64 instrument_id` 与产品。不同目录项获得不同本地槽，同一基线的重复装配、恢复和连接轮换保持槽位不变。`BookKey.venue/source` 保留配置身份，网络事件与外部策略输入继续使用原 `InstrumentBinding.instrument_id`。本地槽不能跨账户 owner 或目录实例交换；新增目录需重新装配 owner，不在既有 owner 内重新编号。`market_sources` 的共享内存订阅各持有自己的副本，不使用这套本地盘口槽。

## 验证

原始证据包含先失败测试、修复结果、最终源码 SHA 与命令。旧 63 个独立契约向量保持不变，新 46 布局生成检查与独立字节样本覆盖 Exact 消息及发布控制 38–40；独立 256 字节样本覆盖新增 replay 控制 41/42 的身份、挑战、操作、对象和保留字节。

定向真实回归还检查了七类输入的来源时间和精确值、SHM 实际借用、后续完整 JOIN、检查点恢复，以及独立事件不能刷新深度年龄。既有 SBE 深度 mapped 测试继续运行。此阶段未新增 TLA；模块与全局模型在功能代码完成后统一更新。

## 发布目录的本地盘口键

`InstrumentConfig` 将完整的外部 `u64 instrument_id` 绑定到 owner 分配的本地 `BookKey`；`BookKey.instrument` 可以为零，不要求等于外部 ID 或其低 32 位。同一 topic 的不同目录行必须使用不同的完整 `(venue, instrument, source)` 键，`TopicPublisher::new` 在接纳目录时拒绝重复键。不同 venue 或 source 下相同的本地 instrument 值仍是不同键。

深度发布先核对 `BookView` 的完整键与目标行一致，错误键不得改变该行的盘口、revision 或已发布位置；既有故障路径仍同步失效发布 bank。正确视图按其目录行的稳定 ID 编码 Quote、FeedState 与深度记录，JOIN 快照的成员 ID 和状态记录也保留完整 `u64`。回归必须覆盖低 32 位相同的两个高位外部 ID，分别绑定本地键 0、1，并验证错行视图被拒绝、失败 bank 交接后缓存内容保持原值。

## 发布热路径的有界存储

`TopicPublisher` 在构造时预留每标的完整四类状态的容量和两侧盘口缓存。状态仍按 `message_type` 排序，保持 JOIN 与换 bank 时的既有编码/重发顺序；独立事件 source 使用三个固定槽，分别对应 BBA、Trade、MarkFunding。首次加入类别也不能扩容。

每个 topic 共享一个双侧合计最多 4096 档的解码工作区和一个 4096 项 `DepthLevel` 发布工作区，不按标的重复分配。缓存盘口仍可配置每侧最多 5000 档。普通深度通知使用最多三条记录和三项消息引用的固定数组，经同步 `publish_market` 发布；该调用返回后即可复用工作区。JSON/SBE、首次及连续深度/事件从发布入口至 SHM 在健康路径不进行堆分配，JSON 完整发布须能在 2 MiB 线程栈执行。网络 owner 的接收缓冲、帧解析、输入解码另有分配边界，不由本条结论覆盖。

`cache_bytes` 限制发布器自身持有的堆载荷保留量：行数组、每行状态与两侧深度容量、目录及解码 context 的文本、固定标的索引、共享解码/发布工作区。使用实际 capacity 与元素尺寸核算，所有求和与乘法检查溢出；`reserved_cache_bytes()` 返回同一核算结果。预算不含分配器元数据、发布器内联体及有界调用栈，SHM/JoinOwner 和临时 JOIN 快照由各自资源预算管理。构造预检与实际保留量复核均拒绝超限，不把原 wire 字节估算当实际堆上限。

失败语义保持不变：错误 key、旧代次或超过 view 容量在修改行前拒绝。原始增量解码、增量与视图不符或 SHM 整批发布失败时，可以保留最新健康 `BookView` 缓存与 revision，但已发布序号不得前移；同步触发 `FaultSink`，设置 dirty/needs_bank 并拒绝快照和 Live。bank 失败期间继续接收健康源缓存，供后续完整 JOIN；不能因复用工作区而丢失新缓存或泄漏上一帧尾部。回归覆盖总 4096/4097、inline 12/arena 13、长帧后短帧、首次类别、ring/arena 耗尽、失败 bank 交接与缓存预算临界值。
