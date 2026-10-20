# 开源交易系统与 Rust 组件调研

本文保留调研时的源码/协议事实与推导背景；运行架构和生产要求以 [v0.3 主设计](../design/binance-ultra-low-latency.md)、[共享内存契约](../design/shared-memory-ipc.md)与[策略生命周期](../design/strategy-lifecycle.md)为准。早期关于单进程或首版取舍的建议不再作为当前部署方案。

首版建议采用自有的小型同步交易内核：借鉴 NautilusTrader 的订单生命周期、资金预留和对账，Barter 的索引状态与同步事件处理，hftbacktest 的双时间轴、订单延迟和排队模型。直接复用网络、TLS、WebSocket、队列和测试基础库；hftbacktest 放在离线研究侧。三个框架都不适合作为不经裁剪的超低延迟 Binance 内核。

范围覆盖 **Binance Spot 与 USDⓈ-M Futures**。调研日期为 **2026-10-20**；以下仓库结论来自固定提交的代表性源码，组件版本来自当日 crates.io 注册信息。本轮没有编译这些项目，也没有运行性能基准；性能取舍是待用 Tickwire 工作负载验证的设计建议。

## 研究快照

| 项目 | 固定提交与提交日期（UTC） | 源码中的版本与许可证 | 对 Tickwire 的主要用途 |
| --- | --- | --- | --- |
| NautilusTrader | [`42c3ca5f8f8d537f69772277e77792bb5b307ea5`](https://github.com/nautechsystems/nautilus_trader/commit/42c3ca5f8f8d537f69772277e77792bb5b307ea5)，2026-10-19 | Rust workspace `0.67.0`，`LGPL-3.0-only`；这是 `develop` 研究快照 | 订单与账户模型、风险预留、恢复和统一回放 |
| Barter-rs | [`9770b27a83f844472b93b593b08063affc974b0d`](https://github.com/barter-rs/barter-rs/commit/9770b27a83f844472b93b593b08063affc974b0d)，2026-08-20 | `barter 0.14.0`，MIT | 单一状态所有者、索引化状态、策略／风控接口 |
| hftbacktest | [`5f3ec40b2afb764e0fea112f941ed85523ef4e88`](https://github.com/nkaz001/hftbacktest/commit/5f3ec40b2afb764e0fea112f941ed85523ef4e88)，2025-12-23 | Rust crate `0.9.4`，MIT | 行情回放、排队假设、延迟模型和实盘回报去重参考 |

三个仓库在查询时均未归档；提交日期只描述研究快照，不代表生产成熟度。版本和许可依据见 [Nautilus Cargo.toml][n-cargo]／[LICENSE][n-license]、[Barter Cargo.toml][b-cargo]／[LICENSE][b-license]、[hftbacktest Cargo.toml][h-cargo]／[LICENSE][h-license]。

## NautilusTrader：借鉴状态、资金预留和恢复语义

### 状态所有权与执行链

`NautilusKernel` 集中持有 clock、cache、portfolio、data/risk/execution engines 和 trader，各组件使用 `Rc<RefCell<_>>`。源码明确指出这个 kernel 不是跨线程同步边界。可以借鉴其单线程内集中更新状态的做法；`Rc<RefCell<_>>`、动态分发和消息总线本身仍有运行时成本，Tickwire 可用直接字段与 `&mut CoreState` 表达相同所有权。[内核定义][n-kernel]

`RiskEngine::handle_submit_order` 检查订单、合约定义和 reduce-only 条件，再进入账户风险检查；`execution_gateway` 在把命令交给节流器前记录资金预留。后续检查会扣除尚未反映到账户状态的预留，避免连续订单重复消耗同一笔可用资金。Tickwire 应把“校验＋预留＋产生待发送命令”放在同一状态所有者的一次状态转换中，按 Spot 钱包与 Futures 保证金账户分别记账。[提交检查][n-risk-submit]、[预留管理][n-risk-reserve]、[执行门控][n-risk-gateway]

执行引擎在路由前验证命令，拒绝已经分派或订单状态已过期的重复提交；还提供订单、成交和仓位报告的对账入口。值得复用的是“命令意图、发送状态、交易所事实”分开建模，以及回报统一进入订单状态机。socket 写成功只说明本地发送进度，订单接受状态由交易所回报确认。[提交去重][n-exec-submit]、[对账入口][n-exec-reconcile]

### Spot 与 Futures 的分离

模型中有独立的 `CashAccount` 和 `MarginAccount`。Binance Futures 执行适配器把钱包余额、可用余额和抵押资产的初始／维持保证金映射到账户状态，并在连接时查询双向持仓模式。订单管理系统（Order Management System，OMS）把订单意图和回报组织成一致的订单／仓位状态；该快照对于“配置的 OMS 模式与交易所持仓模式不一致”只记录 warning。Tickwire 首版建议把这种不一致作为禁止新开仓的条件。[CashAccount][n-cash]、[MarginAccount][n-margin]、[Futures 账户映射][n-futures-balance]、[持仓模式检查][n-futures-mode]

其默认保证金模型包含按名义价值、杠杆和合约保证金率计算的通用实现。这个抽象可借鉴为 `MarginModel`，但不能据此认定已经覆盖 Tickwire 所需的 Binance 风险阶梯、资金费、逐仓／全仓、标记价格和清算约束；这些应由独立的 USDⓈ-M 风控模型及场景验收承担。[保证金模型][n-margin-model]

### 行情与回放

通用订单簿支持 L1 最优报价、L2 按价格档聚合和 L3 逐订单数据，保存 sequence、最后时间和更新数；价格梯使用 `BTreeMap`，另有订单索引。适合作为功能模型和正确性参考。Tickwire 可以先采用整数 tick 的有序价格结构，待分析实际标的的价格范围与更新分布后，再决定是否使用局部密集数组。[OrderBook][n-book]、[BookLadder][n-ladder]

回测循环依据 `ts_init` 推进虚拟时钟，先处理数据、再清空延迟命令并推进交易所队列；同一时间戳的数据完成后才收尾定时器。流式分批时特别处理时间戳边界。Tickwire 的回放也需要明确同时间事件的稳定次序，避免文件分块大小改变结果。[回测循环][n-backtest]

### 复用范围与许可证

默认只借鉴设计并独立实现 Tickwire 核心。`LGPL-3.0-only` 允许商业使用，但分发包含该库的组合产物时有许可通知、相应源码和用户可替换／重新链接库等要求；Rust 静态链接场景需要落实对应交付方式。不能把复制某个 `.rs` 文件或生成代码当作绕开许可证的方式。[LGPL 条款][n-license]

若后续决定直接采用 Nautilus crate，应把它作为明确的架构与分发决策；其多资产模型、适配器体系和较大的依赖面会一起进入项目。

## Barter-rs：借鉴小型同步内核，重写交易所关键语义

`Engine<Clock, State, ExecutionTxs, Strategy, Risk>` 直接持有各组件，`Processor::process(&mut self, event)` 更新状态；同步 runner 从迭代器逐条处理事件，异步 runner 则把输入改成 `Stream`。这支持把网络与事件来源隔离在内核外，并保持策略、风险和状态更新接口相同。[Engine][b-engine]、[runner][b-run]

`InstrumentStates` 和 `AssetStates` 用索引映射持有状态，以 `InstrumentIndex`／`AssetIndex` 直接访问；热路径无需反复用交易对字符串查表。Tickwire 可进一步在启动时固定 instrument registry，使用带类型的整数 ID；外部符号只在协议边界解析，未知符号返回错误，不把无效索引带进内核。[instrument 索引][b-instruments]、[asset 索引][b-assets]

策略生成订单后，代码先调用 `risk.check`，再发送批准的命令并记录 in-flight 状态。`RiskApproved<T>` 有助于表达接口，但其字段公开，不能独立保证所有发送路径都经过风控。Tickwire 应由私有构造函数创建批准令牌，并让手动命令、策略命令和恢复命令共用强制检查入口。[生成与发送顺序][b-generate]、[风险接口][b-risk]

有两处会直接影响选型：

- 固定提交中的 `barter-execution/src/client/binance/mod.rs` 是空模块，文件仅一个换行。不能据此获得完成的 Binance 现货或合约执行适配器。[Binance execution 模块][b-binance-empty]
- `DefaultRiskManager` 把全部订单包装成 `RiskApproved`，源码标注仅用于演示。账户级资金预留、保证金、仓位限制和异常状态门控必须另行实现。[默认风控][b-risk]

行情侧分别实现 Spot 与 USDⓈ-M 的 L2 sequencer，并有对应单元测试。这种分离值得采用，但规则必须逐项对照交易所现行规范：Barter Spot 的后续更新要求 `U == previous_u + 1`；本轮读取的 Binance 官方规则是在 `U > local_u + 1` 时判定缺口，因而允许某些重叠区间。直接移植 Barter 的等号条件会触发多余重建。Futures 则独立验证 `pu == previous_u`。[Spot 源码][b-spot-depth]、[Futures 源码][b-futures-depth]、[Binance Spot 规范][binance-spot-depth]、[Binance Futures 规范][binance-futures-depth]

审计副本通过递增 sequence 丢弃旧事件，对缺失中间事件返回错误，适合借鉴为监控副本完整性检查。但 runner 的审计出口接受 `ChannelTxDroppable`，不能直接当作可靠交易日志。回测入口使用 `HistoricalClock` 和 mock execution；同一事件处理器可以复用，不代表已经具有精细的交易所排队或网络延迟模型。[审计副本][b-replica]、[runner][b-run]、[回测入口][b-backtest]

MIT 许可允许在保留版权和许可通知的条件下复制或修改。可挑选类型设计、测试用例和非关键行情工具；首版不把整个 Barter 引擎和它的执行层作为基础。对于所需的行情序列状态机，按 Binance 规范独立写小实现更容易覆盖边界条件。[LICENSE][b-license]

## hftbacktest：复用研究工具，借鉴时间和回报模型

`Backtest` 分别保存 local 与 exchange processor，事件循环区分 `LocalData`、`LocalOrder`、`ExchData`、`ExchOrder`。这让“交易所已经发生的事”与“本地策略此刻知道的事”分离。Tickwire 回放应保留交易所时间、本地接收时间和本地处理序号，策略只能看到按本地可见时间到达的事件。[Backtest 状态与循环][h-backtest]

`LatencyModel` 分别计算订单入口和响应延迟；历史样本保存 `req_ts`、`exch_ts`、`resp_ts`，插值模型可驱动逐笔订单延迟。`QueueModel` 在订单接受、同价成交和深度变化时更新排队估计；保守模型和概率模型可以作为成交假设的敏感性分析。Tickwire 可把自己的实测延迟样本输入离线模型，而不是把“订单发出后立刻成交”作为默认假设。[延迟模型][h-latency]、[排队模型][h-queue]

`ROIVectorMarketDepth` 把指定价格区间映射成连续数组，并维护最佳买卖 tick。它说明局部订单簿可用空间换取直接索引，但其价格／数量转换和深度值使用 `f64`，区间外数据也有独立处理。Tickwire 实盘应保留整数 tick／lot，并定义价格移出区间时的重建或回退行为；不能静默丢掉之后可能成为最优价的档位。[区间数组订单簿][h-roi]

Binance Spot 与 Futures connector 都有订单管理器。Futures 的实现用 `Arc<Mutex<OrderManager>>` 聚合 REST 与 WebSocket 回报，通过 `removed_by_ws`／`removed_by_rest` 延迟清理终态订单，防止另一通道迟到的回报把订单重新带回本地状态。可借鉴跨通道去重、唯一 client order ID 和终态保留；Tickwire 用单一状态所有者、累计成交量和成交 ID 去重实现这些语义。[Futures 订单管理器][h-futures-om]、[Spot 订单管理器][h-spot-om]

回测 `State` 维护净仓位、余额、费用和成交统计；`LinearAsset`／`InverseAsset` 负责名义金额和权益计算。这个账本可用于策略研究，但没有在这组实现中表达 Binance 全仓共享保证金、逐仓隔离、双向仓位、阶梯维持保证金、资金费和清算流程。首版 Futures 实盘账本不能直接复用这组 `f64` 状态。[回测账本][h-state]、[资产模型][h-asset]

官方说明其回放不改变历史市场，无法模拟自身交易对盘口的影响；即使选择部分成交模型，也需要理解主动吃单的模拟范围。它适合作为研究工具与独立参考模型，不能作为收益或实盘成交的证明。[成交模型说明][h-fill-doc]

MIT 许可适合离线直接依赖或有选择地复用代码。建议先通过导出适配把 Tickwire 的原始／规范化事件交给 hftbacktest，保持实盘整数账本、恢复和账户风控由 Tickwire 自己负责。[LICENSE][h-license]

## 对首版内核的具体建议

以下为综合上述证据形成的设计建议，尚未实现。

| 决策 | 落地方式 | 借鉴来源 |
| --- | --- | --- |
| 一个风险域由一个状态所有者更新 | 一个同步核心更新订单、Spot 钱包、Futures 仓位与各自资金预留；跨市场组合敞口在同一有序事件流中更新。扩容时按能独立授权的风险域拆分，避免不同交易对线程同时消费同一份余额 | Nautilus 预留；Barter 的 `Engine<State>` |
| 统一事件外壳，保留市场差异 | 公共字段包括 account、instrument、event kind、source sequence、receive time、local sequence；Spot 余额与 Futures 保证金／position side／funding 使用不同 payload | Nautilus 账户模型；Barter 的双 sequencer |
| 风控与发送之间保留明确状态 | 校验后预留，明确区分尚未排队、已排队、部分写出、已写出、接受、未知、终态；网络状态不明时继续占用相应风险预算并对账 | Nautilus execution；hftbacktest 跨通道回报 |
| 终态仍保留去重信息 | 累计成交不能回退；同一成交 ID 只记一次经济结果；终态订单保留至对账及迟到事件窗口结束 | hftbacktest 终态保留；Nautilus 对账 |
| 记录接收次序，独立注入时间 | 本地 journal 用单调递增序号定义回放顺序；时间源、输入源、执行出口可替换，策略和风险状态转换保持同一实现 | Nautilus 虚拟时钟；Barter runners；hftbacktest local/exchange |
| 有界队列的满载行为是协议的一部分 | 深度增量丢失后把订单簿置为无效并重建；执行回报不可静默丢弃；无法保留回报或可靠日志时停止增加风险；未入队命令才能按“确定未发送”撤销预留 | 本调研对有界队列与状态一致性的设计推导 |

最小首版账户能力应分别列清：Spot 处理可用／冻结资产、买卖预留和费用币种；USDⓈ-M 处理仓位模式、position side、reduce-only 语义、杠杆与保证金模式、标记价格、初始／维持保证金、资金费及交易所账户更新。两者可以共享事件驱动方式和订单生命周期，不共享同一套余额公式。

## Rust 组件选择

版本栏记录调研时的已发布稳定版本，便于复查。实际实现时锁定完整依赖图和 feature，并用统一基准选择版本组合。传输层安全（Transport Layer Security，TLS）负责认证对端并加密网络字节；下表中的网络库仍各自负责 socket、TLS 与 WebSocket 的不同层次。

| 能力 | 建议 | 官方证据与版本快照 | 替代与取舍 |
| --- | --- | --- | --- |
| 热网络事件循环 | `mio` 候选；显式管理 readiness、读写进度和每轮处理预算 | `mio 1.2.4`，MIT；[Poll 文档][c-mio]、[注册版本][c-mio-meta] | `Tokio current_thread` 是必须保留的对照。mio 给调度控制，不自动带来更低尾延迟；自己承担重连、定时器和公平性 |
| 控制面与恢复任务 | Tokio 处理元数据、HTTP 快照、订阅管理、管理接口和慢任务；同步内核保持普通 Rust 调用 | `tokio 1.54.0`，MIT；[运行时文档][c-tokio]、[注册版本][c-tokio-meta] | 多线程 Tokio 用 work stealing；current-thread 不迁移自身任务，但仍有任务调度。避免把阻塞磁盘、长计算和交易状态更新混在同一 executor |
| TLS | `rustls`，热路径显式驱动 `read_tls`／`process_new_packets`／`write_tls`；Tokio 侧可用适配层 | `rustls 0.23.46`，Apache-2.0 OR ISC OR MIT；[官方文档][c-rustls] | rustls 不负责 socket、DNS 或文件 I/O；默认密码学 provider 是 AWS-LC，另有 ring。启动时确定 provider、证书根与配置，不在逐笔处理中重新构造 |
| WebSocket | mio 候选用 `tungstenite`；Tokio 候选用 `tokio-tungstenite` | 两者 `0.30.0`，分别 MIT OR Apache-2.0、MIT；[项目][c-tungstenite]、[写缓冲配置][c-ws-config]、[Tokio 适配层][c-tokio-ws] | `fastwebsockets 0.10.0` 为对照候选，Apache-2.0。它暴露 frame，需要按使用方式处理分片和协议完整性；只在完整业务链基准有收益时换用。[文档][c-fastws] |
| 跨线程有界队列 | 线程关系明确时用 `rtrb` | `rtrb 0.4.0`，MIT OR Apache-2.0；[文档][c-rtrb] | 单生产者单消费者（Single Producer Single Consumer，SPSC）队列只允许一侧各有一个线程操作。容量固定；构造后队列本身不再分配，满时返回错误，不覆盖。没有自动等待／唤醒。确需多生产者多消费者时才评估 `crossbeam_queue::ArrayQueue`。[ArrayQueue][c-arrayqueue] |
| 内核索引与散列表 | instrument、account、strategy 使用整数索引与预分配数组；订单映射先用预留容量的标准库表 | 可选 `hashbrown 0.17.1`、`ahash 0.8.12`，均 MIT OR Apache-2.0；[hashbrown][c-hashbrown]、[aHash][c-ahash] | 性能剖析证实散列热点后再引入。aHash 输出可能随机器和版本变化，不能用作持久 ID 或回放状态摘要；映射遍历顺序不能决定交易决策 |
| 价格、数量和金额 | 自有类型 `PriceTicks`、`QtyLots` 和带币种／尺度的整数金额；乘法中间值用有检查的更宽整数 | `rust_decimal 1.43.0`，MIT；[Decimal 表示][c-decimal] | `rust_decimal` 用于配置解析、报表和参考计算；其 96-bit mantissa／十进制 scale 适合精确十进制，不应未经测量放进每笔盘口更新。所有舍入规则显式指定，禁止先转 `f64` 再量化 |
| JSON | 类型化 `serde_json` 作为正确性基线；尽量借用输入字符串、复用缓冲 | `serde_json 1.0.154`，MIT OR Apache-2.0；[文档][c-serde-json] | `simd-json 0.18.1` 可作实测候选，MIT OR Apache-2.0；其借用接口需要可变输入缓冲，跨线程传递前必须解决生命周期。先比较实际 Binance 消息，不用通用 JSON 吞吐结果替代。[文档][c-simd-json] |
| 延迟直方图 | 每个测量线程独立记录 `hdrhistogram`，离线／后台汇总 | `hdrhistogram 7.6.0`，MIT OR Apache-2.0；[Histogram API][c-hdr] | 预设范围并禁用自动扩容；超界单独计数，不能用饱和记录隐藏尾部。输出原始与可选校正分布，明确单位与测量边界 |
| 属性测试 | `proptest` 生成协议输入和事件序列，比较小型参考状态机 | `proptest 1.12.0`，MIT OR Apache-2.0；[文档][c-proptest]、[状态机测试][c-state-machine] | 放在开发依赖。若采用 `proptest-state-machine`，另行锁定其版本；普通序列生成即可覆盖首批关键不变量，无需为框架本身增加抽象 |
| 自有原子／并发原语测试 | 仅在引入自有并发代码时使用 Loom | `loom 0.7.2`，MIT；[文档][c-loom] | Loom 要求被测同步操作使用它的类型；仅把测试包在 `loom::model` 中不会自动覆盖第三方队列和 OS 调用 |

### mio 与 Tokio 的实际边界

mio 的 readiness 可能是伪就绪；一次可读通知也不代表只读一次即可。适配器应按非阻塞语义持续读写到 `WouldBlock`，记录 TLS 与应用层尚未发送的缓冲，并根据需要更新 writable interest。为了避免行情突发长期饿死回报和计时器，每轮处理预算必须与后续 readiness 重试机制配合。[mio Poll][c-mio]

建议比较两个完整候选：一是专用 OS 线程上的 mio reactor，二是专用 OS 线程上的 Tokio current-thread runtime；两者调用同一个同步 core。若把 feed、core 和 order gateway 分到不同线程，则每个边界都增加一次排队和缓存所有权转移。首版不要仅因为“多核”就拆出更多线程。[Tokio 调度行为][c-tokio]

WebSocket 的发送策略必须显式配置。tungstenite 默认 write buffer 目标为 128 KiB，`write_buffer_size(0)` 会积极写出消息，`flush` 会推动缓冲写出；`max_write_buffer_size` 默认无限制。订单通道需要有界缓冲与明确 flush 策略，同时保留部分写入位置，不能在 `WouldBlock` 后把整条业务命令重新发送。[WebSocketConfig][c-ws-config]

队列元素也必须有界。即使 `rtrb` 不分配，元素内的 `String`／`Vec` 仍会分配；跨线程应传定长事件、拥有所有权的缓冲句柄或有代次校验的池索引。多个网络线程各自使用一条 SPSC 队列，再由状态所有者确定消费次序；同一线程内的多个任务可以串行使用同一 producer，但必须保持单一生产者约束。[rtrb][c-rtrb]

### FIX 与 SBE

金融信息交换协议（Financial Information eXchange，FIX）包含会话规则和业务消息。Rust 的 `fefix` 可作为编解码、字典和校验的候选，但其已发布文档明确说它不是完整 FIX engine。注册表最新稳定版仍为 `0.7.0`，发布于 2021-10-26；官方仓库 `main` 研究快照是 [`ca2bbe4c6461108646f35f7cc9245bf1848ec368`](https://github.com/ferrumfix/ferrumfix/commit/ca2bbe4c6461108646f35f7cc9245bf1848ec368)，2026-04-15。仓库主干与发布包不能混为一个经过生产验证的版本。[fefix 文档][c-fefix]、[发布元数据][c-fefix-meta]

首版若接 FIX，应独立实现并验收 Binance 的有限会话状态机：认证、消息边界、序号、心跳、TestRequest、断线恢复、ExecutionReport 处理和跨通道对账。按 Binance 方言选择能力；当前 Spot FIX 规范要求 Logon 中 `ResetSeqNumFlag=Y`，不能套入通用 FIX 的跨连接消息重发假设，也不能从支持 Spot FIX 推导 Futures 有同一入口。[Binance FIX 规范][binance-fix]

简单二进制编码（Simple Binary Encoding，SBE）用 schema 指定消息布局。官方生成器现位于 `aeron-io/simple-binary-encoding`，固定研究提交为 [`059a2c0b9913268750543f11bc6c71db8299e767`](https://github.com/aeron-io/simple-binary-encoding/commit/059a2c0b9913268750543f11bc6c71db8299e767)，2026-10-19，Apache-2.0；该主干 `version.txt` 为 `1.42.0-SNAPSHOT`。代码内确有 Rust 生成器和 Rust 扩展 schema 测试，不必从 Nautilus 复制 LGPL 代码。[RustGenerator][sbe-generator]、[Rust 测试][sbe-test]、[版本][sbe-version]、[LICENSE][sbe-license]

建议流程是固定 Binance schema 文件及哈希、固定生成器发布产物或提交及哈希，生成源码并提交仓库，再在持续集成中检查可重复生成。行情 SBE、请求／响应 SBE、FIX SBE 的 schema 和外层封装必须分别管理。当前 Binance FIX SBE 使用简单开放帧头（Simple Open Framing Header，SOFH）标识消息长度和编码类型，解码器需先处理这个外层帧头，再进入相应的 SBE schema。[Binance FIX SBE][binance-fix]

生成器的 `LibRsDef` 会产生 `#![forbid(unsafe_code)]`，但读取字节使用 `slice[index..index+N]` 和直接索引；安全 Rust 仍可在截断报文上 panic。进入生成 codec 前，应校验消息长度、schema ID、template ID、version、block length、重复组与变长字段的长度范围，并定义未知版本的拒绝／兼容策略；用截断和畸形长度 fuzz 覆盖这些入口。[生成器缓冲访问实现][sbe-buffer]

## 复用与许可交付边界

| 来源 | 本轮建议 | 引入代码时的具体动作 |
| --- | --- | --- |
| NautilusTrader | 借鉴设计；默认不复制核心或适配器源码 | 若改为直接依赖，记录 LGPL-3.0-only，并按实际链接和分发方式落实通知、相应源码与重新链接／替换要求 |
| Barter-rs | 有选择地复用 MIT 类型、工具和测试思路 | 保留 MIT 版权／许可；逐个替换不适用的 Binance 规则，不能只保留旧测试然后宣称满足新规范 |
| hftbacktest | 优先作为离线研究依赖 | 保留 MIT 版权／许可；把模拟假设、输入转换和实盘模型差异写入研究结果 |
| Rust 基础组件 | 选择功能明确的少数直接依赖 | 按发布包记录 SPDX、版本、来源和 Cargo feature；保留各自通知，检查传递依赖 |
| SBE 生成器与 schema | 自行生成并保留来源链 | 分别记录生成器、schema、生成产物的来源与适用许可；保留生成器模板带入的通知 |

许可结论依据上文固定文件和注册包声明；交付义务按实际采用的源文件、依赖和分发方式落实。

## 进入实现时的验证顺序

1. **先锁定行为。** 为 Spot／Futures 各建立 depth snapshot＋delta 参考实现和执行状态机；覆盖重复、重叠、缺口、乱序、撤单与成交交叉、回报先于请求响应、连接失效后的未知状态。
2. **验证经济不变量。** 同一成交只入账一次；累计成交不倒退；未确认发送失败不释放资金；同一风险域的并发意图不能超额预留；reduce-only 在已选仓位模式下不能增加可执行敞口。
3. **建立确定性回放。** 相同接收序号、元数据版本、参数、时间输入和随机种子必须产生相同决策与状态；数据分批边界不能改变结果。模拟器另加入口延迟、响应延迟与排队模型。
4. **比较整条业务链。** 使用同一份真实报文，比较 mio 与 Tokio current-thread、WebSocket 库、JSON 解码器及数据布局；包含 TLS、解析、订单簿、策略、风控、编码和排队。分别报告本地处理延迟、排队延迟和外部网络／交易所耗时，不用单个函数的吞吐替代下单链路结论。
5. **保留尾部。** 每项结果记录硬件、CPU 亲和性、编译参数、输入速率／突发、样本数、p50／p99／p99.9／最大值、超界样本和队列高水位。使用外部固定到达率输入暴露排队；若采用 HdrHistogram 的 coordinated-omission 校正，原始与校正结果分别存储。[直方图 API][c-hdr]

[n-cargo]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/Cargo.toml#L51-L56
[n-license]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/LICENSE
[n-kernel]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/system/src/kernel.rs#L16-L27
[n-risk-submit]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/risk/src/engine/mod.rs#L587-L664
[n-risk-reserve]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/risk/src/engine/mod.rs#L1259-L1293
[n-risk-gateway]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/risk/src/engine/mod.rs#L1906-L1989
[n-exec-submit]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/execution/src/engine/mod.rs#L2168-L2196
[n-exec-reconcile]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/execution/src/engine/mod.rs#L1135-L1168
[n-cash]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/model/src/accounts/cash.rs
[n-margin]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/model/src/accounts/margin.rs
[n-futures-balance]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/adapters/binance/src/futures/execution.rs#L367-L406
[n-futures-mode]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/adapters/binance/src/futures/execution.rs#L1859-L1873
[n-margin-model]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/model/src/accounts/margin_model.rs#L277-L337
[n-book]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/model/src/orderbook/book.rs#L43-L72
[n-ladder]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/model/src/orderbook/ladder.rs
[n-backtest]: https://github.com/nautechsystems/nautilus_trader/blob/42c3ca5f8f8d537f69772277e77792bb5b307ea5/crates/backtest/src/engine.rs#L882-L955
[b-cargo]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter/Cargo.toml#L1-L8
[b-license]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/LICENSE
[b-engine]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter/src/engine/mod.rs#L75-L113
[b-run]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter/src/engine/run.rs
[b-instruments]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter/src/engine/state/instrument/mod.rs#L48-L82
[b-assets]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter/src/engine/state/asset/mod.rs#L31-L90
[b-generate]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter/src/engine/action/generate_algo_orders.rs#L46-L66
[b-risk]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter/src/risk/mod.rs
[b-binance-empty]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter-execution/src/client/binance/mod.rs
[b-spot-depth]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter-data/src/exchange/binance/spot/l2.rs#L205-L274
[b-futures-depth]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter-data/src/exchange/binance/futures/l2.rs#L212-L281
[b-replica]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter/src/engine/audit/state_replica.rs#L73-L114
[b-backtest]: https://github.com/barter-rs/barter-rs/blob/9770b27a83f844472b93b593b08063affc974b0d/barter/src/backtest/mod.rs#L196-L215
[h-cargo]: https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/Cargo.toml#L1-L19
[h-license]: https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/LICENSE
[h-backtest]: https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/backtest/mod.rs#L755-L857
[h-latency]: https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/backtest/models/latency.rs#L13-L98
[h-queue]: https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/backtest/models/queue.rs#L24-L95
[h-roi]: https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/depth/roivectormarketdepth.rs#L13-L95
[h-futures-om]: https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/connector/src/binancefutures/ordermanager.rs#L17-L108
[h-spot-om]: https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/connector/src/binancespot/ordermanager.rs
[h-state]: https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/backtest/state.rs
[h-asset]: https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/backtest/assettype.rs
[h-fill-doc]: https://hftbacktest.readthedocs.io/en/latest/order_fill.html
[binance-spot-depth]: https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly
[binance-futures-depth]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
[binance-fix]: https://developers.binance.com/en/docs/products/spot/fix-api
[c-mio]: https://docs.rs/mio/latest/mio/struct.Poll.html
[c-mio-meta]: https://crates.io/api/v1/crates/mio/1.2.4
[c-tokio]: https://docs.rs/tokio/latest/tokio/runtime/
[c-tokio-meta]: https://crates.io/api/v1/crates/tokio/1.54.0
[c-rustls]: https://docs.rs/rustls/0.23.46/rustls/
[c-tungstenite]: https://github.com/snapview/tungstenite-rs
[c-ws-config]: https://docs.rs/tungstenite/0.30.0/tungstenite/protocol/struct.WebSocketConfig.html
[c-tokio-ws]: https://docs.rs/tokio-tungstenite/0.30.0/tokio_tungstenite/
[c-fastws]: https://docs.rs/fastwebsockets/0.10.0/fastwebsockets/
[c-rtrb]: https://docs.rs/rtrb/0.4.0/rtrb/
[c-arrayqueue]: https://docs.rs/crossbeam-queue/0.3.14/crossbeam_queue/struct.ArrayQueue.html
[c-hashbrown]: https://docs.rs/hashbrown/0.17.1/hashbrown/
[c-ahash]: https://docs.rs/ahash/0.8.12/ahash/
[c-decimal]: https://docs.rs/rust_decimal/1.43.0/rust_decimal/
[c-serde-json]: https://docs.rs/serde_json/1.0.154/serde_json/
[c-simd-json]: https://docs.rs/simd-json/0.18.1/simd_json/
[c-hdr]: https://docs.rs/hdrhistogram/7.6.0/hdrhistogram/struct.Histogram.html
[c-proptest]: https://docs.rs/proptest/1.12.0/proptest/
[c-state-machine]: https://proptest-rs.github.io/proptest/proptest/state-machine.html
[c-loom]: https://docs.rs/loom/0.7.2/loom/
[c-fefix]: https://docs.rs/fefix/0.7.0/fefix/
[c-fefix-meta]: https://crates.io/api/v1/crates/fefix/0.7.0
[sbe-generator]: https://github.com/aeron-io/simple-binary-encoding/blob/059a2c0b9913268750543f11bc6c71db8299e767/sbe-tool/src/main/java/uk/co/real_logic/sbe/generation/rust/RustGenerator.java
[sbe-buffer]: https://github.com/aeron-io/simple-binary-encoding/blob/059a2c0b9913268750543f11bc6c71db8299e767/sbe-tool/src/main/java/uk/co/real_logic/sbe/generation/rust/LibRsDef.java#L178-L230
[sbe-test]: https://github.com/aeron-io/simple-binary-encoding/blob/059a2c0b9913268750543f11bc6c71db8299e767/rust/tests/extension_test.rs
[sbe-version]: https://github.com/aeron-io/simple-binary-encoding/blob/059a2c0b9913268750543f11bc6c71db8299e767/version.txt
[sbe-license]: https://github.com/aeron-io/simple-binary-encoding/blob/059a2c0b9913268750543f11bc6c71db8299e767/LICENSE
