# Tickwire 首版设计：本地技术材料依据

本文保留调研时的源码/协议事实与推导背景；运行架构和生产要求以 [v0.3 主设计](../design/binance-ultra-low-latency.md)、[共享内存契约](../design/shared-memory-ipc.md)与[策略生命周期](../design/strategy-lifecycle.md)为准。早期关于单进程或首版取舍的建议不再作为当前部署方案。

首版同时支持现货与合约，建议以**风险域内单写者、尽量连续处理的交易循环、有界通信、可恢复订单状态和分段延迟测量**为基础。低延迟优化的第一步是减少不必要的交接和等待；现有材料不足以为 Rust、币安接口或目标服务器给出纳秒级性能承诺。

本笔记于 2026-10-20 阅读 `/home/obscure/TradingSystemLearning` 的本地材料整理，证据固定到来源仓库提交 `332a395dbbdaa32c2aab2e0d5b539385cdfe2488`，全部行号均指该提交。网络篇在本次工作期间出现并发编辑，因此工作区中的当前行号可能不同；可在源仓库用 `git show 332a395:hft/HFT_Engineering_06_网络通信.md` 查看对应版本。本次没有运行文章实验，没有接入交易所，没有联网核对接口与第三方库现状。下文的“首版建议”和“实验方案”是对材料的工程推导；币安各产品的接口、权限及订单语义由接口调研另行确定。

## 1. 现有证据能支持什么

### 1.1 用户仓库已有的实验与验证记录

| 本地记录 | 已记录的结果和范围 | 对 Tickwire 的用途 |
|---|---|---|
| [Disruptor 验证，L3–25](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/disruptor_cpp20/VALIDATION.md#L3) | Linux、GCC 13.3.0、C++20；Release、TSan、ASan/UBSan 各 28/28；四生产者示例验证 40,000 条。原文明确没有性能测量，也没有完成泄漏检查。 | 复用环绕、背压、发布空洞、退出和消费者失败等测试场景；不能当作 Rust 队列的正确性或延迟证明。 |
| [Stop-bit 示例，L6–21](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/examples/stop_bit/README.md#L6) | README 记载 Apple M1 Pro、Clang 17 的正确性和合成平均耗时基准；未绑核、未使用真实行情，也未测网络或 p99。此次未找到配套原始跑分文件，不引用其数值。 | 借鉴截断输入、跨缓冲区解析和独立参考实现；不是币安协议实现。 |
| [SBE 示例，L19–39](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/examples/components_07/README.md#L19) | macOS ARM64，验证 4/4 版本交叉解码、90/90 截断前缀、独立 Python 字节核对及视图别名。 | 借鉴按 schema 版本与长度校验的测试方法；不是当前币安 SBE schema 的验证，也没有性能结果。 |
| [回测实验，L17–27](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/examples/backtest_09/README.md#L17)、[结果 CSV，L1–14](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/examples/backtest_09/results/summary.csv#L1) | 确有随文生成的 13 个合成场景结果。所有时间是虚拟微秒，单价位、全可见 FIFO；基础场景成交 15 手，错误的碰价成交算法记为 20 手。 | 验证事件顺序、撤单竞态、数量守恒；不能作为真实行情、网络延迟或收益结果。 |
| [时钟采样示例，L83–89](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/examples/clock_stability/README.md#L83) | 已运行合成测试及离线 CLI；Linux 采样、真实网卡 PHC 尚未实机验证。 | 借鉴整数时间、采样窗口和拟合检查；没有硬件同步精度结论。 |
| [20 µs 等待示例，L40–46](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/examples/precise_sleep/README.md#L40) | 只编译和静态审阅，没有运行等待实验。 | 可以扩展成定时回放实验；20 µs 是设定值，10 µs 忙等窗口是演示参数。 |

上述记录属于仓库已有证据，本次仅检查了文件。没有找到 Rust 币安行情到发单、订单确认往返、现货与合约共置运行的本地基线。

### 1.2 文章引用的外部测量

| 外部数字 | 原本条件和测量边界 | 可迁移的依据 |
|---|---|---|
| 订单簿更新中位数约 33/63/34/32/29/22 ns | Gross 的 C++ 演讲；一周、数十只大型股票的行情，单边约千级价格档位；33→63 是改变堆布局后的基准差异，后续数字是容器与搜索调整。笔记明确未重跑。[来源 L87–166](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/literature/When_Nanoseconds_Matter_详细解读.md#L87) | 输入深度分布和内存布局比单看复杂度更重要。先比较真实币安 L2 更新下的紧凑数组、查找及插删成本。 |
| 期权重估 p50 从 510 到 329 ns、p90 从 1,012 到 550 ns | i9-14900KS，4 个隔离 P 核、SMT 关闭，核心 5.90 GHz、TSC 3.1872 GHz；每 5 µs 一个 192 B 合成事件，更新 64 个期权。最终计时结束后还有清除缓存、屏障和统计工作；不含协议、风控和发包。[环境 L39–76](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/literature/The_Latency_Slippage_详细解读.md#L39)、[边界 L340–366](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/literature/The_Latency_Slippage_详细解读.md#L340) | 研究数据依赖、已有工作内部的并行和跨核交接；不能直接选择 D=4、40/24 分工或 CLWB。 |
| SPSC 往返 133 ns | 第 03 篇引用 Rigtorp：Ryzen 9 3900X 同一 chiplet 的不同核，两个队列传递 `int`，测双向往返。[L666–678](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_03_C++线程通信与性能测量.md#L666) | 一个队列调用、较大事件交接和 TLS 发送均需独立测量；133 ns 不是可直接写入预算的单次入队成本。 |
| 持仓查询 p99 <25 ms、推送 <30 ms，持久预扣放行 <5 ms | 分布式持仓设计的目标和估算，包含跨 AZ Kafka 事务，原文标为待压测。[L62–71](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/system_design/distributed_account_service.md#L62)、[L1433–1451](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/system_design/distributed_account_service.md#L1433) | 借鉴状态和提交协议，不把该服务复制到每笔订单的发送链路。 |

## 2. 交易循环、内存与队列

### 2.1 先按状态所有权划分

第 04 篇直接列出单进程集成行情、订单簿、策略、风控、网关的布局，并指出拆成三个进程至少增加两次 IPC；扩展时优先按独立状态分片。[L51–91](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_04_高频交易系统的部署.md#L51)

首版建议让一个固定线程拥有一个交易单元的可变状态，连续完成行情事件处理、策略、限额检查、订单状态转换和编码。现货与合约使用独立适配器和状态模型；是否共用线程由负载实验决定，不能仅因业务模块名称不同就添加线程。网络 I/O 若必须由另一线程推进，则在测量中显式包含交接和队列等待。

**风险域比标的更重要。** 同一现货钱包中的多个标的可能共享同一种资产，多个合约也可能共享一组保证金。首版选择由一个风险域单写者管理共享额度；只有在控制面先分配了互不重叠、总和受控的额度后，才允许多个交易单元独立检查。分布式持仓文档本身要求同一账户的“检查 + 预扣”串行执行，并明确账户聚合结果不能代替严格风控。[L775](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/system_design/distributed_account_service.md#L775)、[L1176–1178](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/system_design/distributed_account_service.md#L1176)

### 2.2 将零分配定义为可验证的区间

第 02 篇将预分配、固定上限、复用缓冲区和对象池作为减少扩容、分配竞争与首次缺页的办法，超过容量时显式失败。[L172–213](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_02_C++内存与计算优化.md#L172)

首版将“稳定连接下，完整消息交付后至订单编码完成”定义为首个零堆分配目标。在 Rust 中使用有容量上限的复用缓冲区、紧凑标的 ID、整数价格和数量、预分配订单槽位；容量耗尽进入明确错误分支，不在运行中悄悄扩容。接收、TLS 解密、WebSocket 重组和发送另设分配计数，只有整段通过测试后才扩大零分配声明的范围。

解析器优先从已有字节切片提取字段，再转换为拥有自身存储的内部数值。跨线程队列不能保留会被下次接收覆盖的借用视图；可传定长值，或显式移交缓冲区所有权。数据复制成本和生命周期应一起评估。[第 02 篇 L314–349](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_02_C++内存与计算优化.md#L314)、[第 06 篇 L199–213](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_06_网络通信.md#L199)

价格档位可先比较预留容量的连续布局和常规有序容器。真实更新集中于最优价附近时，再测试从热点端扫描；深档更新、快照重建、价位大量插删需要单独覆盖。预留容量不能保证插删后的索引或元素地址保持不变。[第 02 篇 L404–429](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_02_C++内存与计算优化.md#L404)

### 2.3 每条队列都需要满载语义

单生产者单消费者（Single Producer Single Consumer，SPSC）队列适用于一条明确的线程边界。第 03 篇同时要求“写完后发布”和“读完后归还”两个方向的 acquire/release 同步，普通槽位只能在消费者完成后复用；写、读游标还应避免伪共享。[L66–161](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_03_C++线程通信与性能测量.md#L66)

| 消息类型 | 首版满载行为 | 保留的业务语义 |
|---|---|---|
| 用于重建订单簿的增量 | 标记该簿失效、停止依赖它的新交易并恢复快照与增量衔接 | 不能任意丢一条后继续宣称簿完整。 |
| 允许覆盖的最新状态，如已归一化的最优报价 | 仅在策略明确接受合并时保留最新值，并记录版本、年龄和跳过数量 | 不把这项策略用于逐笔成交或订单回报。 |
| 新单意图 | 在进入发送状态前拒绝或停止产生；已有意图检查有效期 | 不依靠无限排队延迟暴露过载。 |
| 订单回报、风控事件、关键日志 | 预留容量；持续无法接纳时停止新风险并进入恢复流程 | 不静默覆盖，也不把“诊断允许丢弃”推广到订单事实。 |
| 指标样本和普通诊断 | 不阻塞交易；丢弃计数、导出滞后和最后成功时间可见 | 性能结果同时报告样本损失。 |

这些行为由材料中的有界发送、诊断日志允许丢弃及 Aeron 的消息语义划分推导。[第 06 篇 L217–235](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_06_网络通信.md#L217)、[第 03 篇 L164–194](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_03_C++线程通信与性能测量.md#L164)、[第 07 篇 L35–52](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_07_开源项目解析.md#L35)

每轮可消费已经到达的有限批次，不等待凑齐。订单回报、撤单、停机控制应有有界的处理机会，避免单次行情批量处理饿死其他通道。后者是首版事件循环的调度约束，需要通过满载实验验证。

不移植演讲的覆盖式 FastQueue 演示：本地笔记明确指出读者复制普通字节时可能被生产者覆盖，事后校验和 acquire/release 不能补救已发生的数据竞争。选择可证明槽位生命周期的实现，分别测试发布空洞、环绕、消费者停顿和关闭。[L250–260](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/literature/When_Nanoseconds_Matter_详细解读.md#L250)

## 3. CPU、NUMA 与网络

### 3.1 把核、网卡和热内存作为同一个部署单元

非一致内存访问（Non-Uniform Memory Access，NUMA）表示不同处理器访问各内存节点的成本不同。第 04 篇强调 socket、NUMA node、末级缓存与逻辑 CPU 不是同一概念；同一物理核的 SMT 兄弟线程仍共享执行资源。[L161–176](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_04_高频交易系统的部署.md#L161)

首版在目标主机记录 CPU 拓扑、允许的核心、网卡 NUMA 节点、RSS 接收队列和 IRQ 实际位置，再决定亲和性。热状态由目标线程在目标节点分配并触页，检查锁页结果；为日志、恢复、控制和系统工作留出独立资源。不要把文章示意的 CPU 2/3/4 写成产品默认值。[准备顺序 L275–284](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_04_高频交易系统的部署.md#L275)

绑核限制运行位置，不能证明独占或隔离。同机日志、监控和其他策略还会争用末级缓存、内存带宽和互连；至少比较独立运行、预期共置、共置压力三组负载。[L213–249](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_04_高频交易系统的部署.md#L213)

`nohz_full`、RCU 迁移、深 C-state 限制和大页分别解决不同问题。原文关于移走设备 IRQ 的方案以“用户态持续轮询、kernel bypass、交易核不承担普通 socket 和文件 I/O”为条件；首版使用内核 TCP 时不能整体照搬。[L254–269](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_04_高频交易系统的部署.md#L254)

### 3.2 内核 TCP、TLS 与 WebSocket 先形成完整基线

首版可从内核 TCP 长连接、TLS、非阻塞收发和固定线程事件循环建立基线。传输层安全协议（Transport Layer Security，TLS）负责认证和加密；它的记录处理与 WebSocket 消息重组应明确纳入测量。行情、订单请求与私有回报的具体连接组合取决于各币安产品的已核实接口。

| 选择 | 本地依据与首版取舍 |
|---|---|
| `TCP_NODELAY` | 用于减少 Nagle 对短消息的等待，但拥塞窗口和发送队列仍存在；同时确认 TLS/WebSocket 层实际提交字节的时机。[第 06 篇 L133–142](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_06_网络通信.md#L133) |
| 接收缓冲区 | 预分配并复用，容量覆盖协议最大允许消息和预期突发；持续过载时更大缓冲只增加陈旧度。[L199–235](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_06_网络通信.md#L199) |
| 忙轮询 | 单独比较就绪通知、用户态非阻塞轮询和支持时的 `SO_BUSY_POLL`；后者是内核 NAPI 轮询，用户态反复 `recv` 不等价。[L98–117](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_06_网络通信.md#L98) |
| NIC 合并、RSS、GRO/TSO | 记录原配置、单因素调整、读回生效值，比较尾延迟、PPS 和 CPU。文章支持逐项测量，没有支持全部关闭。[L80–94](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_06_网络通信.md#L80)、[L393](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_06_网络通信.md#L393) |
| TLS 与帧边界 | 一个 `recv` 可能是半条或多条消息；先完成 TLS 处理、WebSocket 控制帧和消息重组，再消费业务消息。压缩若可配置，应独立做延迟与带宽对照。[L480–490](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_06_网络通信.md#L480) |
| JSON/SBE | 分别实现到同一内部事件模型的适配器。文章说明 WebSocket 也能承载 SBE，不能把“二进制”与“没有 TLS/帧处理”混为一谈；币安现货的能力不能推定为合约能力。[L459–488](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_06_网络通信.md#L459) |
| kernel bypass | 普通内核路径出现可归因瓶颈且硬件、驱动、完整 TCP/TLS 接入可用时再评估。DPDK 原始包接口本身不提供完整 TCP 语义；RDMA 需要双方支持。[L411–445](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_06_网络通信.md#L411) |
| 发送零拷贝 | 暂不作为短订单的默认优化；页面固定、完成通知和缓冲生命周期有成本，完成通知也不等于交易所接受。[L313–316](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_06_网络通信.md#L313) |

公网或云上部署位置先依据真实可访问入口做区域与时段对照。第 04 篇以 Coinbase 的 PrivateLink 举例，不能据此宣称币安提供相同接入；同区域名称也不能替代实际网络测量。[L319–332](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_04_高频交易系统的部署.md#L319)

## 4. 时钟和延迟统计

### 4.1 完成点必须进入指标名称

第 03 篇明确区分应用取得行情、订单准备完成、`send()` 返回、网卡发送、交易所接受和成交。[L405–415](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_03_C++线程通信与性能测量.md#L405)

首版建议记录同一事件和订单的以下边界：

| 标记 | 精确定义 |
|---|---|
| `t_rx_socket` | 应用取得本次传输字节的位置；只代表用户态接收边界。 |
| `t_msg_ready` | TLS/WebSocket 完整业务消息已可供解析。 |
| `t_event_ready` | 协议已解析，得到规范化行情或私有事件。 |
| `t_decision` | 策略产生订单意图。 |
| `t_risk_ok` | 本地风险检查与预扣完成。 |
| `t_encoded` | 订单业务报文编码完成。 |
| `t_tx_accepted` | 相应密文字节全部由本地传输层接受；若只写入用户态缓冲，另记该位置，不混称完成。 |
| `t_ack_local` | 收到并识别对应交易所接受或拒绝回报。 |
| `t_fill_local` | 收到并识别对应成交回报。 |

`t_encoded - t_msg_ready` 是应用处理；`t_tx_accepted - t_rx_socket` 包含更多本地协议和排队；请求发送至回报是本地可测的往返。多次部分接收、TLS 记录或 WebSocket 帧拼接必须定义本条消息关联的最早/最终传输位置，不能机械地把当前批次一个时间戳分配给所有消息。若有匹配的 NIC 收发硬件时间戳，另报主机线侧延迟。

先对每个事件计算总时间，再计算总分位数，不能将各段 p99 相加。完整处理循环还包含采样、清理和其他收尾，要与业务区间同时观察。[第 03 篇 L601–605](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_03_C++线程通信与性能测量.md#L601)、[第 05 篇 L358–376](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_05_高频交易系统中的时钟.md#L358)

### 4.2 先保证时间可比较

本地超时、事件年龄和常规延迟使用单调时钟；日历时间用于外部时间记录。原文分别解释了 `MONOTONIC`、`MONOTONIC_RAW` 与 `REALTIME` 的用途，RAW 不代表更接近 UTC。[第 05 篇 L229–240](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_05_高频交易系统中的时钟.md#L229)

处理器时间戳计数器（Time Stamp Counter，TSC）适合经过平台验证的低成本打点。启用前检查固定计数频率、迁移、跨核偏差、编译器和 CPU 排序，并测空窗口；用 TSC 频率换算，不能用睿频。Intel 的 fenced TSC 示例也不能原样推广到所有处理器。[第 03 篇 L480–534](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_03_C++线程通信与性能测量.md#L480)、[第 05 篇 L299–328](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_05_高频交易系统中的时钟.md#L299)

网卡精密时间协议硬件时钟（PTP Hardware Clock，PHC）与 TSC 属于不同时钟域。比较前维护映射、校准版本和采样不确定性；云主机没有可用 PHC 时，只报告实际可获取的边界。交易所时间减本地时间同时含有时钟偏移和字段语义差异，不能无条件标为单向网络延迟，更不能把往返简单除二当作准确单程。[第 05 篇 L824–830](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_05_高频交易系统中的时钟.md#L824)、[L954–962](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_05_高频交易系统中的时钟.md#L954)

### 4.3 直方图要保留慢样本

热线程只写固定字段和整数时间到预分配的线程私有缓冲，后台统计、格式化、导出。队列满时记录样本丢失；定时统计线程对缓存和带宽的干扰也计入共置实验。[第 03 篇 L608–663](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_03_C++线程通信与性能测量.md#L608)

业务区间和订单往返使用不同量程的直方图，记录桶宽或有效精度、p50/p90/p99/p99.9/max、样本量、溢出数和丢样数，并保留有限数量的慢事件及关联 ID。禁止静默饱和到末桶。原文的 512 桶 ×16 TSC ticks 仅覆盖约 2.57 µs，3 µs 和 30 µs 都会被压入末桶。[第 05 篇 L380–388](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_05_高频交易系统中的时钟.md#L380)

## 5. WAL、订单恢复与控制面

### 5.1 日志可见和日志持久是两个水位

预写日志（Write-Ahead Log，WAL）的含义取决于外部动作是否等待其持久化。异步留档能用于分析和辅助恢复，但“已入队”“已写页缓存”和“已同步到稳定存储”必须分开。第 07 篇明确区分 mmap 可见性、异步写回和同步持久化，并指出进程退出与机器掉电是不同故障。[L88–116](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_07_开源项目解析.md#L88)

首版保留有界的结构化订单日志通道，由单独写入者追加日志并报告已接纳、已写入和已持久化序号。记录序号、格式版本、长度、校验和以及配置/启动代次，恢复时处理尾部半条记录。订单意图不能在日志通道已满时继续无记录地产生；持久化水位持续落后超过策略阈值时停止增加风险。热路径不等待共享数据库或跨 AZ Kafka 事务。

有两种完整的行为契约可选，不能只保留速度好的一半：

| 契约 | 发送前保证 | 崩溃后的恢复 |
|---|---|---|
| 每单等待持久化 | 订单意图和预扣已提交后再发送 | 本地保有发送意图；仍可能不知道交易所是否接收，必须查询与对账。持久化等待计入延迟。 |
| 发送不等待每单持久化 | 在运行前持久分配启动代次、订单 ID 区间及可用风险预算，运行中本地串行预扣 | 可能丢失最后一段订单意图。重启冻结整个受影响风险域，恢复日志并对账交易所订单、资产/持仓及检查点以来的成交，未解决的占额不释放；缺口未闭合不恢复新交易。 |

第二行是面向首版低延迟目标的建议，需要用故障实验验证恢复窗口和接口可查询范围。控制面不得在旧实例失联后立即把同一额度重新发给新实例；在无法证明旧发送者已被实际隔离时，不自动接管发送。仅有租约、启动代次或数据库 fencing 不会让交易所拒绝旧进程的订单。

分布式持仓文档的 fencing 保护的是 Kafka 输出，恰好一次范围也明确限定为内部事件处理；它不能单方面保证与外部交易所的一致性。[L218–245](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/system_design/distributed_account_service.md#L218)

### 5.2 保留预扣和未知状态的语义

公开订单簿与自己的订单台账分别恢复：公开行情不能证明本人的订单是否成交。第 08 篇用 Nasdaq 的会话说明了“连接断开后，请求结果未知，先补回报或查询”的原则；快照与增量应按协议位置衔接。这些原则可用于设计币安恢复状态机，但 Nasdaq 的续传游标和消息字段不能直接移植到币安。[L3–13](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_08_交易所协议.md#L3)、[L25–33](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_08_交易所协议.md#L25)

可迁移的约束包括：金额采用定点数；状态由有序事件确定性更新；订单身份与请求修订分离；重复成交幂等处理；撤单请求不立即结束订单；迟到成交仍进入状态；快照必须绑定已提交的事件位置。[原则 L218–224](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/system_design/distributed_account_service.md#L218)、[订单规则 L454–493](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/system_design/distributed_account_service.md#L454)、[快照 L679–711](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/system_design/distributed_account_service.md#L679)

“权威持仓/订单状态已覆盖的部分 + 尚未覆盖的预扣”共同表示已占用风险；部分成交不能释放整单预扣，减量或撤单也要等确认。超时进入待核查状态，继续占额。原文 `available_exposure` 公式实际累加的是风险占用，Tickwire 应另行定义 `available = limit - used`，避免变量名称倒置。[L1180–1203](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/system_design/distributed_account_service.md#L1180)

现货实现必须维护资产可用量、挂单占用和费用预留；合约实现必须维护适用账户模式下的保证金、持仓方向、挂单风险及估值新鲜度。这里是 Tickwire 的模型需求，原持仓文档明确将现金台账和保证金计算排除在自身职责之外，不能拿其股票数量公式代替合约风险计算。[L114–124](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/system_design/distributed_account_service.md#L114)

### 5.3 控制面不逐单远程裁决

控制面负责配置版本、预算分配、启动恢复、停机与发送资格；统计和账户账本以异步事件构建查询视图。交易循环只接收已验证的版本化命令并在事件边界应用，本地执行逐单风险检查。额度变更需要确认归还和交接，不能一边旧额度可用、一边新额度已分配。第 04 篇已经将全账户额度分配放在控制面、每笔订单检查放在网关进程内。[L55–62](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_04_高频交易系统的部署.md#L55)、[L84](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_04_高频交易系统的部署.md#L84)

首版不因“低延迟”名称引入 Aeron、Chronicle Queue 或 Kafka 作为必备组件。第 07 篇明确认为单进程线程通信使用 SPSC 更直接，公网请求不适用 Aeron 的可靠 UDP 模型；Chronicle 的文中开源实现又有语言边界。只在确有多进程扇出、独立升级或长期档案需求时，按职责单独评估。[L74–86](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_07_开源项目解析.md#L74)

## 6. 可重复的延迟实验方案

以下实验尚未执行。它们建立 Rust 首版自身的基线，并验证前述取舍。

### 6.1 固定实验记录

每次运行保存一个 manifest：代码提交、未提交差异摘要、Rust 工具链和锁文件、优化/LTO/PGO 参数、操作系统和内核、CPU/微码/频率策略、SMT 与核心拓扑、线程及 IRQ 位置、NUMA/内存页策略、网卡/驱动/合并配置、TLS 和协议配置、输入文件校验和、随机种子、回放节奏、测量边界、时钟校准、直方图精度、运行起止时间及同机负载。材料直接要求保留构建、输入、部署和统计口径。[第 04 篇 L344–354](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_04_高频交易系统的部署.md#L344)

建议第一轮每个方案至少重复 5 次，交错 A/B 顺序，每次预热后采集至少 100 万条有效事件；这只是起始采样规模。p99.9 比较应保留每次结果、尾部原始样本和时间分块变化，再根据波动增加样本或运行时长，不能挑最快的一轮作结论。性能运行使用无 sanitizer 的发布构建，正确性和并发检查单独运行。

### 6.2 从局部到完整协议逐层验证

| 实验 | 固定输入与对照 | 必须输出 |
|---|---|---|
| A：时钟与观测开销 | 空窗口；单调时钟与经校准的 TSC；无打点、少量边界打点、完整分段打点；真实核心配对 | 读取成本分布、跨核偏差区间、校准漂移、插桩增量和丢样。 |
| B：解析与订单簿 | 同一现货/合约报文集合；小消息、大增量、深档、快照、分片、无效字段；常规容器与紧凑布局 | 解析及更新分布、每事件分配/字节数、容量拒绝、结果 checksum。 |
| C：线程边界 | 同线程函数调用，与同 NUMA 不同物理核 SPSC；需要时再测跨 NUMA；真实事件大小和实际队列容量 | 入队耗时、交接耗时、到达至完成、队列高水位、拒绝与满载恢复、CPU。 |
| D：完整传输 | 可控的远端 TLS/WebSocket 仿真端；业务处理、TLS 与发送均真实运行；按原始节奏送报文 | 接收、消息完整、解析、策略、风控、编码、本地提交、远端回应各边界；部分写入与发送积压。 |
| E：内核和 NIC 参数 | 同一负载逐项比较忙轮询、IRQ/RSS、合并、GRO/TSO、缓冲容量 | p50/p99/p99.9/max、吞吐、PPS、CPU、重传/丢包、消息年龄；记录实际生效配置。 |
| F：预期部署与压力 | 现货和合约同时运行；加入日志、指标、恢复流量、磁盘减速、突发输入；逐步提高到达率 | 端到端分布、排队、每产品公平性、采样缺失、丢弃、日志持久化滞后、停止新风险的触发时刻。 |
| G：恢复正确性 | 在预扣、编码、部分发送、完整发送、回报和日志提交前后注入崩溃；重复/乱序回报、断线、日志尾截断、旧进程恢复 | 不重复发未知订单、不提前释放额度、数量守恒、对账闭合、旧发送者隔离、恢复后状态一致。 |
| H：真实环境校准 | 先只读真实行情和影子决策；实际订单往返由另行授权的受控交易采样获得 | 按现货/合约、入口、时段、消息大小和负载分组，不用测试环境延迟替代生产环境。 |

仿真端以独立时间表驱动输入，不等待被测程序处理完上一条才发送下一条。保存计划到达、实际发出、接收和完成时间；发生生成器落后时报告调度误差。这样才能观察排队，避免闭环负载在系统停顿时同步停止施压。原材料已经指出消费完成后再发送下一条会漏掉过载等待。[The Latency Slippage L358–364](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/literature/The_Latency_Slippage_详细解读.md#L358)

对零分配区间，用线程局部的分配计数验证稳定负载、较大合法消息和拒绝路径；检查计数器本身的扰动。对容量和内存准备，用缺页、队列峰值和工作集记录说明预分配是否真正覆盖测试范围。超过容量时必须看到预期停止/恢复行为。

解释变快原因时，另跑性能监控单元（Performance Monitoring Unit，PMU）计数实验，观察指令、周期、缓存未命中和分支预测失败。计数窗口与业务计时窗口分别声明，保存实际处理事件数和计数器复用情况；不把每事件 ioctl 开关放入主延迟运行。材料已经揭示原 C++ 框架的计数轮会干扰节奏，且选最小均值的逻辑可能混合运行来源。[The Latency Slippage L539–558](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/literature/The_Latency_Slippage_详细解读.md#L539)

### 6.3 回放、回测与影子运行各自回答不同问题

离散事件快放验证策略与状态语义；按真实节奏回放验证程序的调度、积压和吞吐。两者使用相同业务状态转换，但时间推进方式不同。[第 09 篇 L110–124](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_09_高频回测.md#L110)

只有 L2 价位总量时，队列前后撤单不可辨识，需要模型；影子单没有真实入队，不能测真实成交优先级。先保留保守队列假设和多个参数结果，再用获得授权后的真实订单链校准成交量、等待、撤单竞态和成交后价格变化。原文提醒不能用单一总体成交率掩盖订单子集差异。[第 09 篇 L51–88](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_09_高频回测.md#L51)、[L126–132](https://github.com/Obscurepawn/TradingSystemLearning/blob/332a395dbbdaa32c2aab2e0d5b539385cdfe2488/hft/HFT_Engineering_09_高频回测.md#L126)

## 7. 设计评审时应避免的误推断

1. **329 ns、22 ns 或 133 ns 可以成为 Tickwire 的已达成目标。** 它们来自不同硬件、语言、工作负载和完成边界；本地已有验证也未提供 Rust 币安端到端基线。
2. **分工更多就更快，绑核后没有竞争。** 必须同时支付线程交接、最后完成分支等待以及共享缓存和内存资源的成本。
3. **无锁、预分配或事后校验就保证安全。** 槽位所有权、发布、归还、覆盖、退出和恢复必须各自成立，Rust 的 `unsafe` 也不能绕过这些条件。
4. **可见的 mmap 写入或异步日志保证掉电后不丢订单。** 必须明确持久化水位；发送不等持久化时，恢复依赖冻结、外部对账和完整的查询覆盖。
5. **Kafka 的 exactly-once 或 epoch 能阻止旧实例向币安发单。** 外部副作用需要实际发送资格隔离及交易所状态核对；自动接管是独立协议。
6. **现货余额和合约保证金可以用一个净持仓数字处理。** 资产占用、费用、在途状态、持仓模式和保证金域需要独立规则，额度分片必须守住共享资源总量。
7. **吞吐更高、阶段 p99 更低就是总延迟更低。** 总延迟应按同一事件计算，包含排队、协议处理和收尾造成的后续等待；各阶段分位数不能相加。
8. **纳秒输出代表纳秒准确度，交易所时间可直接减本地时间。** 时钟域、校准误差、字段语义和直方图量程决定结果能解释到哪里。
9. **客户端优化能够改变交易所推送频率或服务端排队。** NIC、TLS 和解析优化只改变可控制部分，接口和部署位置需另行核实与实测。
10. **本地回测 CSV 是真实成交验证。** 随文数据是合成事件和虚拟时间，价值在于暴露碰价成交、撤单在途、信息不足等模型错误。
