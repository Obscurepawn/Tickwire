# Tickwire 共享内存 IPC 与多消费者广播契约

版本：v0.3 · 2026-10-20 · 生产目标设计，尚未实现或测量。

行情数据面采用 **Disruptor 式共享广播 ring**：发布者写入一份事件，多个策略、执行风控和其他获授权订阅者各自读取，分别维护消费进度。策略意图与执行回报采用独立的有界点对点通道。控制面负责身份、授权、资源交付与生命周期，不参与逐笔数据转发。

本文件规定可实现、可测试的通信契约；进程职责见[系统设计](binance-ultra-low-latency.md)，独立发布与订单移交见[策略生命周期](strategy-lifecycle.md)。这里的多进程布局是正式运行架构，不以单进程原型作为上线替代物。逐字节布局、消息ID/错误码、控制响应和黄金向量由[IPC ABI](ipc-abi.md)规定；本文定义并发和生命周期语义。Market每topic4banks，SPSC每方向2banks，参考容量见[部署契约](deployment-and-slo.md)。

## 1. 三类通道与所有权

| 通道 | 拓扑与内容 | 满载或失联的语义 |
|---|---|---|
| `MarketBroadcast` | 每个 topic 一个生产者、多个独立消费者；共享事件及其 payload | 不覆盖尚未释放的槽位；高水位触发慢消费者处置或 bank 切换，耗尽则标记该数据源不可交易 |
| `StrategyIntent` | 每个 `(strategy instance, exec risk domain)` 一条 SPSC；固定大小的订单/撤改单意图 | 满时返回未入队；已提交但未收到业务 ACK 的意图按原 ID 查询，不换 ID 重发 |
| `ExecutionReport` | 每个 `(exec risk domain, strategy instance)` 一条 SPSC；订单/成交/预留及授权事件 | exec 的账本和持久事件流是事实来源；策略跟不上时撤销其增险许可，恢复后快照加回放补齐 |
| `Control` | Unix Domain Socket，跨主机使用相互认证的控制连接 | 版本化、幂等 request ID、received/applied 分开；不把本地写入成功当成远端生效 |

SPSC 是 Single-Producer Single-Consumer，即单生产者单消费者。行情广播中的多个消费者都获得所订阅 topic 的事件；意图通道则只交给指定的执行所有者。两种语义不混用。

一个 topic 由 `(venue, product, feed_shard, event_class, schema_major, qos)` 标识。不同网络输入先在所属行情分片内按实际接收次序归一化，由唯一发布者分配 `topic_seq`。多产品、多分片之间不提供虚构的交易所全局顺序；策略记录自己的输入合并次序用于回放。订阅只关注部分 symbol 的消费者仍推进其 topic 游标，过滤行为不能被误判为传输丢包。

每个执行进程独立消费风控所需行情，不使用策略提供的价格或“风控已通过”标志替代自身判断。策略与行情进程没有正常交易请求的执行凭证。

## 2. 广播 ring、消费游标与依赖屏障

### 2.1 单写发布

每个 ring 有容量 `C`、已发布序号 `published`，以及每个消费者的 `read_seq`、`applied_seq` 和 `released_seq`。`applied_seq` 表示连续完成业务处理的水位，`released_seq` 表示该消费者及其内部工作线程已经结束访问的**连续前缀**；读到消息、复制了描述符或推进 `read_seq` 都不代表 payload 已释放。

设活跃 gating 消费者集合为 `G`。发布者只有在不会越过 `min(released_seq[G])` 所保护的槽位时才能复用该槽位。序号采用有界差值的无符号单调计数，容量和未决范围小于半个计数空间，并在达到约定上限前换代；不得用环形数组下标充当跨重启身份。[Disruptor 的游标、广播和 gating 语义](https://lmax-exchange.github.io/disruptor/user-guide/)

gating 最小值可以缓存以减少逐事件扫描，但缓存只能是保守下界。每次 membership 变更、JoinSeq 变化或 bank 激活都必须在发布边界失效/重建缓存，不能使用不包含新读者的旧最小值；消费者批量推进水位的大小纳入容量和尾延迟测试。

发布顺序为：

1. 验证容量和 payload arena 余量，取得尚未被任何读者引用的槽位。
2. 写完整事件头、固定字段和变长 payload，填好版本、长度与校验信息。
3. 使用 release 操作发布序号；消费者使用 acquire 语义的观察操作后，才能访问该记录。
4. 消费者完成全部读取和业务所需复制后，以 release 推进连续释放水位；生产者以 acquire 语义读取水位后才回收。

这里的 acquire 是协议语义，不等同于在任意映射上直接调用 `load(Ordering::Acquire)`。对于只读映射，获准的 Linux x86_64/aarch64 目标使用对齐的 64 位 `Relaxed load` 加 `std::sync::atomic::fence(Acquire)`，读取到相应 release 发布后才访问 payload；写端在自己有权限的可写页做 release store。消费者读取发布序号、生产者读取只读 consumer cursor 都遵循同一规则。只读页禁止 RMW，也不依赖 Rust 未保证的直接 Acquire load；其他目标/宽度必须另行资格验证。[Rust 对只读内存原子访问的规定](https://doc.rust-lang.org/std/sync/atomic/index.html#atomic-accesses-to-read-only-memory)

内存屏障负责可见性，槽位所有权负责生命周期。不能在读者仍访问普通字节时覆盖，然后依靠“读完再比一次序号”补救。生产者在发布前崩溃的半写记录不可见；该进程恢复时创建新 `ring_epoch`，不猜测半写槽位已经有效。

### 2.2 数据只保存一份

小事件可以内联在槽位中；大深度更新使用同一代次下预分配的 payload arena。槽位记录 arena 内偏移、长度和分配代次，所有消费者看到同一份不可变 payload；arena 块与对应槽位共享释放条件。不会因为多策略订阅而复制 N 份完整深度消息。

arena 使用固定的大小类别和容量上限，热路径不依赖通用堆分配。协议允许的最大消息必须有对应容量或明确的分段协议；任何分段逻辑都必须以完整业务事件为可见单位，不能把部分深度更新发布给策略。完整簿快照使用独立、带版本和水位的不可变快照对象，不用超大记录占住普通行情 ring。

策略需要长时间保留数据时，使用有上限的借用配额或复制到自己的内存。内部并行计算若乱序完成，释放水位必须维护缺口，不能把“最大的已完成序号”当成连续释放水位。

### 2.3 依赖屏障

同一 topic 上独立策略互不等待；有依赖的处理阶段配置为无环图。例如特征进程消费行情，在独立输出 ring 中发布特征及 `source_seq`，依赖它的策略等待对应的特征水位。禁止不同消费者在同一个已发布 payload 中各自写字段。

依赖屏障约束的是明确的业务前提，例如“特征已包含行情至 S”，并不表示多个交易所事件时间相同就已一致。依赖图、版本与超时处置进入部署清单；某个特征消费者失败只使依赖它的策略失效，不让无关策略等待它。

## 3. 慢消费者隔离与 ring 代次切换

有限容量、无条件无损和任意长时间暂停的消费者不能同时保证生产者持续前进。生产方案通过独立 QoS、明确摘除协议和有界 bank 轮换处理这个约束，不能把读者的暂停当成退出。

### 3.1 常规摘除

1. 监测队列占用、最老未释放记录年龄和消费速率，在容量耗尽前识别落后者。
2. exec 撤销相关策略的增险授权；授权撤销以 exec 的应用序号为生效点。
3. 控制面请求消费者 quiesce。消费者停止新借用，结束内部所有访问，报告最后释放水位与该代次的 detach ACK。
4. 只有确认 quiesce，或操作系统确认该消费者及所有获授权引用者已经退出后，才能把它从相关 bank 的 gating 集合摘除。

单纯的 heartbeat 超时、SIGSTOP、调度饥饿或撤销交易许可都不是内存释放证明。无法回应的策略可被终止；收集真实退出证据后再回收其保护的槽位。

### 3.2 冻结旧 bank，让健康读者继续

所有数据面预分配若干 ring bank。出现慢消费者时，可在当前 bank A 容量耗尽前执行以下协议：

| 阶段 | 生产者与消费者行为 |
|---|---|
| Prepare | 分配尚未使用的 bank B，校验 ABI；形成 membership 版本与健康消费者清单；完成将迁移读者的资源映射/Ready 确认和待激活 membership，不能让 B 在它注册前覆盖起始事件 |
| Freeze | 唯一生产者确认 A 无未结束的可写 guard/半写发布后，在完整事件边界把 A 封口为 `last_seq=S`，发布 `{old_epoch, new_epoch, cutover_seq, membership_version}`；此后不再写 A 的事件/payload；S 必须是连续完整发布的最大序号 |
| Publish | 先把 B 的每个获准读者 gating 初始化到 S，再从 `S+1` 继续同一逻辑 topic；正常数据无缺口时仅改变 `ring_epoch`，不改变 `feed_generation` |
| Switch | 健康消费者完成 A 至 S 的业务处理并释放引用，校验 B 的前驱关系后切换；尚未完成切换的消费者受 B 的预注册 gating 保护 |
| Retire | 旧读者仍可读取被冻结的 A；只有全部引用结束或退出已确认，A 才能回收。冻结不是把旧读者游标强行改成 S |

B 的初始 gating=S 只是新 bank 的容量基点，不能当成旧 bank 已经释放至 S；旧 bank 的最后访问/Quiesced ACK 独立跟踪。每个 bank 使用自己的 payload arena，冻结同时停止改写该 arena；新 bank 不复用仍被旧读者持有的 arena。

新策略加入使用已登记 membership 的 JoinSeq 与一致快照基线，只读该基线之后受 gating 保护的连续事件。不能拿一个历史 cursor 直接追读已经可能被复用的槽位；MapReady/快照/历史覆盖不齐时实例保持 Shadow/Syncing。

切换描述符是可查询、幂等的控制状态，不能只发一次通知后丢掉。策略启动、退出和切换并发时，membership 操作由发布者串行应用，绑定其 generation；晚到的 ACK 不能改变新代次成员关系。发布者在 Freeze 与 Publish 之间崩溃时，订阅者停止交易并按新的 feed generation 重同步，不能把“找到了另一个 segment”当成连续性证明。

若消费者已错过可保留的历史，则使用 `snapshot(S) + events(S+1...)` 重建，并由 exec 重新认可数据 readiness。快照必须是簿所有者发布的同一版本不可变状态；不能一边更新 mutable book，一边让另一个进程无锁 memcpy 得到混合时刻的快照。

### 3.3 有界资源与服务质量

bank 数、每个 bank 的字节数、最大消费者数、最大未释放记录数和最大 snapshot 数均预先分配并计入资源清单。容量规划同时按记录数和 payload 字节计算，例如 `C >= burst_records + ceil(peak_rate × bounded_interruption)`，其中 interruption 包含检测、隔离/切换与调度余量；这个公式只在输入峰值和处置期限有界且已验收时成立。bank 预算覆盖 Active、Preparing 和最大同时 Retiring 数；同时计算 `N_consumers × cursor/control_resources`、各代次 arena、快照及新旧策略共存开销。无法给出有限回收期限时必须保留耗尽动作，不能用容量公式许诺永不阻塞。bank 数必须依据“最长故障检测/隔离时间内的轮换需求”容量规划；不能把某个固定数字写成普遍充分条件。没有可安全复用的 bank 时，停止相关发布并把该行情源标为失效；保留 exec 的撤单与私有回报处理能力。

严格行情消费者、允许合并最新状态的消费者、离线录制使用不同 QoS topic 或受控中继。对允许合并的订阅仍需安全的内存所有权，不能为了丢旧数据直接覆盖正在读的槽位。离线 recorder 的磁盘停顿不能成为交易 ring 的无限期 gating 条件；它通过独立采集进程/relay 消费并转存，出现缺口会使该段录制失去完整回放资格。

## 4. 策略意图、回报与公平汇聚

每个策略实例到每个执行风险域拥有独立 SPSC 通道。执行循环按声明的轮询配额/优先级消费，给风险撤销、撤单、私有成交和控制事件保留处理机会。队列先到次序不被当成市场因果总序；最终应用顺序记录在 exec journal。

意图至少包含：

```text
logical_strategy_id / instance_id / strategy_generation
intent_id / causal_topic_id / feed_generation / market_seq
exec_generation / authorization_epoch / config_version
instrument_id / command_kind / price / qty / order references
created_mono_ns / valid_until_mono_ns / risk_scope
```

channel 身份在控制面建立，不能由 payload 中自行填写的 strategy ID 覆盖。exec 校验身份、授权代次、期限、独立行情新鲜度、账户状态与剩余额度，然后预留并提交自己的订单状态机。策略不能直接持有 gateway，也不能直接修改预留或订单状态。

共享内存入队只是传输状态。`Received`、`RiskRejected`、`Accepted`、`MayHaveSent` 与交易所确认分别表达不同阶段。`Accepted` 只在意图、预留、授权及其 RecoveryManifest 满足生产提交水位后发出；此前是 Received/Prepared。提交后 ACK 丢失时，策略查询原 `intent_id`；exec 在持久化去重域内返回原结果。不能因为 ring 中看不到消息就认为没有接受，也不能重新生成一个经济等价的新 ID 绕过去重。

回报中的 `report_seq`、`exec_journal_seq` 和订单/成交身份允许检查缺口。回报通道满时，exec 继续维护账户事实，将该策略转为不可增险；策略重连通过一致快照与回放补齐。不能要求 exec 为等待死策略读回报而停止处理交易所成交。

## 5. Linux 资源、权限与固定 ABI

应用二进制接口（Application Binary Interface，ABI）定义各进程如何解释同一片内存。采用固定的 Tickwire wire ABI，独立于 Rust 内部结构体和业务 crate 版本。

### 5.1 建立连接

supervisor 的资源管理器在控制面创建资源，用 Unix socket 的 `SCM_RIGHTS` 交付文件描述符，并核验 peer credentials、策略实例、订阅权限和进程身份。数据通过 `memfd_create` 与 `mmap(MAP_SHARED)` 共享；`eventfd` 只作通知，正常活跃的热数据通道采用专用核轮询。memfd 是易失内存，不是 WAL。[Linux memfd 文档](https://man7.org/linux/man-pages/man2/memfd_create.2.html)、[Unix socket 与描述符传递](https://man7.org/linux/man-pages/man7/unix.7.html)

payload/发布元数据与消费者游标放在不同资源中；**每个消费者的游标/ACK 使用独立的 memfd 能力对象**，不能只把所有游标放在同一可写 memfd 的不同页。只映射某一页不是权限限制，持有可写描述符的对端可能重新映射其他页。

每个单写资源强制采用握手顺序：创建与定长分配 → 唯一获准写者完成 RW 映射并返回带资源身份的 MapWriter ACK → 创建方设置并核验 `F_SEAL_GROW/F_SEAL_SHRINK/F_SEAL_FUTURE_WRITE` 及锁定封印集合 → 才向读者交付描述符 → 读者检查 seals/ABI 后 ReadReady → owner 激活。payload 的写者是 md，某个 cursor 的写者是对应消费者；读者按各自角色反向建立。创建方完成初始化后撤销自己不再需要的写映射，登记所有保留写能力的参与者。

`SCM_RIGHTS` 传递原有描述符，不自动减少其访问权；`F_SEAL_FUTURE_WRITE` 保留此前建立的可写映射，因此不能先把未封印 FD 交给读者。任何一步失败都不得发送 Ready，也不能把半初始化对象公开。封印防止 shrink/grow 与新建不应有的可写映射，不撤销合法写者已有映射。[Linux 文件封印](https://man7.org/linux/man-pages/man2/F_ADD_SEALS.2const.html)

执行进程、行情进程和不同信任等级的策略使用独立 OS 身份及资源限制；策略无交易密钥、无任意读写其他进程资源的权限。自有策略也按可故障的对端处理：长度、偏移、枚举、游标推进范围和身份都要验证，不能因“来自本机共享内存”而信任。

### 5.2 布局和版本

头部包含 magic、ABI major/minor、header/record size、capacity、字节序、对齐、topic/schema、producer identity、epoch、membership 版本和资源尺寸。跨进程记录使用固定宽度整数、明确标签与相对偏移；不传 `usize`、裸指针、`Vec/String`、Rust enum 内存布局、trait object 或进程内锁。

`repr(C)` 不是完整协议：仍需显式规定 padding、字段有效值、长度界限与兼容策略。所有记录完整初始化后发布；解析器不能把不可信字节直接转成可能有非法判别值的 Rust 对象。命令在执行侧复制到有界本地结构并校验后才进入业务 reducer，不在异步操作中持有策略可写区域的引用。复制只固定后续解释的数据，不单独证明复制期间没有并发改写。

生产准入区分两种信任画像：`qualified_sdk` 策略通过受审计的 SDK 写入，发布操作消耗写 guard，发布后到释放前不能再写该记录，原子视图和引用不能逃出 SDK；故障模型允许在任意合法指令处崩溃、暂停和失联，不把任意违反内存协议的写入当成可由序号补救的行为。包含不受信任 native 代码或无法证明遵守该协议的策略使用 `isolated_adapter`：不给它数据面可写 FD，经内核消息边界向独立的可信适配进程提交有界记录，再由适配进程使用 SHM SDK 发布到 exec。此画像额外的复制和系统调用计入延迟，不能宣称与直接 SHM 相同。行情 payload 始终由获准 md 写、策略只读；exec 仍对全部输入执行账户风控。

本地部署画像必须规定 CPU 架构、原子宽度/对齐和缓存行隔离。只启用已验证为原生、跨进程可用的原子操作，不接受内部依赖进程内锁的退化实现；不同架构各有独立资格测试。生产者和消费者的高频游标放在不同缓存行，初始化由唯一创建者在 Ready 之前完成。[Rust 原子操作与平台约束](https://doc.rust-lang.org/std/sync/atomic/index.html)

minor 升级仅允许协议明确定义的兼容扩展；major 升级使用新 namespace/新资源，旧、新进程通过并行通道或经验证的版本桥接迁移，不能修改仍有读者的原地布局。日常策略二进制更新不需要升级 IPC ABI。

### 5.3 进程死亡与回收

运行中使用 pidfd 等操作系统身份句柄跟踪进程生命周期，避免仅凭可能复用的 PID 清理资源。pidfd 可用于观察实际退出；它不表示 SIGSTOP 的进程已经死亡。[Linux pidfd](https://man7.org/linux/man-pages/man2/pidfd_open.2.html)

共享映射设置适用的 `MADV_DONTFORK`，描述符采用 close-on-exec，并通过运行时约束禁止参与者把映射/描述符泄漏给未登记子进程。需要子进程时必须重新注册为独立消费者。确认原进程退出不能证明未知后代没有继承映射，因此这项约束属于资源回收证明的一部分。[Linux madvise](https://man7.org/linux/man-pages/man2/madvise.2.html)

生产者退出后的旧 segment 不交给新生产者续写；新实例创建新代次。资源管理器只清理自己创建且身份匹配的资源，不按模糊文件名前缀删除。任何超时只能触发隔离和回收流程，不能直接授权复用仍可能被访问的内存。

## 6. 通知、NUMA 与发布期间的尾延迟

交易活跃阶段使用独占核轮询、预触页、锁页检查和 NUMA 本地分配。跨 NUMA 消费量大时比较本地复制副本与直接读远端共享页，按完整策略延迟而非复制次数选择；跨主机仍使用网络协议，共享内存不承担远程通信。

低频通道可切换到 eventfd 通知模式。进入休眠前需要完成发布者已启用通知的确认，再检查队列并等待；该模式中每批发布都有持久计数通知，退出休眠后才能关闭通知。禁止用一个未经模型验证的 armed 标志加两次普通读来声称无丢失唤醒。活跃轮询和通知模式都需要 lost-wakeup 回归用例。[Linux eventfd](https://man7.org/linux/man-pages/man2/eventfd.2.html)

新策略注册、队列和 bank 创建、内存映射、schema 协商、快照准备与大规模初始化在控制线程执行。发布者仅在明确事件边界应用已准备的 membership 变更；热路径不临时发现服务、扫描目录或分配消费者管理结构。策略加入/退出和 bank 切换仍会影响 cache/带宽，必须单独测量并报告 p99.9/max。

每个进程有 CPU、内存、SHM、文件描述符和借用数量上限。共享内存避免了逐笔 socket 复制，但仍有缓存一致性、内存带宽、排队和轮询成本；以完整因果链的时间戳验证优化。

## 7. 实现选型与生产准入

主实现方向是 **固定 Tickwire ABI 的 Linux 共享内存 transport**，用小而封装的 Rust `unsafe` 边界实现映射和原子视图，业务层保持安全 Rust。Disruptor 提供广播、游标和依赖屏障的设计依据，线程内 Rust Disruptor crate 不能未经改造当成跨进程库。

[IPC 组件调研](../research/shared-memory-ipc.md)比较了共享广播 ring、独立 SPSC 和 iceoryx2。iceoryx2 的零拷贝发布订阅、故障清理和资源管理值得作为对照，但其版本兼容和访问权限的实际限制必须先满足本系统的独立发布契约；不因为 benchmark 宣传或 API 名称相似就直接替代本协议。

需要交付可复现基准和以下资格证据，才能把 transport 标记为可上线：

- 单元/性质测试覆盖序号、容量、发布与释放、ABI、偏移、溢出、环绕和切换边界。
- loom 检查使用其同步原语建成的最小并发核心；Miri 检查可执行的 unsafe/布局部分；不能声称它们自动覆盖真实 mmap/进程死亡。
- 真实多进程故障测试覆盖 SIGKILL、SIGSTOP、半发布、持有借用时退出、句柄泄漏、双向背压、bank 耗尽与切换中崩溃。
- vibe-tla 从实际 Rust 源码建立 `broadcast_ring`、`bank_rotation`、`membership`、`intent_delivery`、`strategy_handoff` 模型，动作锚定源码并记录 TLC 覆盖与反例。
- 延迟基准覆盖 1/2/4/8/16/32 个策略、不同 payload、独立核/同 NUMA/跨 NUMA、落后者、策略加入退出、bank 切换、执行回报洪峰和持久化等待。

当前文件定义的是上线契约，没有生产实现、TLC 通过结果或纳秒级 IPC 实测结论。
