# 多进程共享内存 IPC：生产选型与保证边界

行情主方案采用 **Disruptor 式共享 payload 广播 ring＋每消费者独立 sequence、gating 与 barrier**，按 topic 和服务质量等级（Quality of Service，QoS）分区，分别规定完整性、时延与满载行为。每个订阅该 topic 的策略读取同一条流；正常运行时，生产者受活跃读者进度限制，避免覆盖仍被读取的槽位。慢策略隔离通过预分配 ring bank 的冻结、切换与安全退役实现。策略意图和回报按 `(strategy_instance, account_domain)` 建立独立有界入口，由执行进程中的单写核心公平汇聚；它最终负责订单、逐单风控、资金预留和交易所发送。

**现有 Rust `disruptor` crate 不能直接承担跨进程 ring。iceoryx2 也不是同一种结构。** 前者使用线程内堆对象与 `Arc`；后者采用不可变 payload 池和每对发布订阅端的 offset 通道，保留为内存生命周期参考及性能对照。生产主方案需要明确的 Linux SHM transport 实现与独立的 ABI、成员关系、切换和回收协议，不能把任一库的宣传延迟当作 Tickwire 实测或上线资格。

本文依据 2026-10-20 查询的官方文档、固定提交源码和发布包。只交付研究与设计，没有构建 IPC 原型、执行故障注入或性能测试。它支持“独立行情进程、独立策略实例、独立账户执行进程、supervisor 控制面”的生产方向。

## 固定研究版本

| 候选 | 固定版本／提交 | 状态与许可 | 本文审查范围 |
| --- | --- | --- | --- |
| LMAX Disruptor | `4.0.0`；[`95c705f60c1833b07f1fed6e08a08d7bee7f0971`][lmax-commit] | Apache-2.0 | Java sequencer 与消费进度；算法参考，原库用于线程间通信 |
| Rust disruptor | `4.4.0`；[`08b469143ac8e2db9bdc7c1aba0b143fefd25807`][rust-disruptor-ring] | crates.io 发布于 2026-08-08；MIT | 堆布局、gating、consumer guard；不具备可直接采用的 SHM IPC 布局 |
| iceoryx2 | `0.10.0`；[`135d09dd8b29f321f1725920d434864c4e512378`][iox-commit] | crates.io 发布于 2026-09-18；GitHub release 标记 **Pre-release**；MIT OR Apache-2.0 | Rust core、平台相关源码 `iceoryx2-cal`、进程监测、共享内存连接、样本生命周期及静态配置 |
| rtrb | `0.4.0`；[`d1f3a2eb042602bcf2a2be2fef195a0a7dfb98af`][rtrb-source] | MIT OR Apache-2.0 | 判断其对象和分配方式是否适合跨进程 |
| shared_memory | `0.12.4`；[`8dfa117c7f0bed6a5bf3dfe4a5382bb282b1ecf2`][shmem-source] | crates.io 发布于 2022-03-01；MIT OR Apache-2.0 | Linux 映射、生命周期及裸内存接口 |
| Aeron | `1.53.3`；[`377943bcb8256fa5279fc4e5c6d9f2a076294222`][aeron-commit] | Apache-2.0 | IPC publication、订阅者流控和官方发送接口；作为另一生产对照 |

iceoryx2 各 `0.10.0` 发布包中的 `.cargo_vcs_info.json` 指向同一上述提交。不能把 crates.io 无预发布后缀的版本号解读为 GitHub 已宣布稳定生产版本。[iceoryx2 发布页][iox-release]、[包元数据][iox-crate]、[shared_memory 包元数据][shmem-crate]、[Aeron 发布页][aeron-release]

## 三种广播结构的取舍

共享内存（Shared Memory，SHM）让多个进程映射同一段物理内存。进程间通信（Inter-Process Communication，IPC）还需要规定消息发布、读取、确认、回收和访问权限，单有 `mmap` 不构成消息协议。

| 结构 | 发布与存储成本 | 慢消费者和崩溃 | 更新、权限与适用范围 |
| --- | --- | --- | --- |
| N 条独立 SPSC，逐订阅者复制 payload | 每个订阅者保存一份数据；复制和写缓存流量随扇出增加。单生产者单消费者（Single Producer Single Consumer，SPSC）环只由一端各一个参与者操作 | 每条队列独立满载，能停止向某个策略投递而继续服务其他策略；崩溃遗留的槽位只影响该通道。不可覆盖消费者尚在读取的槽位 | 易形成每策略独立的文件描述符权限边界；对小型意图、短事件及严格隔离链路可能有更好的实际尾延迟，不能只因存在复制就排除 |
| 不可变 payload 池＋每订阅者独立 offset/loan 通道 | payload 生产一次，向 N 个订阅者分发偏移量／句柄；仍有随 N 增加的队列、引用和缓存通信成本 | 限定每条连接的排队数与借用数；队列满可以只淘汰该订阅者的未读引用，已借数据保留到返还或确认进程死亡。容量按所有获准读者的最坏占用预分配 | 适合行情扇出。iceoryx2 属于这一结构；权限、版本和资源清理需逐项验收 |
| 单共享日志／多游标广播 ring | payload 写入一次，每个订阅者维护游标；减少 payload 重复存储，但回收边界、游标扫描与缓存争用取决于实现 | 无损模式由最慢读者限制回收。若移除落后读者，必须同时解决它可能仍在读旧槽位的问题；心跳过期或逻辑退订不能证明内存已经无人访问 | 适合消费速率与留存窗口可明确约束的高扇出场景；可采用成熟实现或自研。允许丢消息的广播也必须有合法的读写与回收协议 |

这是结构比较，不是性能排名。对单生产者向 N 个策略广播，第一类需要 N 条通道；若多个行情进程各自发布，则按发布者—订阅者对建通道或由明确的单一发布者归并。多个消费者竞争同一个工作队列会分摊消息，不会让每个策略收到全部消息。

**生产选择**：第三类为行情扇出的主架构，第一类用于独立的小型意图／回报通道；第二类及 Aeron 是同等条件下的正式对照。主方案的实现需要同时满足慢消费者隔离、权限、滚动升级、崩溃回收与性能门槛。

## 共享广播 ring 的生产契约

### Disruptor 中哪些内容可以借鉴

LMAX 文档区分了两种 gating：生产者不能绕过仍在处理槽位的消费者；消费者也可以等待其他依赖消费者完成。独立交易策略之间通常没有计算依赖，各自 barrier 只等待该 topic 的发布进度；不能把全部策略串成依赖链。依赖关系必须对应真实业务先后要求。[LMAX 用户指南][lmax-guide]

固定 LMAX 源码中的单生产者 sequencer 根据最小 gating sequence 判断是否有空间。Rust `disruptor 4.4.0` 也在发布前检查消费者进度，`EventGuard` 在结束读取作用域时推进 cursor。这些可作为内存顺序和消费生命周期参考。[LMAX sequencer][lmax-sequencer]、[Rust producer][rust-disruptor-producer]、[Rust guard][rust-disruptor-guard]

Rust crate 的 ring 使用 `Box<[UnsafeCell<E>]>`，builder 用 `Arc`、`Vec` 管理 ring、cursor 与消费者；支持库管理线程或由调用方使用 EventPoller。无论哪种入口，底层仍是进程内地址和所有权。它没有提供本设计需要的跨进程映射、文件描述符授权、进程死亡回收与滚动 ABI 协议，不能直接把 `Arc<RingBuffer<_>>` 放进共享内存。[ring 布局][rust-disruptor-ring]、[builder 所有权][rust-disruptor-builder]

### 正常发布与读取

每个 `(topic, QoS, ring_epoch)` 有一个发布者。容量为 `C`，待写序号为 `n`，每个活跃消费者的 `released_seq` 表示它对该序号及之前槽位的最后一次访问都已结束。生产者只有在 `n - C <= min(released_seq)` 时才能复用对应槽位；批量写入按本批最高序号检查。消费者的业务 `applied_seq` 另行表示状态更新完成，不能用“已经取到指针”代替任一完成进度。

生产者写完完整槽位后以 Release 发布连续 committed sequence；消费者通过具有 acquire 语义的 `acquire_observe` 观察发布进度，再访问不可变 payload；消费者结束全部借用后以 Release 推进 `released_seq`，生产者通过同一抽象观察。动态成员增减与缓存的最小 gating 值也属于同一协议：由 publisher 在事件边界登记新读者的 JoinSeq／gating，配合一致快照建立订阅起点；新读者不能追读该起点之前已可能被复用的内容。

这里的 acquire 语义不等于在只读映射上直接调用 `AtomicU64::load(Ordering::Acquire)`。本轮读取的 Rust 1.99.1 官方文档只对列出的目标及足够小的 Relaxed 原子加载给出只读内存保证，并建议以 Relaxed load 后接 Acquire fence 获取 acquire 效果。Tickwire 在获准的 64 位目标上使用对齐 `AtomicU64` 的 Relaxed load＋`std::sync::atomic::fence(Ordering::Acquire)`，仅在观察到相应 Release 发布值之后读取其保护的数据；只读页禁止直接 Acquire load 和任何读改写操作。拥有写权限的一端用 Release store。发布序号和消费者释放水位的两个方向都执行这一约束，目标／宽度不满足时拒绝该部署画像。[Rust 只读内存上的原子操作][rust-ro-atomics]

payload、发布元数据与每个消费者的 cursor 控制页分别授权。策略只读 payload，仅能写自己的 cursor／状态页；不能把所有策略进度放在每个策略都可写的一张共享页中。SDK 不允许引用逃出读取 guard；异步计算需要先复制自己所需的小型快照。布局、原子宽度和内存顺序在实际 Linux／CPU 目标上验证。

### 冻结旧 bank、切换健康消费者

“冻结旧 ring，健康消费者读完 `S` 后迁到新 ring 的 `S+1`”可以隔离滞后读者，前提是把它实现为有状态的协议：

1. **预备**：高水位触发前就保留并预热备用 bank；其状态为 `Preparing`。supervisor／publisher 协调者确认新 bank 的布局、授权、目标读者集合和 generation，不能到旧 ring 已满时才临时申请无界内存。
2. **冻结**：发布者在完整事件边界选择 `S = last_contiguous_committed_seq`，发布 `Frozen(S)`，此后不再写这个旧 bank。预留序号或半写样本不能作为 `S`。如果未来采用多生产者，必须先解决发布空洞；不能直接用最大 claimed sequence 切换。
3. **激活**：新 bank 的逻辑起点为 `S+1`，容量计数基点为 `S`。所有拟迁移读者在新 bank 进入 Active 前已登记，其初始 gating 为 `S`。这个初始值不表示旧 bank 已读完，旧 bank 的释放／Quiesced ACK 单独跟踪。新 bank 可以在健康读者排空旧数据期间接收后续事件，但受到这些初始 gating 和容量限制，不能把尚未接入的读者当成已经消费。
4. **迁移**：读者在旧 bank 完成到 `S` 的业务处理并结束所有引用，再从新 bank 的 `S+1` 开始。`Quiesced(old_epoch, S)` 的含义包括无存活借用、不再访问旧 bank，并完成 SDK 的脱离／解除映射流程；单纯报告“看到了 S”无效。
5. **隔离**：没有迁移的旧读者可以继续拥有被冻结的旧映射，但不属于新 bank 的活跃集合，其 strategy generation 不再获准提交新意图。执行核校验意图所依赖的 topic／epoch／sequence；旧代次未决意图仍按 ID 对账，不作为新意图重发。
6. **退役**：旧 bank 只有在其所有潜在读者完成匹配 instance／epoch 的 Quiesced ACK，或确认相应进程死亡后才可回收、重新初始化。心跳、消费租约超时或把读者从逻辑名单删除都不能替代这个条件。

新 bank 的读者也可能在迁移期间变慢，所以该协议依然需要正常 gating 和下一次有界隔离。环库的基本 sequencer 不会自动提供 bank 切换、读者许可及崩溃恢复；上述部分是 Tickwire 自身的生产协议。

### 有界 bank 与崩溃边界

预分配数量包含 Active、Preparing、Frozen/Retiring，以及滚动部署期间旧新策略的并存占用。限制每个 reader instance 可同时借用的 bank 和每个策略可并存的 generation，禁止通过反复重启无界积累旧映射。达到 bank 上限时，对该 topic/QoS 停止继续发布并使依赖它的策略禁增险；不能为了继续运行覆写仍有活读者的旧 bank。这是明确的失效范围，不能承诺有限内存下无限次隔离永久停顿的读者。

需要分别测试 publisher 在 Frozen 前、Frozen 后但新 bank 尚未 Active、Active 后但通知未送达时崩溃；消费者在 drain、ACK、attach 各步崩溃；supervisor 在任一迁移阶段重启。迁移标识、旧／新 epoch、`S`、成员集合和确认阶段必须可重建，重复控制消息幂等。重启不能凭同名共享段或较大的 sequence 自动认定切换完成；无法解释连续性时进入行情重新同步。

高水位迁移并不消除上游输入的突发。发布者冻结／切换期间仍要为输入和待发布事件保留有界容量，记录切换停顿；容量不足就产生显式 gap 并使相应行情失效。新 bank 必须保持单一序号分配权，避免重复或遗漏 `S+1`。

## iceoryx2：不同结构的候选与源码证据

### 数据路径与有界容量

官方文档说明：每个 publisher 有自己的 payload segment，每个 publisher—subscriber 对有独立 offset channel。Rust API 的 `loan`／`loan_uninit` 从发布者数据段借出样本，发送后订阅者直接读取该样本；`send_copy` 则先把调用方的值复制进样本。这里的零拷贝指 IPC payload 传递，不能扩展成“网络解码到策略全过程没有复制”。[发布订阅结构][iox-pubsub-book]、[Publisher API][iox-publisher-api]

在 `0.10.0` 中，每个发布者数据段的默认预分配样本数是：

```text
S = max_subscribers × (subscriber_max_buffer_size + subscriber_max_borrowed_samples)
    + history_size + publisher_max_loaned_samples
```

这是一份按服务上限计算的容量预留，包含慢读者排队、借用、历史缓存和发布端未发出的 loan；不是只按当前订阅者数量分配。实际总内存还包括每样本的头部、对齐、offset 通道、管理结构、在途旧连接和迁移期间并存的服务。配置上限必须由部署准入检查，不能在容量耗尽后无限增加策略。[样本数计算][iox-capacity]、[发布者创建][iox-publisher-create]

推荐固定消息大小或明确的最大 slice 长度，使用 `AllocationStrategy::Static`，并保留默认最坏情况预分配，不缩减 `override_sample_preallocation`。静态策略不扩容，超限会返回分配错误。动态数据段在发布者退出而订阅者尚未映射新段时可能丢样本，源码对此有明确分支；静态配置可避开这一类动态映射窗口。[分配策略][iox-allocation]、[动态段丢失分支][iox-dynamic-loss]

### safe overflow、借用与慢订阅者

`enable_safe_overflow(true)` 在订阅者队列满时替换最旧的**未读队列项**。底层返回被挤出的 offset，发布者释放该项持有的引用；已经被 `receive()` 取出的样本由独立借用计数保护，不能当作未读队列项覆盖。`Sample::drop` 把 offset 放回返还通道，发布者回收这些引用，最后一个引用消失后才把块交还分配器。[发送／接收连接][iox-connection-send]、[借用与返还][iox-connection-receive]、[Sample Drop][iox-sample-drop]、[块回收][iox-chunk-reclaim]

需要分清三种满载策略：

| 配置 | 满载行为 | Tickwire 用途 |
| --- | --- | --- |
| safe overflow 开启 | 该订阅者丢失最旧的未读样本，继续接收较新样本 | 适合允许检出缺口后重新同步的行情流；“safe”描述内存处理，不代表行情无损 |
| safe overflow 关闭，`DiscardData` | 跳过当前满载接收者；其他接收者仍可收到 | 适合独立的非阻塞入口，但调用方必须处理未入队与结果不明；不能把丢弃当作业务拒绝 ACK |
| safe overflow 关闭，`RetryUntilDelivered` | 发送调用重试到接收方有空间或发生其他结束条件 | 一个慢订阅者可拖住该次发布，进而影响后续订阅者和行情处理；不用于必须隔离慢策略的行情扇出 |

依据是实际 `try_send`／`blocking_send` 分支，不能仅凭“lock-free”字样判断一次 API 调用是否会等待。[背压策略][iox-backpressure]、[发送分支][iox-delivery]

在固定服务、符合容量约束且采用非等待策略时，慢读者只占其有界排队和借用预算，不要求 publisher 等它消费。活着但停顿的进程仍持有这些借用；不能因为超时就强制释放。应用层应在 `Sample` 有效期间完成读取，长期历史写入策略自己的状态，再尽快 Drop。SDK 应约束 loan 的作用域，并要求关闭 subscriber／切换数据代次前先归还全部样本。

订阅者还保存发布者退出后的旧连接，以保留未读或已借数据。旧连接缓存耗尽时，源码可能丢弃尚未借出的数据；若已借连接无安全腾挪空间，则有 `fatal_panic` 路径。因此“发布者反复重启＋策略长期持有旧样本”必须测试，不能宣称借用和连接代次无限留存。[旧连接处理][iox-expired-connections]

### 发送成功、顺序与公平性

`send_copy`／样本发送返回成功接收该次投递的 subscriber 数量，它表示传输投递，不表示策略已经处理、执行核心已经接受意图或 WAL 已持久化。广播可能只投递到部分订阅者；业务确认必须有自己的 request／intent ID 和状态机。[Publisher API][iox-publisher-api]、[投递循环][iox-delivery-loop]

多个 publisher 接入同一个 subscriber 时，`Receiver::receive()` 遍历连接，找到首个可用样本便返回；这段实现没有轮询公平保证，也没有全局跨发布者顺序。因此策略意图使用独立的 strategy→account-domain 服务／入口，由执行 owner 明确实施有界轮询、优先级与速率约束，再分配单调 ingress sequence。[接收遍历][iox-receive-fairness]

### 新增策略不是无成本的后台操作

`send_sample()` 会调用 `update_connections()`；订阅者列表变化后会进入连接更新，必要时建立共享内存连接和映射。库不提供一个独立后台线程替应用隐藏这些工作。因此，新增策略、重启和滚动切换都必须纳入发布线程最大停顿测试。显式预热连接、Ready 确认、按 topic／epoch 建立影子服务，以及必要的连接管理改造，应由 transport adapter 负责。[发送与连接更新][iox-publisher-update]、[官方连接使用说明][iox-faq]

## 死亡识别与内存回收

推荐 IPC monitoring 实现是 `FileLockMonitoring`，底层 `ProcessMonitor` 检查进程状态文件和文件锁；同进程路径还使用内部状态跟踪。此机制不是心跳超时推断。`SIGSTOP`、CPU 饥饿或策略死循环不会使其因失去心跳而自动成为 dead node。[monitoring 选择][iox-monitoring]、[进程状态检查][iox-process-state]

`try_cleanup_dead_nodes` 会清理已判定死亡的节点及其服务记录；权限不足、另一清理者正在工作或版本不兼容都可能阻止完成。默认还有在创建／销毁节点、打开服务时触发的清理配置。生产运行需要 supervisor 中独立的资源清理职责、结果检查和容量告警，不能假设每次策略退出都立即完成全局回收。[清理 API][iox-cleanup-api]、[清理实现][iox-cleanup-impl]

publisher 在移除已消失接收者的连接时，使用其已投递 offset 清单取回引用。这个步骤的安全前提是接收者确实不再使用连接；Tickwire 不能用自己的租约超时绕过库的死亡或关闭协议。[接收者移除][iox-remove-connection]

生产恢复按以下顺序处理：

1. **业务撤权**：执行进程拒绝失效 strategy generation 的新意图；保留它已接受订单和预留。撤权无需等内存回收。
2. **停止投递或切换 topic epoch**：受影响消费者退出当前数据契约；旧映射及其已借数据仍保持合法。失联与进程死亡分别记录。
3. **确认退出**：同机进程可由 supervisor 持有的 pidfd／子进程等待结果确认退出，避免只凭可复用 PID 判断；库的 dead-node 清理遵循它自己的锁协议。若进程只暂停，继续保留借用或终止进程后等待退出。[Linux pidfd][linux-pidfd]
4. **清理并确认结果**：清理旧端口、检查未回收样本和资源计数；新进程使用新的 instance／generation。清理失败消耗保留容量，不能把失败当作已释放。
5. **分别恢复业务与资源**：策略重启不拥有交易所状态恢复权；执行进程崩溃则进入原有订单／成交／资金对账屏障。SHM 回收成功不构成订单未执行或交易所旧发送者已被隔离的证据。

`force_remove_service` 是 unsafe API，安全要求是没有任何其他进程使用该服务。不得在生产中用它处理“心跳不见了”的活跃服务。[强制移除前提][iox-force-remove]

## 独立策略更新、ABI 与权限

### 稳定接口与按 topic／epoch 迁移

应用二进制接口（Application Binary Interface，ABI）决定共享内存中字段的布局、对齐和解释。iceoryx2 的 `ZeroCopySend` 要求类型自包含、没有指向其他进程资源的指针／引用，并有一致内存表示；`#[repr(C)]` 是必要约束之一。不能直接放入普通 `String`、`Vec`、文件描述符或 Rust 堆对象。[ZeroCopySend 安全要求][iox-zero-copy-send]

库检查类型名、类型类别、大小和对齐，不能检查业务字段单位或识别同尺寸字段换位。服务打开还会精确比较 `PackageVersion`。因此需要独立的 `protocol_version`、schema fingerprint、固定宽度字段、字段偏移／对齐检查及业务枚举验证；不能让不同策略各自复制一份同名 Rust struct 后自由演进。[类型兼容检查][iox-type-check]、[库版本检查][iox-version-check]

策略逻辑更新与 transport 更新应分开：

- **只更新策略逻辑**：稳定 SDK、同一 IPC 版本和兼容 payload 保持不变；新策略进程预热行情、取得执行状态、完成 Ready 后，执行 owner 切换获准的 strategy generation。旧 generation 的在途意图继续按 ID 对账，不重复产生经济动作。
- **变更消息布局**：建立新的 topic/schema epoch，按订阅者逐个迁移；需要兼容转换时由明确的桥接组件承担。两套服务的并存内存和 CPU 纳入容量预算。
- **升级 iceoryx2 版本**：新旧版本使用不同 namespace／服务资源；通过兼容桥接或生产者双发布逐 topic 迁移。保留与旧版本匹配的清理工具，直到旧资源退役；不要让新库直接清理旧库资源或强制忽略版本检查。

新策略拿到 publisher history 不等于获得完整订单簿。启动协议必须定义快照对应的本地流序号、后续增量桥接、缺口与 generation 检查；策略进入 Ready 之前不得增险。广播序号必须对应实际订阅的流分区，不能用另一个过滤范围的全局计数误报丢失。

### 权限是选型条件

固定版本 FAQ 明确：iceoryx2 尚未实现服务访问权限管理；不同 OS 用户访问会失败，`dev_permissions` 全局开放资源且不能用于生产。源码中的普通动态 SHM 默认权限为 owner read/write，payload 在 subscriber 侧按只读模式映射；管理和连接资源仍需要可写。[跨用户限制][iox-faq-permissions]、[资源权限][iox-permissions]、[只读 payload 映射][iox-data-readonly]

同一用户运行受信任策略，可获得进程崩溃隔离，但服务名、namespace 和只读 API 不构成对恶意同用户程序的权限隔离。若生产要求不同策略 UID、严格按 topic 授权或不能信任策略代码，则原版 iceoryx2 0.10.0 尚不满足该准入项。应实现并审计带能力授权的 transport／CAL，或选择支持相应边界的 IPC 方案；不能开启 `dev_permissions` 代替授权。

UDS 控制面负责认证进程身份、批准订阅和绑定 `(process_instance, publisher_id, strategy_id, generation, account_domain)`。执行核不能仅信任 payload 自报的策略 ID。账户凭证只留在执行进程，SHM 载荷只含获准的行情、意图和状态。

## 自有 Linux memfd／mmap 方案

Linux `memfd_create` 创建可映射的匿名文件；通过 Unix Domain Socket（UDS）传递文件描述符后，各进程可映射同一对象。文件引用全部释放后由内核回收；映射在进程死亡时释放，仍有活进程映射的内容继续存在。可以固定大小并封印增长／缩小，避免运行时改变布局；不能给持续写入的队列简单加上禁止所有写入的 seal。[memfd_create][linux-memfd]、[mmap][linux-mmap]

这允许按通道授予文件描述符，而不是让每个策略按公共名字任意加入服务。生产实现还必须自己完成以下协议，不能把“只有两个游标”当作全部工作：

- **布局**：固定宽度头部、magic、ABI／schema、capacity、element size、generation、生产者与消费者身份；用偏移量表达位置。按目标 CPU 校验原子宽度、对齐和缓存行隔离。
- **发布与读取**：生产者完成 payload 后以 Release 发布；消费者通过上述 `acquire_observe` 确认后读取；消费者读完或归还 loan 后才确认可复用。外部句柄和未初始化 padding 不能泄漏进 payload。
- **满载**：普通 SPSC 满载返回失败或停止该通道；不能由生产者随意推进消费者游标。若要覆盖旧数据，需另行实现安全覆盖或不可变块借用协议。
- **回收**：关闭握手、进程死亡确认、borrow 计数、崩溃中的半完成操作和新旧 generation 分开处理。逻辑撤销读者不等于可以覆写它仍映射的内存。
- **唤醒**：忙轮询、eventfd／futex 等方式必须有避免丢失唤醒的协议，并分别测稳态、空闲恢复和 CPU 成本。
- **运行与升级**：预热、资源限额、截断／错误尺寸防护、发现与准入、迁移、观察指标和资源清理都需要可重复测试。

对多游标 ring，读取前后检查 sequence 只能发现某些覆盖，并不能自动使“消费者读取普通字节时生产者并发写同一槽位”变成合法的 Rust 内存访问。必须证明不发生这种重叠，或采用具备相应原子／借用协议的实现。活读者暂停时可暂停向它发布、迁移其他读者到新段，或终止它并确认退出；旧段释放仍等待真实访问结束。

`shared_memory 0.12.4` 主要包装映射及裸指针／slice，Linux 路径使用 `shm_open`、`mmap`、`munmap`、`shm_unlink`；它没有替调用者实现上述队列、同步和崩溃一致性。正常析构时 owner 会 unlink，进程崩溃不会执行 Rust Drop，命名资源的清理仍需监督者。[裸内存接口][shmem-source]、[Linux 生命周期][shmem-unix]

`rtrb 0.4.0` 的 `RingBuffer` 包含普通 `data_ptr`，构造时使用进程堆中的 `Box` 和 `Vec`，两端通过 `ArcRingBuffer` 持有对象。把它放进 `mmap` 或复制对象不能把这些地址和分配器改成跨进程可重定位资源。它仍可用于单个进程内部；跨进程环必须使用真正支持共享内存的实现。[rtrb 实现][rtrb-source]

## Aeron 作为生产对照

Aeron `1.53.3` 的 IPC publication 使用共享 log buffer，并由 Media Driver 参与管理。官方接口中 `offer` 复制调用方数据进 log，`tryClaim` 允许原地写后提交，但有最大载荷和提交时限约束。接收或入 log 成功也不等于业务处理成功。[官方发送接口][aeron-api]

其 IPC 源码包含订阅位置检查和 untethered subscription 状态处理；官方配置允许订阅者退出本地流控。选择这一模式时必须给策略定义缺口和重新同步，不能把从流控移除理解为进程死亡或任意长期借用都安全。[IPC publication 源码][aeron-ipc-source]、[订阅流控配置][aeron-channel]

Aeron 应进入生产基准而不是仅作概念参考；评估项包括 Driver 调度／故障、Rust 到所选官方客户端接口的绑定、claim 生命周期、重连与滚动版本、每个 publication 的 log 内存。它具有不同运行模型，不能仅把 iceoryx2 的 payload loan 假设套到 Aeron 的读取回调上。

## 正式上线资格门槛

以下门槛同时适用于主方案的共享广播 ring、iceoryx2 对照及独立 SHM 通道。性能达标不能替代安全与恢复，源码审查不能替代进程故障测试。

| 门槛 | 必须产生的证据 | 不通过时的结果 |
| --- | --- | --- |
| 版本与来源 | 锁定库、传递依赖、feature、发布包校验和、协议布局和许可证；所有进程检查 namespace／SDK／schema | 拒绝加入对应服务 |
| 只读原子视图 | 固定目标、原子宽度和对齐；实际 RO 映射测试发布序号与释放水位的 Relaxed load＋Acquire fence，核对所依赖的 Rust 文档保证 | 禁止直接用 RO Acquire load／RMW，拒绝不满足平台画像的部署 |
| 消息与业务确认 | 测试 `0` 接收者、部分投递、连接变化、重复／乱序、队列满、ACK 丢失；执行端意图 ID 幂等，明确 Received／Accepted／持久化／交易所状态 | 意图保持未确认或拒绝入场，不能凭传输成功放行 |
| 慢消费者隔离 | 某策略停止取样、取样后永久暂停、长时间计算；健康策略继续满足约定延迟，内存占用与丢弃计数符合上限 | 停该订阅者增险、重建流或更换拓扑，不扩大无限缓存 |
| 内存与崩溃 | 在 loan、发布、接收、返还、注册、清理各阶段注入 `SIGKILL`；区别 `SIGSTOP`；测试 publisher／subscriber 退出次序、持有 Sample 时关闭端口、反复重启和旧连接缓存耗尽 | 保留未确认资源；定位回收缺陷，不能用超时强制复用 |
| 资源清理 | 权限不足、清理者自身崩溃、重复清理、错误版本、PID 重用；资源计数最终收敛，并有无法收敛时的隔离流程 | 不重复授予同一资源或执行权 |
| 广播与 bank 迁移 | 小容量下枚举 wrap、读者暂停在读取中途、成员增减、切换 S 的边界、所有迁移崩溃点、旧 ACK 迟到、重复通知、旧／新 bank 同时可见和 bank 耗尽 | 保留旧 bank；对该 topic/QoS 停止发布并禁增险，不能跳过 gating 或覆盖活引用 |
| 权限 | 符合实际 UID／容器／namespace 部署；禁止 `dev_permissions`；未获准策略不能伪造其他策略意图或加入私有流 | 原版候选不符合权限要求时改 transport／隔离方案 |
| 独立部署 | 单策略逻辑更新、旧新策略并存、schema/topic epoch 迁移、库版本迁移和回滚；已接受订单连续可追踪 | 保留旧服务，拒绝跨不兼容 ABI 直接接入 |
| 公平汇聚 | 一策略洪峰时，其他策略、撤单、回报与风控事件仍获得有界处理机会；每队列深度、预算和拒绝原因可观察 | 不允许所有策略直接竞争一个无公平保证的入口 |
| 性能 | 实际行情尺寸／扇出／订阅集合，正常与突发、热加入、退出、清理、迁移并发；逐链路和端到端 p50/p99/p99.9/max、掉样、CPU、内存、缺页、跨 NUMA 传输 | 按数据改结构或资源划分，不引用库宣传数字填补 |

性能比较至少覆盖相同 payload、相同 fan-out、相同无损／丢弃策略和相同 CPU 预算。N×SPSC 复制、共享池 offset、共享多游标和 Aeron 必须使用同一业务工作负载；不能用丢数据更积极的配置赢得延迟。独立记录 publisher 分发、consumer 排队、策略计算、意图排队与执行核处理，再测完整因果链，分位数不相加。

主方案的形式模型覆盖 `Preparing → Active → Frozen(S) → Retiring → Reusable`、发布／释放序号、成员关系、旧新 bank 的交界、慢读者与真正死亡、bank 容量守恒和 stale ACK；同时覆盖策略撤权与意图幂等。若采用 loan 型候选，再增加 `Loaned → Queued → Borrowed → Returned/Reclaimed`。TLA+／TLC 模型必须对应将来实际适配器源码，不把“库会安全回收”写成未经验证的公理。Loom／Miri 用于适用的原子与 unsafe 边界；真实进程映射、信号、权限和内核回收仍由 Linux 集成测试验证。

[lmax-commit]: https://github.com/LMAX-Exchange/disruptor/commit/95c705f60c1833b07f1fed6e08a08d7bee7f0971
[lmax-guide]: https://lmax-exchange.github.io/disruptor/user-guide/index.html
[lmax-sequencer]: https://github.com/LMAX-Exchange/disruptor/blob/95c705f60c1833b07f1fed6e08a08d7bee7f0971/src/main/java/com/lmax/disruptor/SingleProducerSequencer.java
[rust-disruptor-ring]: https://github.com/nicholassm/disruptor-rs/blob/08b469143ac8e2db9bdc7c1aba0b143fefd25807/src/ringbuffer.rs#L8-L29
[rust-disruptor-builder]: https://github.com/nicholassm/disruptor-rs/blob/08b469143ac8e2db9bdc7c1aba0b143fefd25807/src/builder.rs#L226-L250
[rust-disruptor-producer]: https://github.com/nicholassm/disruptor-rs/blob/08b469143ac8e2db9bdc7c1aba0b143fefd25807/src/producer/single.rs#L95-L133
[rust-disruptor-guard]: https://github.com/nicholassm/disruptor-rs/blob/08b469143ac8e2db9bdc7c1aba0b143fefd25807/src/consumer/event_poller.rs#L90-L134

[iox-commit]: https://github.com/eclipse-iceoryx/iceoryx2/commit/135d09dd8b29f321f1725920d434864c4e512378
[iox-release]: https://github.com/eclipse-iceoryx/iceoryx2/releases/tag/v0.10.0
[iox-crate]: https://crates.io/api/v1/crates/iceoryx2/0.10.0
[iox-pubsub-book]: https://ekxide.github.io/iceoryx2-book/main/fundamentals/messaging-patterns/publish-subscribe.html
[iox-publisher-api]: https://docs.rs/iceoryx2/0.10.0/iceoryx2/port/publisher/struct.Publisher.html
[iox-capacity]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/service/static_config/publish_subscribe.rs#L79-L87
[iox-publisher-create]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/port/publisher.rs#L486-L506
[iox-allocation]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2-bb/elementary/src/allocation_strategy.rs#L15-L28
[iox-dynamic-loss]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/port/details/receiver.rs#L421-L469
[iox-connection-send]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2-cal/src/zero_copy_connection/common.rs#L676-L737
[iox-connection-receive]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2-cal/src/zero_copy_connection/common.rs#L990-L1039
[iox-sample-drop]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/sample.rs#L127-L138
[iox-chunk-reclaim]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/port/details/sender.rs#L522-L552
[iox-backpressure]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/port/backpressure_strategy.rs#L15-L24
[iox-delivery]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/port/details/sender.rs#L226-L349
[iox-expired-connections]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/port/details/receiver.rs#L260-L323
[iox-delivery-loop]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/port/details/sender.rs#L402-L443
[iox-receive-fairness]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/port/details/receiver.rs#L538-L576
[iox-publisher-update]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/port/publisher.rs#L291-L385
[iox-faq]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/FAQ.md
[iox-monitoring]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2-cal/src/monitoring/recommended.rs#L13-L16
[iox-process-state]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2-bb/posix/src/process_state.rs#L983-L1079
[iox-cleanup-api]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/node/mod.rs#L1280-L1300
[iox-cleanup-impl]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/node/mod.rs#L597-L639
[iox-remove-connection]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/port/details/sender.rs#L555-L566
[iox-force-remove]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/node/mod.rs#L1303-L1316
[iox-zero-copy-send]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2-bb/elementary-traits/src/zero_copy_send.rs#L16-L48
[iox-type-check]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/service/static_config/message_type_details.rs#L214-L224
[iox-version-check]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/service/builder/mod.rs#L847-L860
[iox-faq-permissions]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/FAQ.md#accessing-services-from-multiple-users
[iox-permissions]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2-cal/src/dynamic_storage/posix_shared_memory.rs#L78-L90
[iox-data-readonly]: https://github.com/eclipse-iceoryx/iceoryx2/blob/135d09dd8b29f321f1725920d434864c4e512378/iceoryx2/src/port/details/data_segment.rs#L247-L266
[linux-memfd]: https://man7.org/linux/man-pages/man2/memfd_create.2.html
[linux-mmap]: https://man7.org/linux/man-pages/man2/mmap.2.html
[linux-pidfd]: https://man7.org/linux/man-pages/man2/pidfd_open.2.html
[rtrb-source]: https://github.com/mgeier/rtrb/blob/d1f3a2eb042602bcf2a2be2fef195a0a7dfb98af/src/lib.rs#L100-L153
[shmem-source]: https://github.com/elast0ny/shared_memory/blob/8dfa117c7f0bed6a5bf3dfe4a5382bb282b1ecf2/src/lib.rs#L230-L278
[shmem-unix]: https://github.com/elast0ny/shared_memory/blob/8dfa117c7f0bed6a5bf3dfe4a5382bb282b1ecf2/src/unix.rs#L15-L94
[shmem-crate]: https://crates.io/api/v1/crates/shared_memory/0.12.4
[aeron-commit]: https://github.com/aeron-io/aeron/commit/377943bcb8256fa5279fc4e5c6d9f2a076294222
[aeron-release]: https://github.com/aeron-io/aeron/releases/tag/1.53.3
[aeron-api]: https://aeron.io/docs/aeron/publications-subscriptions/
[aeron-ipc-source]: https://github.com/aeron-io/aeron/blob/377943bcb8256fa5279fc4e5c6d9f2a076294222/aeron-driver/src/main/c/aeron_ipc_publication.c
[aeron-channel]: https://github.com/aeron-io/aeron/wiki/Channel-Configuration
[rust-ro-atomics]: https://doc.rust-lang.org/std/sync/atomic/index.html#atomic-accesses-to-read-only-memory
