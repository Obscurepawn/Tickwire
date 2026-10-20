# Tickwire IPC ABI 1.0：最终字节布局与互通协议

版本：设计 v0.3 · wire ABI 1.0 · 2026-10-20。本文是生产实现的协议契约；当前交付包含布局、编码样例和机械检查，尚未实现或运行真实跨进程通信。

应用二进制接口（Application Binary Interface，ABI）规定进程如何解释相同字节；进程间通信（Inter-Process Communication，IPC）在这里包含共享内存记录和控制消息。本文把[共享内存所有权](shared-memory-ipc.md)与[策略生命周期](strategy-lifecycle.md)落实为独立实现可以互通的格式。正文中的“必须”“拒绝”均是协议要求。

机器可读定义是 [ipc-wire-v1.json](../../specs/contracts/ipc-wire-v1.json)，编码基准样例是 [ipc-vectors-v1.json](../../specs/contracts/ipc-vectors-v1.json)。JSON 的 `layouts` 给出每个字段的 `type/offset/size/count`，偏移相对于该布局的起点；嵌套布局加上父字段偏移。每一字节都有归属，包括显式保留区。消息的语义以本文为准，数值和偏移以 JSON 为准；两者不一致时实现不得自行选择，必须修正规格后准入。

## 1. 编码、身份与兼容性

### 1.1 编码规则

- 所有整数使用小端编码；`i8/i64` 使用二进制补码。布尔值只允许 0 或 1；枚举必须先读成整数、校验后转换。禁止直接把不可信字节转换为 Rust enum、引用或含指针对象。
- `bytes[16]` 身份是按原顺序复制的 16 字节不透明值，不做整数端序转换，不采用 UUID 的混合端序。全零表示该字段不适用；实际 resource/channel/instance/request/operation/order 身份非零且不复用。SHA-256 摘要为原始 32 字节，JSON 才以小写 hex 表示。
- 所有保留字节、未用数组项、未用联合体字段以及固定字符串尾部必须为零。未知枚举、位、消息与非零保留字节在 1.0 中拒绝；不得跳过后继续交易。
- 长度、偏移、数量和乘法都先做检查溢出的整数运算，再访问映射。FD 文件长度必须与 `resource_bytes` 完全相等；对象尾部页补齐字节为零。
- 价格、数量不使用浮点。时间为整数纳秒：带 `mono` 的时间来自当前主机 `CLOCK_MONOTONIC`；UTC 时间以 Unix epoch 为零点。单调时间只能在相同 `host_boot_id` 下比较，控制端跨主机发出的本机 deadline 由接收端明确换算/签发，不能直接照抄。
- 正式本地目标只允许 Linux 小端 x86_64 或 aarch64、4 KiB 页、原生 64 位原子。ABI 使用 128 字节游标隔离；部署必须验证实际缓存行不大于 128 字节。16/64 KiB 页、其他字节序或架构拒绝本 namespace；不能仅修改 `page_shift` 继续使用。

`namespace_id` 隔离账户环境和生产/回放空间；记录中的整数 `logical_strategy_id/instrument_id/topic_id/risk_scope` 由该 namespace 的持久注册表分配，0 无效。generation、epoch、membership revision 和序号都是非零 `u64`，严格小于 `2^63`；用零表示未初始化或不适用。达到上限之前换 namespace/feed/channel，旧身份不复用。

### 1.2 兼容矩阵

| 提供方与使用方 | 行为 |
|---|---|
| ABI 1.0 ↔ 1.0、架构配置相同 | HELLO 选择共同 feature 集后建立新资源 |
| ABI 1.0 ↔ 将来的 1.x | 只有双方能显式协商降到 1.0，且发送端确实按 1.0 的长度、枚举和零保留区编码时才连接 |
| ABI major 不同 | 拒绝直接映射；使用新 namespace/新资源和明确的版本桥 |
| 布局相同但 required feature 未获支持 | 拒绝；不能忽略 arena、只读原子或快照等必需语义 |
| 策略版本不同、wire ABI 相同 | 可并存；仍独立验证策略状态 schema、恢复契约与 exec 授权 |
| x86_64 与 aarch64 | wire 字节一致；共享同一主机资源只允许匹配该主机的目标平台配置，资格测试分别完成 |
| qualified_sdk 与 isolated_adapter | wire 一致，权限不同；后者只能给可信 adapter 传有界内核消息，没有数据面可写 FD |

`required_features` 表示本资源实际使用且接收方必须支持的位；`optional_features` 只广告能力，不准擅自改变已选编码。默认公共必需位为 2、3、4；带 arena 的 MarketBank 另要求位 1。ConfigControlV1 使用预留 feature 位 5：HELLO 声明配置角色时必须同时在 required_features 中声明位 5，接收端先检查必需 feature；WELCOME 确认该位后，才能发送 opcode 80–84 或使用 ConfigProjection 资源；未支持方拒绝 UnsupportedFeature，不能退回无收据的文件覆盖。此扩展只新增布局、资源种类和操作码，原有 ABI 1.0 字段与既有编码向量不变。逐记录 CRC32C 是位 0，可在建通道时启用；不能逐条改变通道设置。CRC32C 是 Castagnoli 循环冗余校验，用于发现编码/存储损坏，不提供身份认证。其反射多项式 `0x82f63b78`、初值与最终异或值 `0xffffffff`；`123456789` 校验值为 `e3069283`。

## 2. 资源、权限和页布局

### 2.1 文件组成

| 资源 | 文件字节布局 | 唯一写者 |
|---|---|---|
| MarketBank | ResourceHeader 4096；ProducerPage 4096；`capacity × 512` 槽位；四类 arena | 所属 md 发布者 |
| IntentBank | ResourceHeader 4096；ProducerPage 4096；`capacity × 1024` 槽位 | qualified 策略或可信 adapter |
| ReportBank | ResourceHeader 4096；ProducerPage 4096；`capacity × 512` 槽位 | 所属 exec |
| Cursor | ResourceHeader 4096；CursorPage 4096 | 该 bank 的该消费者 |
| ConfigProjection | ResourceHeader 4096；有界投影原始字节；零补齐到 4096 | 发布前的配置物化服务；发布后无写者 |
| 快照/目录/TopicSnapshotManifest 对象 | ResourceHeader 4096；SnapshotHeader 256；固定宽度条目；清单可含页对齐子快照；零补齐到 4096 | 发布前的对象构建者；发布后无写者 |

bank 的 `producer_page_offset=4096, slot_offset=8192, cursor_page_offset=0`。Cursor 的 `cursor_page_offset=4096`，其余数据区偏移、capacity、record_bytes、arena 字段均为零；`parent_resource_id` 指向被消费的 bank。每个 `(subscriber_instance, subscription_generation, bank_resource_id)` 独立创建 Cursor memfd，不能把多人的可写 cursor 放在同一个 FD 内。

顶层快照 `slot_offset=4096` 指向 SnapshotHeader，`entry_offset=4352` 是文件绝对偏移；`capacity=entry_count, record_bytes=entry_bytes`，两类原子页偏移与 arena 均为零。资源种类决定这些字段的解释：只有 resource_kind=1/2/3 的三个 bank 要求 capacity 为 64…1048576 的二次幂；Cursor（4）必须 capacity=record_bytes=0；快照/目录/清单（5/6/7/8）capacity 等于经长度校验的 entry_count，可以不是二次幂。ConfigProjection（9）使用 record_bytes=1、capacity=投影原始字节数，数据从 4096 开始，没有 SnapshotHeader 或原子页。清单内子快照没有独立 ResourceHeader，其 entry_offset 仍是整个清单文件的绝对偏移，详见第 3.3 节。

ResourceHeader.magic 为 ASCII `TWIPC001`，endian=1、atomic_width=8（字节）、page_shift=12、isolation_shift=7、clock_domain=1。`ResourceHeader` 从 Ready 开始永远不变；producer/cursor 页的非原子保留区永远为零。`descriptor_hash` 是整个 4096 字节头部在该字段置零后的 SHA-256；`ResourceDescriptor.header_hash`、MapWriter 与 ReadReady ACK 必须匹配它。头部的 `consumer_instance` 只用于 SPSC/Cursor；广播 bank 为零。新 bank 的 `predecessor_ring_epoch/cutover_seq/first_seq` 在准备阶段选定，满足 `first_seq=cutover_seq+1`，第一个 bank 的 predecessor/cutover 为零且 first_seq=1。`initial_membership_version` 记录激活时版本，之后以 ProducerPage 和控制 Applied 状态为准。

### 2.2 FD 能力握手

文件描述符（File Descriptor，FD）通过本机 Unix 域 `SOCK_SEQPACKET` 的 `SCM_RIGHTS` 传递；收到 FD 不等于已经有交易权。

1. supervisor 创建 memfd、设置长度并初始化，登记唯一写者及全部映射持有人。资源 offer 先只发给写者，`access=ExistingWriterMapping`；写者建立 RW 映射，返回匹配 offer、资源、头部 hash、长度和注册代次的 `MAP_WRITER`。
2. 创建方核验 ACK，撤销自己的临时写映射，设置并读回 `F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE | F_SEAL_SEAL`，本规范单写资源要求 seal 数值 `0x17`。保留写者已有映射是有意授权；封印不撤销它。
3. 此后才能向读者发 `access=ReadOnly` 的资源 offer。读者核验 FD 数量、文件类型/长度、seals、头部摘要、身份与目标平台配置，只建立 RO 映射，再发 `READ_READY`。读者不得执行任何 RMW。
4. owner 收齐必需确认后以 `ACTIVATE_RESOURCE` 应用激活。任何失败都关闭收到的未注册 FD，撤销准备状态；不能以收到 offer 或 mmap 成功代替 ReadReady。

不可变快照先完成内容和 hash、撤销所有可写映射，再使用 `F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL`（`0x0f`），不采用保留 writer 映射的方式。消息截断、控制辅助数据截断、重复/越界 FD index 或数量不符导致整帧拒绝，并关闭本帧收到的所有 FD。`SCM_RIGHTS` 不会自动降低描述符权限，因此未封印对象不能先交给只读方。[Linux 文件封印](https://man7.org/linux/man-pages/man2/F_ADD_SEALS.2const.html)、[Unix 域 socket](https://man7.org/linux/man-pages/man7/unix.7.html)

### 2.3 发布、消费与原子区

除 ProducerPage/CursorPage 的 `atomic_u64` 字段外，所有数据都是普通字节。原子字段只允许对齐的 64 位原子访问，不准混入普通读写或不同宽度原子访问。初始化在发布资源前完成，此后每个原子字段只有表中唯一写者。

写者先完成槽位及 arena 的所有普通写，再对 `published` 执行 release store。只读方先做 64 位 Relaxed load，若读到所需序号，再执行 Acquire fence，然后访问记录。读者结束该连续前缀全部借用后 release store `released_seq`；生产者也用 Relaxed load 加 Acquire fence 观察释放。只读页不能直接依赖 `load(Acquire)` 的平台实现，更不能做 compare-exchange。此组合与 64 位上限来自 [Rust 官方只读原子规定](https://doc.rust-lang.org/std/sync/atomic/index.html#atomic-accesses-to-read-only-memory)。

`read_seq` 是已取得处理权的连续前缀，`applied_seq` 是业务应用前缀，`released_seq` 是不会再访问共享字节的前缀。始终 `applied_seq <= read_seq <= published` 且 `released_seq <= read_seq`；复制后释放但稍后应用允许 `released_seq > applied_seq`。每个字段单调，独立读取的瞬时组合可能来自不同时刻，验证器不得用一组不一致采样误杀合法消费者；先按单字段边界检查，需一致组合时用控制 ACK。`outstanding_borrows` 仅供诊断，不能替代释放/退出证明。

对 bank 起点 F，初始 `published/read/applied/released=F-1`。序号 s 的下标为 `(s-F) & (capacity-1)`；允许写下一条 s 的条件为 `s - min(released[G]) <= capacity`。没有读者时最小值视为当前 published。新加入者从 J 开始，注册时 owner 先把它的内部 gating 基线设为 J−1；尚未 Ready 的 cursor 读数不能把该基线推进。所有普通槽位读取必须同时满足正确 resource/epoch、`sequence=s`、消息约束及已观察发布；二次检查 sequence 不能许可覆盖仍被借用的字节。

每批至多 64 条、有效 payload 总长至多 4 MiB；逐条预留全部槽位与 arena，再按连续完整前缀发布。未发布的写 guard 不能跨 Freeze；批处理不得留下可见的序号空洞。`published` 一次跨越多个记录时，其 release 覆盖此前所有记录。消费者应用/释放可批量推进，但任何未完成的低序号缺口都会阻止释放水位越过它。

## 3. 槽位、arena 与不可变对象

### 3.1 公共头与校验

Market/Report record 固定 512 字节，Intent record 固定 1024 字节，均 128 字节对齐。公共头固定 128 字节；`body_bytes` 分别为 384/896/384。`message_type` 决定消息体解释，`schema_major=1, schema_minor=0`。行情 `topic_or_scope_id` 是 topic；意图和回报是风险域，且公共头 `feed_generation=0`，多流因果放在 TriggerRef 中。生产者代次对 md 是其运行代次、对策略是 strategy_generation、对回报是 exec_generation；它们不互换。exec_generation 是主设计 exec_epoch 的 wire 字段名；另有 control_epoch/channel_epoch/ring_epoch，各自独立。Cursor 头部的 producer_instance 是实际消费者写者，producer_generation 使用 subscription_generation，consumer_instance 是反向读取该游标的 bank 生产者。

`SourceTimeValid/IngressTimeValid/PublishTimeValid` 位未置时，对应时间必须零。启用记录 CRC 的通道每条设置 CRC32CValid，`record_crc32c` 覆盖整个固定 record，把两个 CRC 字段都视为零；`payload_crc32c` 只覆盖有效 level 字节，None 时为零。禁用 CRC 时三个相关值/位全零。校验用于发现损坏，不能替代发布屏障或保护恶意并发改写。

Intent 与 Report 不允许外部 arena 或内联 level：payload_kind/offset/length/arena_class/allocation_seq 均为零。行情 Depth 的数组布局为所有买价层后接所有卖价层，每层 `i64 price_ticks + i64 qty_lots`；数量是该价位的绝对数量，零表示删除。买层按价格降序、卖层按价格升序，各侧同价只出现一次；数量不可为负。

### 3.2 arena 引用与回收

Depth 的 `level_count=bid_count+ask_count`，最大 4096，`payload_length=level_count×16`。0–12 层使用 InlineLevels，数组从 record 偏移 320 开始，外部偏移、class 与 allocation_seq 为零；超过 12 层使用 ArenaLevels，内联区全零。InlineLevels 的 `payload_length` 表示有效内联字节数，不是整个 192 字节区的大小。非 Depth 消息 payload_kind=None，其相关字段和内联区全零。

arena 的 class 表固定最多 4 项，表中 id 为 1–4，按尺寸升序；只允许选择能容纳 payload 的最小 class。每个 block 128 字节头后是 payload，再是零补齐。record.payload_offset 指向 payload 第一个字节，是整个 bank 文件的绝对偏移；`payload_offset-128` 必须与对应 class 的块边界一致。校验 class 范围、block 长度、resource_id、ring_epoch、allocation_seq、payload_length 和 CRC；ArenaBlockHeader 的 CRC 必须等于 record.payload_crc32c，禁用时两者都零；`allocation_seq=record.sequence`，在该 bank 中唯一，不能仅凭 offset 接受陈旧引用。

producer 在自己的预分配私有 free-list 中管理块，SHM 内没有多方争抢的 allocator。分配后直到相应记录被全部 gating holders 连续释放前，block header、payload 和零尾部不可改变。槽位与 arena 使用同一回收条件；工作线程必须归入持有者的连续 released 前缀。Freeze 使整个旧 bank/arena 停止写入；切换后的新 arena 不能借用旧 bank 的未释放块。超过 65536 字节的增量不截断、不分片发布，源进入 Invalid 并请求重同步；完整大簿走不可变快照。

### 3.3 快照与 instrument 目录

快照 magic 为 ASCII `TWSNP001`。MarketDepth 的 entry 是 DepthLevel（16 字节），买/卖顺序与数量同增量，`sequence=S` 表示快照已包含至 S 的完整业务状态；`feed_generation/instrument_version` 必须匹配后续 S+1。coverage_flags 指明有限覆盖与覆盖外未知；未知价格层不能当作零。

TopicSnapshotManifest 是多标的 topic 的 JOIN 基线，一个不可变 memfd 同时容纳清单与全部标的子快照。resource_kind=8，顶层 snapshot_kind=4，instrument_id/instrument_version 为零，sequence=S；entry_bytes=128，entry_count 为 topic 在该边界登记的完整标的数，范围 1–4096。条目使用 TopicSnapshotEntry，entry_flags 在 ABI 1.0 必须零，按 instrument_id 严格升序且唯一，记录 instrument/version、snapshot ID/hash、文件内 offset/length、同一 feed_generation/S、目录版本、feed_state 和该标的最后实际事件序号。最后实际事件序号可小于 S；这表示该标的在全 topic 截面 S 上没有更晚变更，不允许把其子快照截面也写成旧序号。

owner 必须在单一发布边界捕获 S 下全部标的状态；可以保留冻结版本或使用预分配的写时复制版本后继续 S+1，但不能遍历仍在独立更新的 mutable book 拼成混合截面。

JOIN 指向清单的 snapshot_id 与整个清单文件 hash，另带 topic_instrument_set_version/hash。集合 hash 的输入是按 instrument_id 升序连接每项两个小端 u64：instrument_id、instrument_version；该集合必须与 owner 的 topic 注册表及 JOIN 请求期望值完全相同。顶层 SnapshotHeader.state_version 保存集合版本，topic_instrument_set_hash 保存集合摘要，catalog_hash 固定同一目录。消费者核验条目数量、集合、所有子 hash 和共同 S，不得只加载自己当前关心的 symbol 就宣告整个订阅 Ready；策略可以在完整检查后只保留所需业务视图，但仍推进整个 topic cursor。新增/删除标的通过新的集合版本与可观察的目录/订阅事件生效，不能让 JOIN 看见混合版本。

清单条目区从 4352 开始，结束后补零到下一个 4096 边界；第一子对象从该边界开始，各子对象连续放置且大小向上补齐到 4096。每个子对象包含 256 字节 SnapshotHeader、有效条目与零尾部，不重复放 ResourceHeader；其 entry_offset=条目.snapshot_offset+256、object_bytes=条目.snapshot_bytes，snapshot_id/hash 与清单条目一致。子 hash 覆盖整个已补齐的子对象范围。顶层 entries_hash 只覆盖清单行；每行的 hash 再覆盖其子对象；JOIN 的 hash 覆盖整个 memfd。所有成员 channel_id/feed_generation/sequence/catalog_hash 必须与顶层相同，manifest_member_bytes 等于全部 snapshot_bytes 之和，resource_bytes 等于页补齐后的清单头/行区加该总和。禁止重叠、空洞、循环引用或指向外部可变 arena。

Depth topic 的子 snapshot_kind=MarketDepth；其他状态型 topic 使用 snapshot_kind=MarketInstrumentState（5），entry 是 512 字节 MarketRecord，保存该事件类的最新 Quote、MarkFunding、FeedState，或 Trade topic 的最后成交。其行按 message_type 升序且唯一，RecordHeader.sequence 使用子对象内从 1 开始的行号，source/ingress 时间保留原观测值。最后成交基线不提供逐笔历史；需要历史窗口或确定性回放的策略还必须具备 manifest 声明的完整输入日志。无有效状态的成员 feed_state=Bootstrapping/Invalid，不能 Ready；不得从清单中省略它。Depth 子快照的 level 数量、其他子快照的行数可以不同，但截面 S 必须相同。

参考部署的两个 topic 使用 event_class=NormalizedMarket（7），同一 topic 内有深度、报价、成交、参考价和 feed 状态。其每标的子对象采用 snapshot_kind=NormalizedMarketSnapshot（6），不会只保存订单簿而遗漏其他状态。SnapshotHeader.entry_bytes=64、entry_count=2、entry_offset=子对象起点+256，条目是两个 SnapshotSection：第一项 StateRecords（1），第二项 DepthLevels（2）。每项给出 entry_bytes/count、文件绝对 offset、length 和该段有效字节的 SHA-256；SnapshotHeader.entries_hash 覆盖这两个 section 描述符。

StateRecords 从子对象起点+384 开始，entry_bytes=512；按 message_type 升序保存最多一条 Quote、Trade、MarkFunding、FeedState，FeedState 必须存在，其他项是否必需由该标的在 topic 的注册能力集合决定，缺必需状态不能 Ready。DepthLevels 紧接状态记录，entry_bytes=16，先买后卖；层数必须等于 SnapshotHeader.bid_count+ask_count。两个 section 的 length 都等于 count×entry_bytes，数据连续放置，最后补零到页边界，禁止重叠或外部指针。各段通过描述符 hash 与子对象 hash 共同绑定同一 S、feed/catalog/instrument 版本；最后成交仍只表示最新事件，不能替代逐笔历史日志。已有单一事件类的 MarketDepth/MarketInstrumentState 子对象继续有效。

一个清单连同所有子对象共用 64 MiB 上限，在每 owner 的 8 个快照对象配额中占 1 个；每个子对象至少一页，全部字节都计入该对象与既定 512 MiB owner 预算。对子集合单独分配 64 MiB 或把子对象藏在预算外均不允许。完整清单无法装入预算时拒绝该订阅/部署配置，不能降成部分标的清单或把多标的 topic 改成单标的来通过此契约。清单作为一个资源登记 holders；任何子借用未结束都阻止整个清单回收。

ExecutionState 的 entry 固定 512 字节，使用 ReportRecord 或 OrderOwnership 消息体的 OwnershipRecord；消息 type 标识订单、预留、余额、持仓、授权及父子关联。每种事实按稳定 identity 排序，`RecordHeader.sequence` 是 1 起的快照行号，`exec_journal_seq <= SnapshotHeader.sequence`，无历史成交重放副作用。快照先完整装载到新视图再原子替换，S+1 后的 journal 增量继续按去重契约应用。每一活动/Unknown 订单必须有 OrderState 与 OrderOwnership 行，关联列表/algo 结构必须包含父、子关系；缺任一项不能 Ready。

InstrumentCatalog 的 entry 是 InstrumentEntry（256 字节），按 instrument_id 升序、唯一；`catalog_hash` 在目录自身 SnapshotHeader 中为零以避免自引用，其使用者引用整个已封印目录文件的 SHA-256。所有对象 `entries_hash` 仅覆盖 `entry_count×entry_bytes` 有效条目，JOIN 的 snapshot_hash 必须覆盖完整 TopicSnapshotManifest 文件，执行 RESYNC 的 snapshot_hash 覆盖完整执行快照文件（含资源头与零页尾）。除清单顶层外，topic_instrument_set_hash/manifest_member_bytes 为零。`object_bytes=resource_bytes`，上限 64 MiB；所有 offset、count、size 必须相互吻合，过期/缺失对象保持 Syncing。

价格值为 `price_ticks × price_step_mantissa × 10^price_exponent`，数量值为 `qty_lots × qty_step_mantissa × 10^qty_exponent`；quote_amount 使用 quote 步长。mantissa 为正 `u64`，各指数范围 −18…18，乘法与单位转换使用检查溢出的 i128。合约价值另乘 `contract_multiplier_mantissa × 10^contract_multiplier_exponent`，规则由 product/filter_version 固定。`rounding_policy=0` 表示不自动舍入；策略提交非法步长或超出过滤器直接拒绝。symbol 是长度 1–32 的 ASCII 原始标的名，尾部为零。

每条订单/行情公共头都带 instrument_id 与 instrument_version，意图另带目录 hash、config/risk policy 版本；exec 必须用自己持有的相同生效目录与配置核验。目录改变创建新不可变对象和版本；旧订单永久保留原精度版本，不能用新 tick size 重新解释旧整数。`filter_hash` 绑定部署的完整过滤器快照；本文列出的 min/max 只是固定数值过滤字段，动态价格带、保证金等规则仍由执行契约验证，不能从这些字段推断全部交易权限。

## 4. 数据消息语义

下面先给类型和有效字段；精确 ID、枚举和值在后面的表及 JSON 中。所有消息体的字段白名单以 JSON 的 message_constraints 为准，并在第 8 节完整列出。先检查白名单外字段为零，再检查枚举和 value_validity 掩码，最后执行业务准入；不得把数值有效位当作允许使用任意联合体字段的许可。

| 行情消息 | MarketBody 的有效业务字段 |
|---|---|
| Quote | bid/ask price 与 qty、book_revision、feed_state、exchange_event_ns/source_schema_id；一侧缺失时该侧 price/qty 都为零，Live 不允许交叉簿 |
| Trade | trade_id、price/qty、side、trade_flags、exchange_event_ns、feed_state/source_schema_id；trade_id 是该 venue/product/instrument 的原始数值 ID |
| Depth | venue_seq_first/last/previous、book_revision、level_count/bid_count/ask_count、depth_flags、feed_state、exchange_event_ns/source_schema_id、levels；不同来源原始序号桥接由 market 契约校验 |
| MarkFunding | mark/index price、funding_rate_e12、next_funding_utc_ns、exchange_event_ns、feed_state/source_schema_id；资金费率定标 10^-12 |
| FeedState | feed_state、reason、exchange_event_ns/source_schema_id；状态变化本身也占 topic_seq；失败通知另外经控制传给 exec，不能等待满 ring |
| MarketSnapshotRef | snapshot_id、snapshot_sequence、coverage_depth、feed_state；完整对象通过控制面检索 |
| CatalogRef | snapshot_id 指目录、instrument_catalog_version；公共头 instrument_id/version 为零，快照 sequence 是目录版本 |

`source_schema_id` 指 namespace 注册的原始协议 schema；交易所时间可能为零，只有相应有效位/字段契约允许时才能用作延迟测量。Depth `venue_seq_previous=0` 表示该协议不提供 previous，不能伪造 Futures 的 pu。Quote/Trade 的 topic_seq 连续也不替代 venue 的原始序号连续性。

### 4.1 意图

意图的稳定去重键为 `(logical_strategy_id, risk_scope, strategy_generation, intent_seq)`；instance、channel 和 exec generation 必须与已绑定身份一致。`economic_intent_id` 标识持续经济动作；原请求确认丢失时用 QUERY 查询原键。同一键不同规范化业务内容为 IdempotencyConflict；规范化内容包含全部 IntentBody 与公共头的 instrument/scope/producer generation，不含 transport sequence、时间戳测量字段和 CRC。策略不能为未解决的同一经济动作换 ID 重试。

`intent_seq` 在一个策略授权代次/风险域内从 1 连续分配；队列满而未发布时不消耗序号。接收端在明确的 channel 恢复后可接受已接纳键的精确重试，但新键出现缺口时返回 SequenceGap 并要求查账。重试必须保留业务 created/valid_until 与内容，不能刷新期限。transport sequence 在新 channel 从 1 开始，intent_seq 不因此归零。订单身份由 exec 分配，New 的 order_id 为零；Cancel/Amend/Modify 必须携带既有 order_id 和 expected_owner_revision。

- New：qty_lots>0；Limit/PostOnly 等价型必须有正 price_ticks；Market price_ticks=0。QuoteAmount 位只用于经产品能力确认的现货 Market 买单，此时 quote_amount_units>0、qty_lots=0。条件类型须有正 stop_price_ticks，working_price、PriceProtect、ClosePosition 按产品能力组合验证。
- Cancel/QueryOrder：order_id 非零，数量和价格字段为零；CancelList/CancelAlgo 分别使用 list_id/parent_order_id，普通 order_id 可零。撤单仍验证管理权与当前动作授权。
- CancelReplace：order_id 指旧单，price/qty 描述新单，使用新的 intent_seq；按最坏新旧并存预留。AmendKeepPriority 只允许原单减量且 price=0；Modify 用明确新价新量。三者不可混用，未知结果保留原身份与占用。
- created_mono_ns <= now < valid_until_mono_ns，且 valid_until 不能超过当前授权/控制租约。expire_utc_ns 只用于 GTD；它不替代本地意图期限。
- trigger_count 为 0–8，前 N 个 TriggerRef 有效、后续全零。新增风险至少一个引用；策略 manifest 可要求更多流，exec 对照完整依赖集合核验，不允许通过省略引用绕过多流 readiness。每个引用带 topic/feed/ring/sequence、instrument/version、观测时间，required_feed_state 必须 Live；跨产品不合成全局序号。
- `recovery_manifest_hash/catalog_hash` 对新增风险非零并由 exec 解析为已验证对象，strategy 自报 hash 不构成持久化证明。取消/查询可按执行处置契约不要求行情引用，不能因此变为新单权限。

qualified SDK 的 publish 消耗唯一写 guard，发布到 release 前不能再写。exec 在持有合法读取权时把整个意图复制到有界本地结构，再校验与应用；copy 不证明恶意作者没有并发改写。isolated_adapter 对策略用同一 1024 字节编码的 `SOCK_SEQPACKET`，核验固定长度后由独立可信进程以自己的受登记写者能力发布；策略 payload 的身份必须与 adapter 的会话绑定匹配。

### 4.2 回报、事实与身份

report_seq 是每个 `(exec_generation, subscriber_instance, report_subscription_generation)` 的连续订阅事件序号；正常 channel 重建与回放不重编号，新的订阅代次才重新从 1 开始。RecordHeader.sequence 是当前 channel 的传输序号，两者不能混用。exec_journal_seq 是风险域 reducer 的事实顺序，某订阅过滤掉其他策略事件时允许该字段跳号；report_seq 不允许跳号。

ReportBody 的 instance/strategy_generation 指当前接收/管理实例，origin 身份由 OrderOwnership 完整报告。成交/费用永远按原订单归属入账；先到成交后到 ACK 不回退累计数量。venue_order_id 最长 64 字节、venue_trade_id 最长 32 字节，按长度保存 ASCII，不补 NUL；交易所身份需更长时拒绝该适配器配置，不能截断。内部 order_id 是固定 16 字节。

| 回报消息 | 字段含义 |
|---|---|
| IntentStatus | status=intent_status；intent/economic/order/reservation 身份、数量/价格、reason、send_state；Accepted 只在生产提交水位成立后产生 |
| OrderState | status=order_status；quantity/cumulative_qty/leaves_qty、price、send_state/cancel_state、owner_revision、venue ID；尚未知成交余量不伪造为零 |
| Fill | status=当前 order_status；quantity=原订单量、cumulative_qty=累计成交、leaves_qty=已知剩余量、price_ticks=订单价；last_qty/last_price_ticks=本笔成交；intent/economic/order/reservation 身份、owner_revision、venue_order/trade ID 及各自长度、send_state/cancel_state、side/position_side 均可携带；fee_units/fee_asset_id/asset_version/asset_exponent 说明费用；fee_units 为对策略资产的有符号现金流，扣费为负、返佣为正 |
| Balance | header.instrument_id=0；fee_asset_id 指资产，asset_version/exponent 指单位；quantity=总资产、cumulative_qty=可用、leaves_qty=交易所锁定，value_0=本地额外预留，aux_version=账户基线版本；均为该资产最小计数单位，不把查询快照重复入账 |
| Position | quantity=净 lots；Hedge 按 position_side 独立行；price=入场价，value_0=已实现损益、value_1=未实现损益、value_2=保证金，现金数值用 fee_asset_id/exponent 的单位；aux_version=持仓版本 |
| Reservation | status=reservation_state；reservation/order/intent 身份；quantity=预留 lots，value_0=原预留资产单位、value_1=剩余、value_2=已转经济事实；fee_asset_id/exponent 固定资产单位 |
| GrantState | status=control_state；authorization_epoch/strategy_generation/exec_generation、aux_version=risk_gate_revision、value_0=permissions、value_1=valid_until_mono_ns；不是策略自发授权 |
| ResyncRequired | reason、aux_version=最后可回放 journal 序号；保持处理账户事实，停止向满队列强行写，控制 STATUS 同时发布缺口状态 |
| LedgerAdjustment | aux_id=不可复用账项 ID、aux_version=更正版本；fee_units=有符号资产变更，reason 表示错误码时仅用于拒绝，业务调整类别用 status：1资金费、2费用更正、3强平、4ADL、5外部调整、6归属移交；Correction 位明确更正 |
| ExecutionSnapshotRef | aux_id=snapshot_id、aux_version=S、value_0=对象字节数；其他业务数值为零 |
| OrderOwnership | 独立 OwnershipBody 保存 origin_strategy/instance/generation/intent、当前 manager、owner_revision、parent/list/algo 身份及 relation_kind；移交不改 origin 字段 |

货币资产 exponent 与 asset_version 必须匹配已确认的资产目录，禁止任意改变指数制造余额。`report_kind=message_type & 0xff`；OwnershipBody 的 status 使用 order_status。`value_validity` 的位 0–9 分别表示 quantity/cumulative_qty/leaves_qty/price/last_qty/last_price/fee/value_0/value_1/value_2 已知；未置位的数值必须零且解释为未知，已知零必须置位。JSON 为每种回报列出 value_validity_allowed_mask 与 required_mask：Fill 允许 0x07f，至少有 LastQty/LastPrice（0x030）；其 quantity/leaves_qty/price 均在明确白名单内。已知零与未知分开表达，Unknown 的余量不能伪造为已知零。status 的有效枚举由消息 type 决定，不能把数值 3 在所有消息中解释成同一状态。所有回报均不以策略 ACK 释放资金；账户调整、Unknown 与迟到事实按执行契约处理。

## 5. 控制协议与状态转换

### 5.1 帧、会话和幂等

控制 envelope 固定 128 字节，magic 为 ASCII `TWCTL001`；frame_bytes 包括 envelope 与 body，总长 ≤65536，单帧最多 16 FD，单会话最多 256 个未决请求。本机一帧对应一个 SOCK_SEQPACKET 数据报；跨主机控制连接使用相互认证的 TLS，按 envelope.frame_bytes 定界，禁传 FD/内存资源能力。远端控制只管理远端资源代理，不能把远端 offset 当成本地地址。

所有控制帧必须校验 CRC：body_crc32c 覆盖实际 body；header_crc32c 覆盖完整 128 字节 envelope，只把 header_crc32c 字段置零，保留实际 body_crc32c。header_bytes 必须 128；普通 opcode 的 body 长度必须与该帧 opcode 对应的布局完全一致。RESOURCE_OFFER 唯一例外，为 64 字节 OfferPrefix 后跟 1–16 个 ResourceDescriptor，descriptor_bytes=128，frame_bytes=128+64+N×128。

请求 flags=Request，status=0；Received 响应为 Response|Received，成功应用为 Response|Applied，失败为 Response|Error 且非零 status；异步状态为 Event。SWITCH 的结构化 ResyncRequired 与配置扩展的 CONFIG_STATUS 错误响应也使用 Response|Error，消息体和 opcode 例外按各节专门规定。其他组合拒绝。响应复用原 request_id/control_epoch/operation 信息，sender/target 对调。Received 一律用 STATUS opcode，普通失败用 ERROR opcode；成功 HELLO 返回 WELCOME，JOIN 返回 JOIN，QUIESCE 返回 QUIESCE，SWITCH 返回 SWITCH，EXEC_LEASE_RENEW 返回 EXEC_LEASE_APPLIED，配置扩展按第 5.7 节返回对应配置操作码，其余成功返回 STATUS。SWITCH 的 ResyncRequired 使用原 SWITCH opcode 与 SwitchBody，flags=Response|Error、status=32；配置扩展按第 5.7 节使用 CONFIG_STATUS/ConfigControlBody 返回已解析且已认证的业务错误；其余错误仍用 ERROR/StatusBody。因此解码始终按实际 opcode 找消息体，不把 StatusBody 当作原请求消息体解析。HELLO 用 control_epoch=0、目标为预配置 supervisor；WELCOME 选择准确版本、features、身份与新 control_epoch，后续全带它。身份还必须与操作系统 peer credentials、pidfd 注册或远端认证主体一致，payload 不能覆盖连接身份。

资源会话默认 resource_session_heartbeat_ms=500、resource_session_lease_ms=3000，WELCOME 给出接收方单调时钟域的截止时间。这只管理控制连接与资源操作会话；exec 的短执行资格租约是独立的 exec_lease_ms=100、exec_lease_renew_ms=25，StrategyGrant 的默认业务有效期又独立为 60000 ms。资源会话尚在线、或策略授权尚未到期，都不能代替当前 exec 短租约。HEARTBEAT 的 StatusBody.state_version 是 lease_revision，subject_id 为会话 nonce 前 16 字节，其他业务字段零；接收方认证并接受更高 revision 后续租并返回 STATUS。租约过期使相关新增风险关闭，不能回收仍存活的共享内存读者；重新续租不自动恢复已关闭的风险门。

幂等键为 `(认证主体, control_epoch, request_id)`，命令摘要为 opcode、operation_id/revision、expected_state_version 和完整 body 的 SHA-256；重试字节含义必须一致，FD重试用相同 resource_id/header_hash 匹配并关闭多余副本。同键同摘要返回已持久保存的 Received/Applied/Rejected 结果，同键异摘要返回 IdempotencyConflict。具有经济效果的 operation 另按 `(operation_id, operation_revision, opcode, risk_scope)` 持久去重；旧 control_epoch 过期后先 QUERY 旧键/operation，再以新 request_id 获取原结果，不能重做一次授权或管理权转移。

先查幂等结果再比较 expected_state_version，已 Applied 的重试不会因为版本已前进而变成失败。普通终态结果保留至少到会话关闭后 24 小时；授权/订单/发布 operation 的去重记录随其审计与恢复日志永久保留身份和结果索引，不能缓存淘汰后执行同键动作。达到并发上限返回 RateLimited。所有变更命令按 expected_state_version 比较后应用，状态版本单调增长；Received 只证明解析接收，只有 Applied 的 state_version/applied_exec_seq 证明相应 owner 已应用。

成功 WELCOME 使用 WelcomeBody；JOIN 的最终成功返回 JoinBody，QUIESCE 的最终成功返回 QuiesceBody，SWITCH 的成功或结构化 ResyncRequired 返回 SwitchBody；EXEC_LEASE_APPLIED 返回 ExecLeaseBody。它们均按实际 opcode 解码，普通状态/错误使用 StatusBody；RESOURCE_OFFER 本身可 Event，映射 ACK 通过独立 MAP_WRITER/READ_READY 请求关联 offer_id。ERROR detail_utf8 最长 30 字节，截断须在 UTF-8 字符边界；业务行为只依赖数值 status，不能依赖错误文本。

### 5.2 exec 短租约与本机挑战

EXEC_LEASE_CHALLENGE（52）是 exec 发出的 Event，消息体 ExecLeaseBody 绑定 exec_instance/generation、risk_scope、host_boot_id、唯一 challenge_id、递增 lease_revision、当前 control_epoch，并用 exec 本机 CLOCK_MONOTONIC 记录 issued_mono_ns 和 deadline=issued+100 ms。控制方每 25 ms 处理新挑战，以 EXEC_LEASE_RENEW（53）Request 原样回传挑战正文；不能自行延长 deadline 或把控制方的单调时间写进来。

exec 校验认证授权、精确挑战内容、host_boot/exec/control 代次、仍未过期的本地 deadline，以及 lease_revision 高于已应用版本后，应用该固定 deadline，返回 EXEC_LEASE_APPLIED（54）、Response|Applied 与同一 ExecLeaseBody。较晚收到的旧挑战不能倒退或延长新租约；响应延迟消耗挑战剩余有效期，不能以接收时刻重新加 100 ms。重启废弃全部旧挑战并从 Frozen 开始。挑战/续租维护单独的短时状态，不要求每 25 ms 为所有 StrategyGrant 写一次授权日志。

GrantBody.control_lease_deadline_mono_ns 绑定 exec 在该次控制申请中认可的本机挑战 deadline，其 host_boot/exec generation/control_epoch 从受认证会话与挑战表一并核验；不能拿资源会话的 3000 ms deadline 填入。StrategyGrant 可有更长的 valid_from/valid_until，但 RELEASE_RISK 应用当刻还要检查短租约和策略健康；之后每次接纳与首次可能发送都检查当前已续期的 exec 租约、当前策略授权/健康和意图期限。租约续期不会复活已撤销的授权或旧代次，策略授权到期也不会因短租约仍有效而延长。意图 valid_until 不得越过它所依赖的各项 deadline；等待后仍须在发送点重查。

### 5.3 JOIN：先保留，再快照与追平

supervisor 向 topic owner 发 JOIN，指定 instance、subscription_generation、topic/channel/feed/ring、已建立的 bank/cursor 能力、QoS、catalog 与 deadline。请求的 join_seq/snapshot_seq/snapshot_id/snapshot_hash 为零；owner 比较请求的 topic_instrument_set_version/hash 与自己的注册表，在串行发布边界选择 S，令 J=S+1，增加 membership_version，并先把 pending reader 的内部 gating 设为 S，再生成包含完整标的集合的一致 TopicSnapshotManifest(S)。零 baseline 只出现在 JOIN 请求，不能作为可消费的结果。

owner 的最终 JOIN 响应填充完整 J、S、清单 snapshot ID/hash、topic 集合版本/hash 和已应用 membership；集合在准备期间发生变化则返回 VersionConflict，不能静默替换请求的集合。既有 ResourceHeader.first_seq 仍是 bank 起点 F，不能改成 J。消费者验证完整清单及全部子快照，把自己的 cursor 初始 read/applied/released 设为 S，完成基线装载后从 J 连续消费；JOIN_READY 回传完整相同绑定与已追平的实际状态。owner 校验资源 ReadReady、快照、generation、deadline 与 cursor 合法性后才转为 Active；交易 readiness 还需 exec 应用。待加入者未确认前按 S gating，超过 deadline 进入 quiesce/退出回收流程，不能直接越过其可能持有的指针。

JoinReady 与 ordinary cursor 没有使策略获得增险权的能力。已有历史不足、快照过期或元数据不匹配返回 ResyncRequired，生成新的 subscription_generation 与新基线；不能用修改旧游标伪造覆盖。

### 5.4 FREEZE/SWITCH：S 与 S+1 的唯一衔接

Prepare 为新 bank B 选定计划 cutoff S（不小于当前完整 published），把 B 的 immutable header 固定为 `predecessor=A.ring_epoch, cutover=S, first_seq=S+1`，登记新 membership 和每个迁移者的独立 cursor、预先 gating=S。所有迁移者完成资源映射/ReadReady；到达计划 S 但准备未完成时，A 不再领取 S 之后的槽位，按既定容量/期限处理准备失败。不得在封印头部之后临时改 cutoff。

FREEZE 请求/描述符精确绑定 freeze_id、A/B resource、channel/feed、old/new ring、S、membership，且 new_first_seq=S+1。唯一发布者在完整记录边界确认 published=S、无未完成写 guard，先写 frozen_seq/successor_ring_epoch/freeze_membership_version，再 release store lifecycle=Frozen；这些 freeze 字段从此不变。控制 Applied 结果可反复查询；普通 ring 满时也必须通过控制连接观察到该状态。

B 激活时 published=S，各新 cursor 初值 S，按同一 feed_generation 从 S+1 连续发布。消费者完成 A 至 S 的业务处理且释放所有借用后发 SWITCH Request，switch_kind=Prepared，携带 old_applied=old_released=S、freeze_id、两个 cursor、新 bank、subscription_generation 和新 membership；snapshot_id/seq 在连续切换为零。owner 校验匹配后用 opcode=SWITCH、flags=Response|Applied、status=0 返回 SwitchBody，switch_kind=Applied，并回显经核验的全部身份/水位；消费者从 B 的 S+1 开始；尚未切换者一直受 B 的预注册 gating 保护。

不能从旧 cursor 读到 S 就推断旧读者已 quiesce；SWITCH 的显式 ACK、旧 cursor 的 Quiesced 状态和持有人登记联合关闭旧借用能力。无法追平者可用 SWITCH Request、switch_kind=ResyncRequired 报告实际 old_released/applied 水位；owner 用 opcode=SWITCH、flags=Response|Error、status=ResyncRequired（32）、SwitchBody.switch_kind=ResyncRequired 返回明确结果。请求与响应的 snapshot_id/snapshot_seq 都必须零，这个回复只要求重新 JOIN，不授予新 baseline。新的清单 ID/hash 与集合绑定仅由后续 JOIN 给出，不能对连续 SWITCH 填一个快照后跳过旧事实。旧 bank 仅在全部注册 holders 明确 Quiesced/Detached 或其所有进程退出证据成立后 Retire；新 bank 的 gating=S 不构成旧 bank 释放证明。

冻结期间 md 崩溃、新 bank 找不到或前驱不匹配，消费者禁止交易，进入新 feed_generation 重同步。membership 的 JOIN/DETACH/SWITCH 都由 publisher 串行应用，并使缓存 gating 最小值失效；晚到 ACK 不改变其他 subscription_generation 或 bank 的成员关系。

### 5.5 GRANT、撤销和管理权

GrantBody 的所有策略/exec/风险域身份必须与认证控制主体、operation 成员清单和已登记通道相符。GRANT 使用 grant_phase=Prepare，记录待授权状态；PREPARE_HANDOVER 同为 Prepare，但要求 handover_hash。COMMIT_OWNERSHIP 使用 phase=CommitOwnership，检查已确认交接及当前 exec 状态，持久应用新的管理权和 authorization_epoch，新增风险仍关闭。RELEASE_RISK 使用 phase=ReleaseRisk；仅 exec 可以在单写 reducer 中比对 expected_risk_gate_revision 后打开权限。

新增风险必须满足：当前 strategy/exec generation、精确 expected_exec_state_version、owner_revision、operation revision、配置/风险版本有效；account_baseline/md_readiness/recovery_manifest/instrument_set 摘要都指向 exec 已验证的对象；在 valid_from≤now<valid_until 且不超过控制租约截止；跨域成员还需 member_set_hash 与 group_certificate_hash 对应同一已准备/已提交成员集合。单域的 group_certificate_hash 可零，但仍核验本域 operation 与 member_set_hash。缺少摘要对象返回 MissingResource/RecoveryEvidence，保持 Prepared，不以哈希字符串本身作为证明。

REVOKE 使用 phase=Revoke，可不携带恢复/行情摘要、permissions=0；exec 先关闭相关闸门、推进 risk_gate_revision，再持久化与返回 Applied。它不释放订单预留。Halt/补偿同样推进 gate revision；旧 RELEASE_RISK 即使未过 deadline 也因 revision 不匹配而失败。重新开放要更高 operation revision 与新的证明，不能将 strategy_generation 回退。

authority_kind=StrategyGrant、ExecutorRiskAction、GuardianEmergencyGrant 分开校验：GuardianCancel 位只能授给 guardian 且绑定正式隔离证明的 operation，RiskReduce 不允许绕过真实最坏敞口检查，NewRisk 只能由当前策略授权/exec执行所有者使用。精确账户预算、过滤器和故障处置政策以风险契约为准，字段不能产生通用全权令牌。

StatusBody 的 Applied 结果绑定 state_version、applied_exec_seq、exec/strategy generation 与 risk_gate_revision；回报 GrantState 向策略同步相同事实。跨域只证明各域自己的应用点，不存在一个 control ACK 使多个 exec 原子生效。

### 5.6 QUIESCE、DETACH、查询与恢复

owner 发 opcode=QUIESCE、flags=Request 的 QuiesceBody，绑定 subscriber/bank/cursor、subscription/channel/ring 代次、membership 与 detach_reason；请求中 released_seq/applied_seq/evidence_kind/exit_evidence_id 为零。消费者停止新借用、结束内部全部引用后，使用相同 opcode/request_id/control_epoch、flags=Response|Applied、status=0 返回 QuiesceBody，回显绑定并填入实际 released_seq/applied_seq 与 evidence_kind=ReaderQuiesced（1）；exit_evidence_id 仍为零。这些水位只证明各自含义，允许 released>applied。无法完成时用 ERROR/StatusBody 返回 NotReady，不发伪造的 Quiesced 确认。消费者 release store Cursor.lifecycle=Quiesced 前先发布 quiesced_seq 和最后 released；owner 在 acquire 观察后核验 ACK。`quiesced_seq` 是该消费者最后曾可能访问的序号，不要求强行等于 bank published。DETACH 由资源管理者应用；活进程必须有完整 quiesce 能力撤销确认，死亡进程使用 SupervisorProcessExit 和不可复用 exit_evidence_id，查询实际 pidfd/holder registry 验证。心跳超时不是 exit evidence。

QUERY 按 query_kind/subject/original key 查真实状态；intent 查询使用 logical_strategy_id、strategy_generation、intent_seq，风险域由已认证 target exec 绑定。STATUS 返回最近已应用状态而非 supervisor 期望值。RESYNC_REQUEST/ACK 绑定实例、exec_generation、last_applied_exec_seq、snapshot ID/hash/S、channel/subscription generation；有完整历史则从下一事实回放，否则由 exec 生成 snapshot(S) 再接 S+1。ACK 仅证明视图追平，exec 还需重新应用授权。

### 5.7 配置提交、运行时应用与本地文件收敛

外层 HTTP 控制接口由[配置契约](configuration.md)和 OpenAPI 定义；本节关闭其控制组、配置物化服务与目标进程之间的 Unix 域 socket 链路。opcode 80–84 都使用固定 ConfigControlBody（768 字节），加 envelope 后为 896 字节。完整 TOML/JSON 不放进控制帧、热行情或意图 ring；请求只携带经认证对象引用，逐笔交易不调用这些接口。

| 操作码 | 请求发送方 → 接收方 | 成功响应 opcode / body | 必须保存的事实 |
|---|---|---|---|
| 80 CONFIG_PREPARE | ConfigController → ConfigMaterializer | 80 / ConfigControlBody，Response\|Applied，Prepared | 控制提交证明、固定目标与旧/新配置绑定已验证，ConfigPrepared 本地 journal 收据成立；还未冒充运行时应用或文件完成 |
| 81 CONFIG_APPLY | ConfigMaterializer → 目标 Runtime | 82 CONFIG_RUNTIME_APPLIED / ConfigControlBody，Response\|Applied | 已有 desired 文件及 Prepared journal 证明；目标先持久保存 ConfigApply，再在 reducer 边界应用投影 |
| 82 CONFIG_RUNTIME_APPLIED | 作为 81 的响应，或 Runtime → ConfigController 的独立报告 | 独立报告用 84 CONFIG_STATUS / ConfigControlBody 确认登记 | 真实 runtime instance/boot/generation、applied_event_seq 与 runtime/journal receipt；file receipt 必须零 |
| 83 CONFIG_FILE_DURABLE | ConfigMaterializer → ConfigController | 84 CONFIG_STATUS / ConfigControlBody，Response\|Applied | Desired 或 Effective 的实际文件持久收据；Effective 还引用已核验的 RuntimeApplied 收据 |
| 84 CONFIG_STATUS | 认证参与者查询原 operation/target；Controller 可发状态 Event | 84 / ConfigControlBody | 返回当前实际目标/全操作状态；只有 Controller 能确认整个固定目标集合 Succeeded |

`role=ConfigController（9）` 对应 actor_kind=Controller（1），`role=ConfigMaterializer（8）` 对应 Materializer（2）；原有进程角色 1–7 通过绑定到具体目标的 runtime 端点使用 Runtime（3）。sender_instance、peer credentials、控制会话、登记主机与 issuer_boot_id 必须匹配真实发送者；actor_kind 只是受校验字段，不能靠填写它取得角色。物化服务不能发送 CONFIG_RUNTIME_APPLIED，runtime 不能发送 CONFIG_FILE_DURABLE 或在自己的收据中填写 file_receipt_hash。Controller 可以汇总已核验的他方收据，不允许把自己的观察写成它们的原始收据。

ConfigControlBody.operation_id/operation_revision 必须等于 envelope 中的同名字段。process_id/target_host_id 是稳定配置 ID，分别以 u16 长度加 bytes[96] 表示；实际配置场景只接受 1–63 字节的 `^[a-z][a-z0-9-]{0,62}$`，尾部补零。UUID 从 HTTP 的规范文本按 hex 从左到右解码为 16 字节，不使用 bytes_le。稳定 ID 与运行身份分开：expected_instance_id、expected_boot_id 是目标运行实例及进程 boot，expected_host_boot_id 是目标主机 OS boot，expected_exec_generation 对 exec 必填，对其他角色为零；instance_slot_id 绑定稳定或滚动替代槽。它们都由原计划、受认证注册表与接管证明确定，不能由收件方换成“当前碰巧活着”的实例。

Hot/Quiesce/NoOpRevisionAdvance 使用原 target 身份。RollingRestart 的 CONFIG_PREPARE 可以准备原计划的 predecessor；向 successor 发 CONFIG_APPLY 前必须有非零 successor_binding_hash、SuccessorBinding 位及已持久的 SuccessorBound 对象，证明当前 expected instance/boot/generation、slot、host 和发布文件正是原计划的一次性 successor。其父子身份转换和退休/切换证明沿配置契约核验；计划外重启、第二个 boot 或任意更换主机都拒绝，不能只更新 expected 字段继续旧 operation。

config_revision/base_revision 与 global_config_hash/base_global_config_hash 绑定整个已提交配置链。expected_runtime_revision 单独绑定计划看到的该目标真实版本：恢复已有部分应用或合法 supersedes 时它可能落后于全局 base_revision，但这种差异必须由原计划明确包含。首次应用必须核验当前 runtime revision 恰好等于 expected_runtime_revision，且新 config_revision 更高；当前目标已是更新版本时返回 VersionConflict，不能回写旧配置。先按持久 operation/target/phase 幂等键查原结果，再做首次应用的版本比较；相同键同内容返回原事实，同键异内容拒绝。envelope.expected_state_version 与 body.state_revision 比较的是该目标的配置状态版本，不与 exec 每笔成交的 journal 序号混用。

哈希具有不同用途：global_config_hash 是完整规范化配置的 JCS 内容摘要；projection_hash 是该目标获授权 payload 的 JCS 摘要；projection_policy_revision 固定投影规则；projection_object_hash 是实际交付对象全部原始字节的摘要。完整交付对象还包含配置契约规定的全局版本、目标身份和投影政策等元数据，因此对象 hash 与业务 projection hash 不必相同。control_commit_ref 的 32 字节是 `artifact://...@sha256:<hex>` 提交证明的 SHA-256 部分；接收方从受认证对象服务解析该证明，核验 operation、base/new revision/hash、固定 target_set_hash、分类、权限和投影。不得将 body 中自报的 hash 直接视为提交或授权证明。

投影最多 4 MiB。projection_transport=AuthenticatedObjectService（1）时 projection_resource_id 为零，按允许的内容地址服务取 exact projection_object_bytes 字节并校验 hash；不能把任意客户端 URL 当成对象服务。transport=ImmutableMemfdResource（2）时先用已有 RESOURCE_OFFER/READ_READY 交付 ConfigProjection（kind 9）只读资源，再在 CONFIG_APPLY 中引用 resource_id，配置帧 fd_count=0。该资源头部不变，数据从 4096 开始，capacity=projection_object_bytes、record_bytes=1，resource_bytes=align_up(4096+object_bytes,4096)，零页尾不参与 projection_object_hash；全文件使用 0x0f immutable seals。consumer_instance 必须是获权目标；给 runtime 的 FD 只含其投影，不能交付完整全局配置再靠应用自觉过滤。projection_format=1/2 分别为 TOML/JSON，解析与凭据引用规则沿配置契约。

每个收据 hash 指向不可变对象，包含 operation/target set、process/host、revision/global hash、目标 projection/hash/policy、slot、actual instance/boot/generation、事件序号、发行者身份和 journal 提交证明。对象 hash 不认证发行者；Controller 与物化服务必须核验可信对象服务或 journal 记录中的真实 issuer。收据定位 URI 在被哈希 payload 之外，不能把含自身 SHA-256 的 receipt_ref 递归放入自身内容。不同目标的 projection_hash 可以不同，成功要求每个目标匹配它在计划中的预期投影，而非所有进程拿到同一投影。

CONFIG_PREPARE 请求 status=Pending，receipt 字段为零；物化成功响应 status=Prepared、journal_receipt_hash 非零，runtime/file/completion receipt 为零。随后 CONFIG_FILE_DURABLE(file_kind=Desired,status=DesiredDurable) 报告 desired 文件与 journal 收据，applied_event_seq=0，不能声称内存已切换。CONFIG_APPLY 引用该 desired 文件与 Prepared journal 证明；目标验证投影并持久保存自己的 ConfigApply 后，才产生 CONFIG_RUNTIME_APPLIED(status=RuntimeApplied)，applied_event_seq>0，runtime_receipt_hash/journal_receipt_hash 非零，file_kind=None、file_receipt_hash=0。

物化服务核验 runtime 原始收据，再按配置契约的文件与目录同步顺序写 effective，持久记录 EffectiveMaterialized。它随后发送 CONFIG_FILE_DURABLE(file_kind=Effective,status=EffectiveDurable)，包含原 runtime receipt、自己的 file/journal receipt 和相同 applied_event_seq。运行时已应用但文件失败时返回 Stalled，保留新内存事实并按操作政策保持风险闸门关闭，不能用旧文件或丢失 runtime receipt 换取成功。risk_gate_revision 是 exec 实际应用的版本；文件写完不会自动开放新增风险，重新放行仍需既有授权协议。

成功 ACK 只证明相应阶段的事实。CONFIG_STATUS 的普通目标状态可以由该目标的认证 owner 返回；其中 runtime 只能报告自己的 Pending/Quiesced/RuntimeApplied/Stalled/Failed，物化服务只能报告自己有证据的准备/文件状态。只有 Controller 在核验完整 target_set_hash 下全部实际 runtime、effective-file、journal 和滚动接管收据后，持久写入 OperationSucceeded，才能发 CONFIG_STATUS(status=Succeeded,actor_kind=Controller,GlobalOperation=1)。这个全操作响应的 process/host 长度、运行身份、投影字段和逐目标 receipt 字段为零，completion_receipt_hash 非零，指向包含完整逐目标证明集合和完成提交水位的证书；不能按可见子集或在线目标集合计算成功。

CONFIG_STATUS 查询使用 Request、config_status=Query，至少绑定 operation 与可选的单一稳定 target；读取本身不改变状态。已认证且可解析的配置业务拒绝用 CONFIG_STATUS、Response\|Error，envelope.status 与 body.status_reason 使用同一错误码，config_status 为 Failed 或 Stalled；一般格式、feature、角色验证失败仍可用 ERROR/StatusBody，避免向无权主体返回配置明细。普通阶段成功的 status_reason=0。配置更新超时、收到消息、投影下载成功或 runtime ACK 都不构成 HTTP `Succeeded`。

## 6. 默认部署尺寸与容量边界

默认单机 64 物理核、256 GiB；2 个行情 product/shard topic、2 个 exec 风险域，最多 16 Active +16 Shadow 实例。单 topic 最多 40 个注册消费者（含策略、exec、relay 与预留），每个 subscriber/bank 都有独立 Cursor。行情资格输入为总计 100000 个完整事件/秒、每 topic 50000，允许 3 倍速率持续 100 ms；合成意图总量 1000/秒。这些是待验收的负载与容量配置，不能由容量算式替代实测。

| 资源 | 单份字节数 | 最大数量 | 合计字节 |
|---|---:|---:|---:|
| MarketBank | 301,998,080 | 8 | 2,415,984,640 |
| Market Cursor | 8,192 | 320 | 2,621,440 |
| Intent/Report bank | 4,202,496 | 256 | 1,075,838,976 |
| Intent/Report Cursor | 8,192 | 256 | 2,097,152 |

所有 bank 与 cursor 合计 **3,496,542,208 字节**（约 3.256409 GiB）。MarketBank 每份 65536×512 字节槽位、256 MiB arena 与 8192 字节元数据，共 301998080 字节；每 topic 4 bank。Intent 4096×1024，Report 8192×512，每份都是 4202496 字节。64 个策略/域对均计入两个方向、每方向两 bank 及其独立游标，不假定 Shadow 共用 Active 的游标。

| Arena class | block 字节（含128字节头） | 数量 | 有效 payload 上限 | bank 内绝对起点 |
|---|---:|---:|---:|---:|
| 1 | 1024 | 32768 | 896 | 33562624 |
| 2 | 4096 | 16384 | 3968 | 67117056 |
| 3 | 16384 | 4096 | 16256 | 134225920 |
| 4 | 131072 | 768 | 65536 | 201334784 |

四 bank 轮换用于 MarketBroadcast；单生产者单消费者（Single Producer Single Consumer，SPSC）通道每方向预分配两 bank，分别为 Active 与 Replacement/Retiring，各自有独立 Cursor。新旧策略发布并存另受最多 32 实例限制。

SPSC 换代先撤销旧 channel 的新接纳权，取得唯一写者 Freeze ACK 或实际 writer 退出证明，记录旧 published 末序号与最后 intent_seq/report_seq，再激活绑定新 channel_epoch 的 replacement。旧 holders 可继续安全读取旧 bank；只有它们全部释放/退出才回收该 bank。旧传输序号不延续到新 channel，新传输序号从 1 开始，业务 intent_seq/report_seq 按去重与回放契约衔接，未解决意图按原键查询，回报缺口按快照/回放补齐。若 writer 未安全冻结、两 bank 都被合法 holders 占据，或恢复覆盖无法建立，保持 Frozen。日常 strategy 重启与 ABI major 迁移均遵守此预算和身份边界，不增设第三 bank。

4 个 bank 是这套配置的资源预算：1 Active、1 Preparing、至多 2 Retiring；既有 Retiring 不可回收时不无限分配。每个 md/exec owner 另预留最多 8 个 64 MiB 快照，4 个 owner 合计 2 GiB；这是总快照资源预算，包含目录和正在构建的对象，不给每种对象各加一份。bank/cursor 加快照共 5644025856 字节（约 5.2564 GiB），纳入单机 8 GiB SHM 总上限；ConfigProjection 另限每主机 128 个存活对象，准备、当前与退休对象共同计数，最坏 128×(4096+4194304)=537395200 字节；bank/cursor、快照与投影合计 6181421056 字节。再计入 256 MiB 中继预算，总提交 SHM 为 6449856512 字节，距 8 GiB 上限剩余 2140078080 字节；其他有界资源仍须计入同一上限，不能视为可无限增设 bank 或保留旧投影的许可。策略本地书、输入日志/WAL、TLS、控制缓存和线程栈由部署清单另计；不能把这里的 SHM 数字当整机常驻内存。

record 高水位为 49152，停止领取普通新记录的水位为 57344；到达高水位立即启动慢读者处置/换 bank，剩余容量为准备与完整事件封口保留。每次批次至多 64 条，在领取前必须同时检查 record 差值、每 class 空闲块和有效 payload 总长≤4 MiB。最大 class 的 64 块占 8 MiB 实际 arena（包含零尾部），不能只按有效 payload 计费。

任一 arena class 使用达到 75% 或不足再容纳一个 64 条最坏批次时触发准备；保留空闲字节分别至少 65536/262144/1048576/8388608。这些是各 class 的块字节，不是可跨 class 借用的“总空闲”。默认事件速率配置还必须附实际 payload class 分布；持续大深度流可能先耗尽 class 4。发布前拿不到完整所需资源则不发布半条事件，达到保护边界即使有其余小块也停止该源并 Invalid。四个 bank 全被 holders 占据、计划 cutoff 无法准备完成或无法满足重建覆盖时，同样停止相关新增风险；exec 继续撤单和处理私有事实。

## 7. 精确字段表

下面是机器定义的完整字段投影。`bytes[N]` 是固定字节数组；嵌套布局的长度等于其 size×count。字段注释中的“原子”只对该字段生效。保留区在编码前清零、解码时校验零；任何实现语言都必须采用这些偏移，不能依赖本地结构体的默认 padding。

### ArenaClass：64 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 2 | `u16` | `class_id` |
| 2 | 2 | `bytes[2]` | `reserved_0（保留零）` |
| 4 | 4 | `u32` | `block_bytes` |
| 8 | 4 | `u32` | `block_count` |
| 12 | 4 | `u32` | `payload_limit` |
| 16 | 8 | `u64` | `offset` |
| 24 | 8 | `u64` | `length` |
| 32 | 32 | `bytes[32]` | `reserved_1（保留零）` |

### ResourceHeader：4096 字节，对齐 4096

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `bytes[8]` | `magic` |
| 8 | 2 | `u16` | `abi_major` |
| 10 | 2 | `u16` | `abi_minor` |
| 12 | 4 | `u32` | `header_bytes` |
| 16 | 2 | `u16` | `resource_kind` |
| 18 | 2 | `u16` | `target_arch` |
| 20 | 1 | `u8` | `endian` |
| 21 | 1 | `u8` | `atomic_width` |
| 22 | 1 | `u8` | `page_shift` |
| 23 | 1 | `u8` | `isolation_shift` |
| 24 | 4 | `u32` | `record_bytes` |
| 28 | 4 | `u32` | `capacity` |
| 32 | 8 | `u64` | `resource_bytes` |
| 40 | 16 | `bytes[16]` | `resource_id` |
| 56 | 16 | `bytes[16]` | `channel_id` |
| 72 | 16 | `bytes[16]` | `producer_instance` |
| 88 | 16 | `bytes[16]` | `consumer_instance` |
| 104 | 16 | `bytes[16]` | `host_boot_id` |
| 120 | 16 | `bytes[16]` | `namespace_id` |
| 136 | 8 | `u64` | `channel_epoch` |
| 144 | 8 | `u64` | `ring_epoch` |
| 152 | 8 | `u64` | `feed_generation` |
| 160 | 8 | `u64` | `producer_generation` |
| 168 | 8 | `u64` | `topic_id` |
| 176 | 8 | `u64` | `risk_scope` |
| 184 | 8 | `u64` | `catalog_version` |
| 192 | 8 | `u64` | `created_mono_ns` |
| 200 | 8 | `u64` | `first_seq` |
| 208 | 8 | `u64` | `predecessor_ring_epoch` |
| 216 | 8 | `u64` | `cutover_seq` |
| 224 | 8 | `u64` | `initial_membership_version` |
| 232 | 8 | `u64` | `slot_offset` |
| 240 | 8 | `u64` | `arena_offset` |
| 248 | 8 | `u64` | `arena_bytes` |
| 256 | 8 | `u64` | `producer_page_offset` |
| 264 | 8 | `u64` | `cursor_page_offset` |
| 272 | 32 | `bytes[32]` | `descriptor_hash` |
| 304 | 8 | `u64` | `required_features` |
| 312 | 8 | `u64` | `optional_features` |
| 320 | 4 | `u32` | `max_payload` |
| 324 | 4 | `u32` | `max_consumers` |
| 328 | 2 | `u16` | `qos` |
| 330 | 2 | `u16` | `schema_major` |
| 332 | 2 | `u16` | `schema_minor` |
| 334 | 2 | `u16` | `venue` |
| 336 | 2 | `u16` | `product` |
| 338 | 2 | `u16` | `event_class` |
| 340 | 4 | `u32` | `feed_shard` |
| 344 | 16 | `bytes[16]` | `parent_resource_id` |
| 360 | 2 | `u16` | `arena_class_count` |
| 362 | 2 | `u16` | `trust_profile` |
| 364 | 4 | `u32` | `clock_domain` |
| 368 | 144 | `bytes[144]` | `reserved_0（保留零）` |
| 512 | 256 | `ArenaClass[4]` | `class_descriptors` |
| 768 | 3328 | `bytes[3328]` | `reserved_1（保留零）` |

### ProducerPage：4096 字节，对齐 4096

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `atomic_u64` | `published（原子）` |
| 8 | 120 | `bytes[120]` | `reserved_0（保留零）` |
| 128 | 8 | `atomic_u64` | `lifecycle（原子）` |
| 136 | 120 | `bytes[120]` | `reserved_1（保留零）` |
| 256 | 8 | `atomic_u64` | `frozen_seq（原子）` |
| 264 | 120 | `bytes[120]` | `reserved_2（保留零）` |
| 384 | 8 | `atomic_u64` | `membership_version（原子）` |
| 392 | 120 | `bytes[120]` | `reserved_3（保留零）` |
| 512 | 8 | `atomic_u64` | `fault_code（原子）` |
| 520 | 120 | `bytes[120]` | `reserved_4（保留零）` |
| 640 | 8 | `atomic_u64` | `heartbeat_mono_ns（原子）` |
| 648 | 120 | `bytes[120]` | `reserved_5（保留零）` |
| 768 | 8 | `atomic_u64` | `successor_ring_epoch（原子）` |
| 776 | 120 | `bytes[120]` | `reserved_6（保留零）` |
| 896 | 8 | `atomic_u64` | `freeze_membership_version（原子）` |
| 904 | 3192 | `bytes[3192]` | `reserved_7（保留零）` |

### CursorPage：4096 字节，对齐 4096

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `atomic_u64` | `read_seq（原子）` |
| 8 | 120 | `bytes[120]` | `reserved_0（保留零）` |
| 128 | 8 | `atomic_u64` | `applied_seq（原子）` |
| 136 | 120 | `bytes[120]` | `reserved_1（保留零）` |
| 256 | 8 | `atomic_u64` | `released_seq（原子）` |
| 264 | 120 | `bytes[120]` | `reserved_2（保留零）` |
| 384 | 8 | `atomic_u64` | `lifecycle（原子）` |
| 392 | 120 | `bytes[120]` | `reserved_3（保留零）` |
| 512 | 8 | `atomic_u64` | `outstanding_borrows（原子）` |
| 520 | 120 | `bytes[120]` | `reserved_4（保留零）` |
| 640 | 8 | `atomic_u64` | `heartbeat_mono_ns（原子）` |
| 648 | 120 | `bytes[120]` | `reserved_5（保留零）` |
| 768 | 8 | `atomic_u64` | `observed_membership_version（原子）` |
| 776 | 120 | `bytes[120]` | `reserved_6（保留零）` |
| 896 | 8 | `atomic_u64` | `quiesced_seq（原子）` |
| 904 | 3192 | `bytes[3192]` | `reserved_7（保留零）` |

### RecordHeader：128 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `u64` | `sequence` |
| 8 | 2 | `u16` | `message_type` |
| 10 | 2 | `u16` | `schema_major` |
| 12 | 2 | `u16` | `schema_minor` |
| 14 | 2 | `u16` | `header_bytes` |
| 16 | 4 | `u32` | `flags` |
| 20 | 2 | `u16` | `payload_kind` |
| 22 | 2 | `u16` | `body_bytes` |
| 24 | 8 | `u64` | `payload_offset` |
| 32 | 4 | `u32` | `payload_length` |
| 36 | 2 | `u16` | `arena_class` |
| 38 | 2 | `bytes[2]` | `reserved_0（保留零）` |
| 40 | 8 | `u64` | `allocation_seq` |
| 48 | 8 | `u64` | `topic_or_scope_id` |
| 56 | 8 | `u64` | `feed_generation` |
| 64 | 8 | `u64` | `producer_generation` |
| 72 | 8 | `u64` | `instrument_id` |
| 80 | 8 | `u64` | `instrument_version` |
| 88 | 8 | `u64` | `source_timestamp_ns` |
| 96 | 8 | `u64` | `ingress_mono_ns` |
| 104 | 8 | `u64` | `publish_mono_ns` |
| 112 | 4 | `u32` | `record_crc32c` |
| 116 | 4 | `u32` | `payload_crc32c` |
| 120 | 8 | `bytes[8]` | `reserved_1（保留零）` |

### ArenaBlockHeader：128 字节，对齐 128

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `u64` | `allocation_seq` |
| 8 | 4 | `u32` | `payload_length` |
| 12 | 2 | `u16` | `class_id` |
| 14 | 2 | `bytes[2]` | `reserved_0（保留零）` |
| 16 | 8 | `u64` | `ring_epoch` |
| 24 | 16 | `bytes[16]` | `resource_id` |
| 40 | 4 | `u32` | `payload_crc32c` |
| 44 | 84 | `bytes[84]` | `reserved_1（保留零）` |

### DepthLevel：16 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `i64` | `price_ticks` |
| 8 | 8 | `i64` | `qty_lots` |

### TriggerRef：64 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `u64` | `topic_id` |
| 8 | 8 | `u64` | `feed_generation` |
| 16 | 8 | `u64` | `ring_epoch` |
| 24 | 8 | `u64` | `sequence` |
| 32 | 8 | `u64` | `instrument_id` |
| 40 | 8 | `u64` | `instrument_version` |
| 48 | 8 | `u64` | `observed_mono_ns` |
| 56 | 2 | `u16` | `required_feed_state` |
| 58 | 2 | `u16` | `product` |
| 60 | 4 | `bytes[4]` | `reserved_0（保留零）` |

### MarketBody：384 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `u64` | `venue_seq_first` |
| 8 | 8 | `u64` | `venue_seq_last` |
| 16 | 8 | `u64` | `venue_seq_previous` |
| 24 | 8 | `u64` | `trade_id` |
| 32 | 8 | `u64` | `exchange_event_ns` |
| 40 | 8 | `u64` | `book_revision` |
| 48 | 8 | `i64` | `price_ticks` |
| 56 | 8 | `i64` | `qty_lots` |
| 64 | 8 | `i64` | `bid_price_ticks` |
| 72 | 8 | `i64` | `bid_qty_lots` |
| 80 | 8 | `i64` | `ask_price_ticks` |
| 88 | 8 | `i64` | `ask_qty_lots` |
| 96 | 8 | `i64` | `mark_price_ticks` |
| 104 | 8 | `i64` | `index_price_ticks` |
| 112 | 8 | `i64` | `funding_rate_e12` |
| 120 | 8 | `u64` | `next_funding_utc_ns` |
| 128 | 4 | `u32` | `level_count` |
| 132 | 4 | `u32` | `bid_count` |
| 136 | 4 | `u32` | `ask_count` |
| 140 | 2 | `u16` | `feed_state` |
| 142 | 2 | `u16` | `reason` |
| 144 | 1 | `u8` | `side` |
| 145 | 1 | `u8` | `trade_flags` |
| 146 | 2 | `u16` | `depth_flags` |
| 148 | 16 | `bytes[16]` | `snapshot_id` |
| 164 | 4 | `u32` | `source_schema_id` |
| 168 | 8 | `u64` | `snapshot_sequence` |
| 176 | 4 | `u32` | `coverage_depth` |
| 180 | 4 | `bytes[4]` | `reserved_0（保留零）` |
| 184 | 8 | `u64` | `instrument_catalog_version` |
| 192 | 192 | `DepthLevel[12]` | `inline_levels` |

### IntentBody：896 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `u64` | `logical_strategy_id` |
| 8 | 16 | `bytes[16]` | `instance_id` |
| 24 | 8 | `u64` | `strategy_generation` |
| 32 | 8 | `u64` | `exec_generation` |
| 40 | 8 | `u64` | `authorization_epoch` |
| 48 | 8 | `u64` | `config_version` |
| 56 | 8 | `u64` | `intent_seq` |
| 64 | 16 | `bytes[16]` | `economic_intent_id` |
| 80 | 8 | `u64` | `created_mono_ns` |
| 88 | 8 | `u64` | `valid_until_mono_ns` |
| 96 | 16 | `bytes[16]` | `order_id` |
| 112 | 8 | `u64` | `expected_owner_revision` |
| 120 | 8 | `i64` | `price_ticks` |
| 128 | 8 | `i64` | `qty_lots` |
| 136 | 8 | `i64` | `stop_price_ticks` |
| 144 | 8 | `i64` | `quote_amount_units` |
| 152 | 16 | `bytes[16]` | `hedge_group_id` |
| 168 | 4 | `u32` | `leg_id` |
| 172 | 2 | `u16` | `trigger_count` |
| 174 | 2 | `u16` | `command_kind` |
| 176 | 1 | `u8` | `side` |
| 177 | 1 | `u8` | `order_type` |
| 178 | 1 | `u8` | `time_in_force` |
| 179 | 1 | `u8` | `position_side` |
| 180 | 1 | `u8` | `reduce_only` |
| 181 | 1 | `u8` | `stp_mode` |
| 182 | 1 | `u8` | `working_price` |
| 183 | 1 | `u8` | `intent_flags` |
| 184 | 8 | `u64` | `expire_utc_ns` |
| 192 | 16 | `bytes[16]` | `list_id` |
| 208 | 16 | `bytes[16]` | `parent_order_id` |
| 224 | 32 | `bytes[32]` | `recovery_manifest_hash` |
| 256 | 32 | `bytes[32]` | `catalog_hash` |
| 288 | 8 | `u64` | `risk_policy_version` |
| 296 | 8 | `u64` | `order_config_version` |
| 304 | 80 | `bytes[80]` | `reserved_0（保留零）` |
| 384 | 512 | `TriggerRef[8]` | `triggers` |

### ReportBody：384 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `u64` | `logical_strategy_id` |
| 8 | 16 | `bytes[16]` | `instance_id` |
| 24 | 8 | `u64` | `strategy_generation` |
| 32 | 8 | `u64` | `exec_generation` |
| 40 | 8 | `u64` | `authorization_epoch` |
| 48 | 8 | `u64` | `intent_seq` |
| 56 | 16 | `bytes[16]` | `economic_intent_id` |
| 72 | 16 | `bytes[16]` | `order_id` |
| 88 | 64 | `bytes[64]` | `venue_order_id` |
| 152 | 32 | `bytes[32]` | `venue_trade_id` |
| 184 | 8 | `u64` | `exec_journal_seq` |
| 192 | 8 | `u64` | `report_seq` |
| 200 | 8 | `u64` | `owner_revision` |
| 208 | 8 | `i64` | `quantity` |
| 216 | 8 | `i64` | `cumulative_qty` |
| 224 | 8 | `i64` | `leaves_qty` |
| 232 | 8 | `i64` | `price_ticks` |
| 240 | 8 | `i64` | `last_qty` |
| 248 | 8 | `i64` | `last_price_ticks` |
| 256 | 8 | `i64` | `fee_units` |
| 264 | 8 | `u64` | `fee_asset_id` |
| 272 | 8 | `u64` | `asset_version` |
| 280 | 8 | `u64` | `event_mono_ns` |
| 288 | 16 | `bytes[16]` | `reservation_id` |
| 304 | 2 | `u16` | `status` |
| 306 | 1 | `u8` | `send_state` |
| 307 | 1 | `u8` | `cancel_state` |
| 308 | 1 | `u8` | `side` |
| 309 | 1 | `u8` | `position_side` |
| 310 | 2 | `u16` | `reason` |
| 312 | 2 | `u16` | `venue_order_id_length` |
| 314 | 2 | `u16` | `venue_trade_id_length` |
| 316 | 1 | `i8` | `asset_exponent` |
| 317 | 1 | `u8` | `report_flags` |
| 318 | 2 | `u16` | `report_kind` |
| 320 | 16 | `bytes[16]` | `aux_id` |
| 336 | 8 | `u64` | `aux_version` |
| 344 | 8 | `i64` | `value_0` |
| 352 | 8 | `i64` | `value_1` |
| 360 | 8 | `i64` | `value_2` |
| 368 | 8 | `u64` | `value_validity` |
| 376 | 8 | `bytes[8]` | `reserved_0（保留零）` |

### OwnershipBody：384 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `u64` | `logical_strategy_id` |
| 8 | 16 | `bytes[16]` | `instance_id` |
| 24 | 8 | `u64` | `strategy_generation` |
| 32 | 8 | `u64` | `exec_generation` |
| 40 | 8 | `u64` | `exec_journal_seq` |
| 48 | 8 | `u64` | `report_seq` |
| 56 | 16 | `bytes[16]` | `order_id` |
| 72 | 8 | `u64` | `origin_strategy_id` |
| 80 | 16 | `bytes[16]` | `origin_instance_id` |
| 96 | 8 | `u64` | `origin_generation` |
| 104 | 8 | `u64` | `origin_intent_seq` |
| 112 | 8 | `u64` | `manager_strategy_id` |
| 120 | 16 | `bytes[16]` | `manager_instance_id` |
| 136 | 8 | `u64` | `manager_generation` |
| 144 | 8 | `u64` | `owner_revision` |
| 152 | 16 | `bytes[16]` | `parent_order_id` |
| 168 | 16 | `bytes[16]` | `list_id` |
| 184 | 64 | `bytes[64]` | `venue_algo_id` |
| 248 | 2 | `u16` | `venue_algo_id_length` |
| 250 | 2 | `u16` | `relation_kind` |
| 252 | 2 | `u16` | `status` |
| 254 | 2 | `u16` | `reason` |
| 256 | 128 | `bytes[128]` | `reserved_0（保留零）` |

### OwnershipRecord：512 字节，对齐 128

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 128 | `RecordHeader` | `header` |
| 128 | 384 | `OwnershipBody` | `body` |

### MarketRecord：512 字节，对齐 128

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 128 | `RecordHeader` | `header` |
| 128 | 384 | `MarketBody` | `body` |

### IntentRecord：1024 字节，对齐 128

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 128 | `RecordHeader` | `header` |
| 128 | 896 | `IntentBody` | `body` |

### ReportRecord：512 字节，对齐 128

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 128 | `RecordHeader` | `header` |
| 128 | 384 | `ReportBody` | `body` |

### SnapshotHeader：256 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `bytes[8]` | `magic` |
| 8 | 2 | `u16` | `snapshot_kind` |
| 10 | 2 | `u16` | `schema_major` |
| 12 | 2 | `u16` | `schema_minor` |
| 14 | 2 | `u16` | `entry_bytes` |
| 16 | 4 | `u32` | `entry_count` |
| 20 | 4 | `u32` | `entry_offset` |
| 24 | 16 | `bytes[16]` | `snapshot_id` |
| 40 | 16 | `bytes[16]` | `channel_id` |
| 56 | 8 | `u64` | `feed_generation` |
| 64 | 8 | `u64` | `exec_generation` |
| 72 | 8 | `u64` | `sequence` |
| 80 | 8 | `u64` | `instrument_id` |
| 88 | 8 | `u64` | `instrument_version` |
| 96 | 32 | `bytes[32]` | `catalog_hash` |
| 128 | 32 | `bytes[32]` | `entries_hash` |
| 160 | 4 | `u32` | `bid_count` |
| 164 | 4 | `u32` | `ask_count` |
| 168 | 4 | `u32` | `coverage_depth` |
| 172 | 4 | `u32` | `coverage_flags` |
| 176 | 8 | `u64` | `created_mono_ns` |
| 184 | 8 | `u64` | `expires_mono_ns` |
| 192 | 8 | `u64` | `logical_strategy_id` |
| 200 | 8 | `u64` | `state_version` |
| 208 | 8 | `u64` | `object_bytes` |
| 216 | 32 | `bytes[32]` | `topic_instrument_set_hash` |
| 248 | 8 | `u64` | `manifest_member_bytes` |

### SnapshotSection：64 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 2 | `u16` | `section_kind` |
| 2 | 2 | `u16` | `entry_bytes` |
| 4 | 4 | `u32` | `entry_count` |
| 8 | 8 | `u64` | `offset` |
| 16 | 8 | `u64` | `length` |
| 24 | 32 | `bytes[32]` | `entries_hash` |
| 56 | 8 | `bytes[8]` | `reserved_0（保留零）` |

### TopicSnapshotEntry：128 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `u64` | `instrument_id` |
| 8 | 8 | `u64` | `instrument_version` |
| 16 | 16 | `bytes[16]` | `snapshot_id` |
| 32 | 32 | `bytes[32]` | `snapshot_hash` |
| 64 | 8 | `u64` | `snapshot_bytes` |
| 72 | 8 | `u64` | `snapshot_seq` |
| 80 | 8 | `u64` | `feed_generation` |
| 88 | 8 | `u64` | `catalog_version` |
| 96 | 2 | `u16` | `snapshot_kind` |
| 98 | 2 | `u16` | `feed_state` |
| 100 | 4 | `u32` | `entry_flags` |
| 104 | 8 | `u64` | `last_instrument_event_seq` |
| 112 | 8 | `u64` | `snapshot_offset` |
| 120 | 8 | `bytes[8]` | `reserved_0（保留零）` |

### InstrumentEntry：256 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `u64` | `instrument_id` |
| 8 | 8 | `u64` | `instrument_version` |
| 16 | 8 | `u64` | `catalog_version` |
| 24 | 2 | `u16` | `venue` |
| 26 | 2 | `u16` | `product` |
| 28 | 1 | `i8` | `price_exponent` |
| 29 | 1 | `i8` | `qty_exponent` |
| 30 | 1 | `i8` | `quote_exponent` |
| 31 | 1 | `u8` | `flags` |
| 32 | 8 | `u64` | `price_step_mantissa` |
| 40 | 8 | `u64` | `qty_step_mantissa` |
| 48 | 8 | `u64` | `quote_step_mantissa` |
| 56 | 8 | `u64` | `contract_multiplier_mantissa` |
| 64 | 1 | `i8` | `contract_multiplier_exponent` |
| 65 | 1 | `u8` | `symbol_length` |
| 66 | 1 | `u8` | `position_modes` |
| 67 | 1 | `u8` | `rounding_policy` |
| 68 | 32 | `bytes[32]` | `symbol` |
| 100 | 4 | `bytes[4]` | `reserved_0（保留零）` |
| 104 | 8 | `u64` | `base_asset_id` |
| 112 | 8 | `u64` | `quote_asset_id` |
| 120 | 8 | `u64` | `settle_asset_id` |
| 128 | 8 | `i64` | `min_price_ticks` |
| 136 | 8 | `i64` | `max_price_ticks` |
| 144 | 8 | `i64` | `min_qty_lots` |
| 152 | 8 | `i64` | `max_qty_lots` |
| 160 | 8 | `i64` | `min_notional_units` |
| 168 | 8 | `i64` | `max_notional_units` |
| 176 | 8 | `u64` | `filter_version` |
| 184 | 8 | `u64` | `effective_from_mono_ns` |
| 192 | 32 | `bytes[32]` | `filter_hash` |
| 224 | 32 | `bytes[32]` | `reserved_1（保留零）` |

### ControlEnvelope：128 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `bytes[8]` | `magic` |
| 8 | 2 | `u16` | `abi_major` |
| 10 | 2 | `u16` | `abi_minor` |
| 12 | 2 | `u16` | `header_bytes` |
| 14 | 2 | `u16` | `opcode` |
| 16 | 4 | `u32` | `frame_bytes` |
| 20 | 4 | `u32` | `flags` |
| 24 | 16 | `bytes[16]` | `request_id` |
| 40 | 8 | `u64` | `control_epoch` |
| 48 | 16 | `bytes[16]` | `sender_instance` |
| 64 | 16 | `bytes[16]` | `target_instance` |
| 80 | 16 | `bytes[16]` | `operation_id` |
| 96 | 8 | `u64` | `operation_revision` |
| 104 | 8 | `u64` | `expected_state_version` |
| 112 | 4 | `u32` | `body_crc32c` |
| 116 | 4 | `u32` | `header_crc32c` |
| 120 | 2 | `u16` | `status` |
| 122 | 2 | `u16` | `fd_count` |
| 124 | 4 | `bytes[4]` | `reserved_0（保留零）` |

### HelloBody：192 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 2 | `u16` | `min_major` |
| 2 | 2 | `u16` | `max_major` |
| 4 | 2 | `u16` | `min_minor` |
| 6 | 2 | `u16` | `max_minor` |
| 8 | 2 | `u16` | `role` |
| 10 | 2 | `u16` | `target_arch` |
| 12 | 1 | `u8` | `page_shift` |
| 13 | 1 | `u8` | `isolation_shift` |
| 14 | 1 | `u8` | `atomic_width` |
| 15 | 1 | `u8` | `trust_profile` |
| 16 | 8 | `u64` | `required_features` |
| 24 | 8 | `u64` | `optional_features` |
| 32 | 16 | `bytes[16]` | `host_boot_id` |
| 48 | 16 | `bytes[16]` | `namespace_id` |
| 64 | 8 | `u64` | `logical_strategy_id` |
| 72 | 8 | `u64` | `instance_generation` |
| 80 | 32 | `bytes[32]` | `build_hash` |
| 112 | 32 | `bytes[32]` | `sdk_qualification_hash` |
| 144 | 32 | `bytes[32]` | `manifest_hash` |
| 176 | 4 | `u32` | `max_frame_bytes` |
| 180 | 12 | `bytes[12]` | `reserved_0（保留零）` |

### WelcomeBody：128 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 2 | `u16` | `selected_major` |
| 2 | 2 | `u16` | `selected_minor` |
| 4 | 2 | `u16` | `role` |
| 6 | 2 | `u16` | `trust_profile` |
| 8 | 8 | `u64` | `features` |
| 16 | 8 | `u64` | `assigned_control_epoch` |
| 24 | 16 | `bytes[16]` | `server_instance` |
| 40 | 16 | `bytes[16]` | `host_boot_id` |
| 56 | 8 | `u64` | `lease_deadline_mono_ns` |
| 64 | 8 | `u64` | `lease_revision` |
| 72 | 4 | `u32` | `max_frame_bytes` |
| 76 | 2 | `u16` | `max_fds` |
| 78 | 2 | `u16` | `max_consumers` |
| 80 | 32 | `bytes[32]` | `session_nonce` |
| 112 | 16 | `bytes[16]` | `reserved_0（保留零）` |

### ResourceDescriptor：128 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `resource_id` |
| 16 | 16 | `bytes[16]` | `channel_id` |
| 32 | 16 | `bytes[16]` | `parent_resource_id` |
| 48 | 8 | `u64` | `resource_bytes` |
| 56 | 8 | `u64` | `channel_epoch` |
| 64 | 8 | `u64` | `ring_epoch` |
| 72 | 2 | `u16` | `resource_kind` |
| 74 | 2 | `u16` | `access` |
| 76 | 2 | `u16` | `fd_index` |
| 78 | 2 | `u16` | `seal_profile` |
| 80 | 32 | `bytes[32]` | `header_hash` |
| 112 | 16 | `bytes[16]` | `writer_instance` |

### OfferPrefix：64 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `offer_id` |
| 16 | 2 | `u16` | `descriptor_count` |
| 18 | 2 | `u16` | `descriptor_bytes` |
| 20 | 4 | `u32` | `required_seals` |
| 24 | 8 | `u64` | `expires_mono_ns` |
| 32 | 8 | `u64` | `membership_version` |
| 40 | 24 | `bytes[24]` | `reserved_0（保留零）` |

### ResourceAckBody：96 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `offer_id` |
| 16 | 16 | `bytes[16]` | `resource_id` |
| 32 | 32 | `bytes[32]` | `header_hash` |
| 64 | 8 | `u64` | `mapped_bytes` |
| 72 | 4 | `u32` | `observed_seals` |
| 76 | 2 | `u16` | `access` |
| 78 | 2 | `u16` | `ack_kind` |
| 80 | 8 | `u64` | `registration_generation` |
| 88 | 8 | `bytes[8]` | `reserved_0（保留零）` |

### JoinBody：256 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `subscriber_instance` |
| 16 | 8 | `u64` | `logical_strategy_id` |
| 24 | 8 | `u64` | `subscription_generation` |
| 32 | 8 | `u64` | `topic_id` |
| 40 | 16 | `bytes[16]` | `channel_id` |
| 56 | 8 | `u64` | `channel_epoch` |
| 64 | 8 | `u64` | `feed_generation` |
| 72 | 8 | `u64` | `ring_epoch` |
| 80 | 8 | `u64` | `join_seq` |
| 88 | 16 | `bytes[16]` | `snapshot_id` |
| 104 | 8 | `u64` | `snapshot_seq` |
| 112 | 8 | `u64` | `membership_version` |
| 120 | 16 | `bytes[16]` | `cursor_resource_id` |
| 136 | 16 | `bytes[16]` | `bank_resource_id` |
| 152 | 32 | `bytes[32]` | `snapshot_hash` |
| 184 | 8 | `u64` | `readiness_deadline_mono_ns` |
| 192 | 8 | `u64` | `expected_catalog_version` |
| 200 | 2 | `u16` | `qos` |
| 202 | 2 | `u16` | `role` |
| 204 | 4 | `u32` | `join_flags` |
| 208 | 8 | `u64` | `topic_instrument_set_version` |
| 216 | 32 | `bytes[32]` | `topic_instrument_set_hash` |
| 248 | 8 | `bytes[8]` | `reserved_0（保留零）` |

### FreezeBody：128 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `freeze_id` |
| 16 | 16 | `bytes[16]` | `old_resource_id` |
| 32 | 16 | `bytes[16]` | `new_resource_id` |
| 48 | 8 | `u64` | `channel_epoch` |
| 56 | 8 | `u64` | `feed_generation` |
| 64 | 8 | `u64` | `old_ring_epoch` |
| 72 | 8 | `u64` | `new_ring_epoch` |
| 80 | 8 | `u64` | `cutover_seq` |
| 88 | 8 | `u64` | `membership_version` |
| 96 | 8 | `u64` | `new_first_seq` |
| 104 | 2 | `u16` | `reason` |
| 106 | 22 | `bytes[22]` | `reserved_0（保留零）` |

### SwitchBody：192 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `freeze_id` |
| 16 | 16 | `bytes[16]` | `subscriber_instance` |
| 32 | 8 | `u64` | `subscription_generation` |
| 40 | 8 | `u64` | `old_ring_epoch` |
| 48 | 8 | `u64` | `new_ring_epoch` |
| 56 | 8 | `u64` | `old_released_seq` |
| 64 | 8 | `u64` | `old_applied_seq` |
| 72 | 8 | `u64` | `new_first_seq` |
| 80 | 8 | `u64` | `membership_version` |
| 88 | 16 | `bytes[16]` | `old_cursor_resource_id` |
| 104 | 16 | `bytes[16]` | `new_cursor_resource_id` |
| 120 | 16 | `bytes[16]` | `new_bank_resource_id` |
| 136 | 16 | `bytes[16]` | `snapshot_id` |
| 152 | 8 | `u64` | `snapshot_seq` |
| 160 | 2 | `u16` | `switch_kind` |
| 162 | 30 | `bytes[30]` | `reserved_0（保留零）` |

### QuiesceBody：128 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `subscriber_instance` |
| 16 | 16 | `bytes[16]` | `bank_resource_id` |
| 32 | 16 | `bytes[16]` | `cursor_resource_id` |
| 48 | 8 | `u64` | `subscription_generation` |
| 56 | 8 | `u64` | `channel_epoch` |
| 64 | 8 | `u64` | `ring_epoch` |
| 72 | 8 | `u64` | `released_seq` |
| 80 | 8 | `u64` | `applied_seq` |
| 88 | 8 | `u64` | `membership_version` |
| 96 | 2 | `u16` | `detach_reason` |
| 98 | 2 | `u16` | `evidence_kind` |
| 100 | 16 | `bytes[16]` | `exit_evidence_id` |
| 116 | 12 | `bytes[12]` | `reserved_0（保留零）` |

### GrantBody：384 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 8 | `u64` | `logical_strategy_id` |
| 8 | 16 | `bytes[16]` | `instance_id` |
| 24 | 8 | `u64` | `strategy_generation` |
| 32 | 8 | `u64` | `exec_generation` |
| 40 | 8 | `u64` | `risk_scope` |
| 48 | 8 | `u64` | `authorization_epoch` |
| 56 | 8 | `u64` | `expected_risk_gate_revision` |
| 64 | 8 | `u64` | `owner_revision` |
| 72 | 8 | `u64` | `control_lease_deadline_mono_ns` |
| 80 | 8 | `u64` | `valid_from_mono_ns` |
| 88 | 8 | `u64` | `valid_until_mono_ns` |
| 96 | 8 | `u64` | `expected_exec_state_version` |
| 104 | 8 | `u64` | `permissions` |
| 112 | 8 | `u64` | `config_version` |
| 120 | 8 | `u64` | `risk_policy_version` |
| 128 | 32 | `bytes[32]` | `member_set_hash` |
| 160 | 32 | `bytes[32]` | `recovery_manifest_hash` |
| 192 | 32 | `bytes[32]` | `handover_hash` |
| 224 | 32 | `bytes[32]` | `group_certificate_hash` |
| 256 | 32 | `bytes[32]` | `account_baseline_hash` |
| 288 | 32 | `bytes[32]` | `md_readiness_hash` |
| 320 | 32 | `bytes[32]` | `instrument_set_hash` |
| 352 | 2 | `u16` | `authority_kind` |
| 354 | 2 | `u16` | `grant_phase` |
| 356 | 28 | `bytes[28]` | `reserved_0（保留零）` |

### ExecLeaseBody：128 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `exec_instance` |
| 16 | 8 | `u64` | `exec_generation` |
| 24 | 8 | `u64` | `risk_scope` |
| 32 | 16 | `bytes[16]` | `host_boot_id` |
| 48 | 16 | `bytes[16]` | `challenge_id` |
| 64 | 8 | `u64` | `lease_revision` |
| 72 | 8 | `u64` | `issued_mono_ns` |
| 80 | 8 | `u64` | `deadline_mono_ns` |
| 88 | 8 | `u64` | `bound_control_epoch` |
| 96 | 4 | `u32` | `lease_ms` |
| 100 | 4 | `u32` | `renew_ms` |
| 104 | 24 | `bytes[24]` | `reserved_0（保留零）` |

### StatusBody：192 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `subject_id` |
| 16 | 16 | `bytes[16]` | `original_request_id` |
| 32 | 8 | `u64` | `original_control_epoch` |
| 40 | 8 | `u64` | `state_version` |
| 48 | 8 | `u64` | `applied_exec_seq` |
| 56 | 8 | `u64` | `exec_generation` |
| 64 | 8 | `u64` | `strategy_generation` |
| 72 | 8 | `u64` | `risk_gate_revision` |
| 80 | 8 | `u64` | `membership_version` |
| 88 | 8 | `u64` | `ring_epoch` |
| 96 | 8 | `u64` | `published_seq` |
| 104 | 8 | `u64` | `released_seq` |
| 112 | 2 | `u16` | `state` |
| 114 | 2 | `u16` | `reason` |
| 116 | 4 | `u32` | `retry_after_ms` |
| 120 | 32 | `bytes[32]` | `result_hash` |
| 152 | 2 | `u16` | `detail_length` |
| 154 | 30 | `bytes[30]` | `detail_utf8` |
| 184 | 8 | `bytes[8]` | `reserved_0（保留零）` |

### QueryBody：96 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `subject_id` |
| 16 | 16 | `bytes[16]` | `original_request_id` |
| 32 | 8 | `u64` | `original_control_epoch` |
| 40 | 2 | `u16` | `query_kind` |
| 42 | 6 | `bytes[6]` | `reserved_0（保留零）` |
| 48 | 8 | `u64` | `requested_state_version` |
| 56 | 8 | `u64` | `intent_seq` |
| 64 | 8 | `u64` | `strategy_generation` |
| 72 | 8 | `u64` | `logical_strategy_id` |
| 80 | 16 | `bytes[16]` | `reserved_1（保留零）` |

### ConfigControlBody：768 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `operation_id` |
| 16 | 8 | `u64` | `operation_revision` |
| 24 | 2 | `u16` | `process_id_length` |
| 26 | 2 | `u16` | `target_host_id_length` |
| 28 | 2 | `u16` | `apply_class` |
| 30 | 2 | `u16` | `config_status` |
| 32 | 96 | `bytes[96]` | `process_id` |
| 128 | 96 | `bytes[96]` | `target_host_id` |
| 224 | 16 | `bytes[16]` | `expected_instance_id` |
| 240 | 16 | `bytes[16]` | `expected_boot_id` |
| 256 | 8 | `u64` | `expected_exec_generation` |
| 264 | 8 | `u64` | `config_revision` |
| 272 | 8 | `u64` | `base_revision` |
| 280 | 8 | `u64` | `projection_policy_revision` |
| 288 | 8 | `u64` | `projection_object_bytes` |
| 296 | 8 | `u64` | `applied_event_seq` |
| 304 | 8 | `u64` | `risk_gate_revision` |
| 312 | 32 | `bytes[32]` | `successor_binding_hash` |
| 344 | 32 | `bytes[32]` | `global_config_hash` |
| 376 | 32 | `bytes[32]` | `projection_hash` |
| 408 | 32 | `bytes[32]` | `projection_object_hash` |
| 440 | 32 | `bytes[32]` | `control_commit_ref` |
| 472 | 32 | `bytes[32]` | `runtime_receipt_hash` |
| 504 | 32 | `bytes[32]` | `file_receipt_hash` |
| 536 | 32 | `bytes[32]` | `journal_receipt_hash` |
| 568 | 32 | `bytes[32]` | `target_set_hash` |
| 600 | 2 | `u16` | `actor_kind` |
| 602 | 2 | `u16` | `file_kind` |
| 604 | 2 | `u16` | `projection_transport` |
| 606 | 2 | `u16` | `status_reason` |
| 608 | 2 | `u16` | `projection_format` |
| 610 | 6 | `bytes[6]` | `reserved_0（保留零）` |
| 616 | 16 | `bytes[16]` | `issuer_boot_id` |
| 632 | 16 | `bytes[16]` | `instance_slot_id` |
| 648 | 8 | `u64` | `state_revision` |
| 656 | 16 | `bytes[16]` | `projection_resource_id` |
| 672 | 32 | `bytes[32]` | `completion_receipt_hash` |
| 704 | 32 | `bytes[32]` | `base_global_config_hash` |
| 736 | 8 | `u64` | `config_flags` |
| 744 | 16 | `bytes[16]` | `expected_host_boot_id` |
| 760 | 8 | `u64` | `expected_runtime_revision` |

### ResyncBody：128 字节，对齐 8

| 偏移 | 长度 | 类型 | 字段 |
|---:|---:|---|---|
| 0 | 16 | `bytes[16]` | `instance_id` |
| 16 | 8 | `u64` | `exec_generation` |
| 24 | 8 | `u64` | `last_applied_exec_seq` |
| 32 | 16 | `bytes[16]` | `snapshot_id` |
| 48 | 8 | `u64` | `snapshot_seq` |
| 56 | 32 | `bytes[32]` | `snapshot_hash` |
| 88 | 8 | `u64` | `channel_epoch` |
| 96 | 8 | `u64` | `subscription_generation` |
| 104 | 8 | `u64` | `recovery_manifest_version` |
| 112 | 16 | `bytes[16]` | `reserved_0（保留零）` |

## 8. 消息 ID、操作码、枚举与位

### 数据消息

| ID（十六进制） | 名称 | 通道 | 消息体 |
|---|---|---|---|
| `0x0101` | Quote | MarketBroadcast | MarketBody |
| `0x0102` | Trade | MarketBroadcast | MarketBody |
| `0x0103` | Depth | MarketBroadcast | MarketBody |
| `0x0104` | MarkFunding | MarketBroadcast | MarketBody |
| `0x0105` | FeedState | MarketBroadcast | MarketBody |
| `0x0106` | MarketSnapshotRef | MarketBroadcast | MarketBody |
| `0x0107` | CatalogRef | MarketBroadcast | MarketBody |
| `0x0201` | New | StrategyIntent | IntentBody |
| `0x0202` | Cancel | StrategyIntent | IntentBody |
| `0x0203` | CancelReplace | StrategyIntent | IntentBody |
| `0x0204` | AmendKeepPriority | StrategyIntent | IntentBody |
| `0x0205` | Modify | StrategyIntent | IntentBody |
| `0x0206` | QueryOrder | StrategyIntent | IntentBody |
| `0x0207` | CancelList | StrategyIntent | IntentBody |
| `0x0208` | CancelAlgo | StrategyIntent | IntentBody |
| `0x0301` | IntentStatus | ExecutionReport | ReportBody |
| `0x0302` | OrderState | ExecutionReport | ReportBody |
| `0x0303` | Fill | ExecutionReport | ReportBody |
| `0x0304` | Balance | ExecutionReport | ReportBody |
| `0x0305` | Position | ExecutionReport | ReportBody |
| `0x0306` | Reservation | ExecutionReport | ReportBody |
| `0x0307` | GrantState | ExecutionReport | ReportBody |
| `0x0308` | ResyncRequired | ExecutionReport | ReportBody |
| `0x0309` | LedgerAdjustment | ExecutionReport | ReportBody |
| `0x030a` | ExecutionSnapshotRef | ExecutionReport | ReportBody |
| `0x030b` | OrderOwnership | ExecutionReport | OwnershipBody |

### 消息体字段白名单与数值有效位

JSON 中每条消息的 allowed_fields 是完整白名单，未列字段必须零；下表直接投影该定义。掩码按十六进制列出，required 是已知数值的最低集合，不替代账户和行情准入。

| 消息 | 允许的消息体字段 | value_validity 允许 / 必需 |
|---|---|---|
| Quote | `bid_price_ticks`, `bid_qty_lots`, `ask_price_ticks`, `ask_qty_lots`, `book_revision`, `feed_state`, `exchange_event_ns`, `source_schema_id` | 不适用 |
| Trade | `trade_id`, `price_ticks`, `qty_lots`, `side`, `trade_flags`, `exchange_event_ns`, `feed_state`, `source_schema_id` | 不适用 |
| Depth | `venue_seq_first`, `venue_seq_last`, `venue_seq_previous`, `book_revision`, `level_count`, `bid_count`, `ask_count`, `depth_flags`, `feed_state`, `exchange_event_ns`, `source_schema_id`, `inline_levels` | 不适用 |
| MarkFunding | `mark_price_ticks`, `index_price_ticks`, `funding_rate_e12`, `next_funding_utc_ns`, `exchange_event_ns`, `feed_state`, `source_schema_id` | 不适用 |
| FeedState | `feed_state`, `reason`, `exchange_event_ns`, `source_schema_id` | 不适用 |
| MarketSnapshotRef | `snapshot_id`, `snapshot_sequence`, `coverage_depth`, `feed_state` | 不适用 |
| CatalogRef | `snapshot_id`, `snapshot_sequence`, `instrument_catalog_version` | 不适用 |
| New | `authorization_epoch`, `catalog_hash`, `command_kind`, `config_version`, `created_mono_ns`, `economic_intent_id`, `exec_generation`, `expire_utc_ns`, `hedge_group_id`, `instance_id`, `intent_flags`, `intent_seq`, `leg_id`, `logical_strategy_id`, `order_config_version`, `order_type`, `position_side`, `price_ticks`, `qty_lots`, `quote_amount_units`, `recovery_manifest_hash`, `reduce_only`, `risk_policy_version`, `side`, `stop_price_ticks`, `stp_mode`, `strategy_generation`, `time_in_force`, `trigger_count`, `triggers`, `valid_until_mono_ns`, `working_price` | 不适用 |
| Cancel | `authorization_epoch`, `catalog_hash`, `command_kind`, `config_version`, `created_mono_ns`, `economic_intent_id`, `exec_generation`, `expected_owner_revision`, `hedge_group_id`, `instance_id`, `intent_seq`, `leg_id`, `logical_strategy_id`, `order_config_version`, `order_id`, `recovery_manifest_hash`, `risk_policy_version`, `strategy_generation`, `valid_until_mono_ns` | 不适用 |
| CancelReplace | `authorization_epoch`, `catalog_hash`, `command_kind`, `config_version`, `created_mono_ns`, `economic_intent_id`, `exec_generation`, `expected_owner_revision`, `expire_utc_ns`, `hedge_group_id`, `instance_id`, `intent_flags`, `intent_seq`, `leg_id`, `logical_strategy_id`, `order_config_version`, `order_id`, `order_type`, `position_side`, `price_ticks`, `qty_lots`, `quote_amount_units`, `recovery_manifest_hash`, `reduce_only`, `risk_policy_version`, `side`, `stop_price_ticks`, `stp_mode`, `strategy_generation`, `time_in_force`, `trigger_count`, `triggers`, `valid_until_mono_ns`, `working_price` | 不适用 |
| AmendKeepPriority | `authorization_epoch`, `catalog_hash`, `command_kind`, `config_version`, `created_mono_ns`, `economic_intent_id`, `exec_generation`, `expected_owner_revision`, `hedge_group_id`, `instance_id`, `intent_seq`, `leg_id`, `logical_strategy_id`, `order_config_version`, `order_id`, `qty_lots`, `recovery_manifest_hash`, `risk_policy_version`, `strategy_generation`, `trigger_count`, `triggers`, `valid_until_mono_ns` | 不适用 |
| Modify | `authorization_epoch`, `catalog_hash`, `command_kind`, `config_version`, `created_mono_ns`, `economic_intent_id`, `exec_generation`, `expected_owner_revision`, `hedge_group_id`, `instance_id`, `intent_flags`, `intent_seq`, `leg_id`, `logical_strategy_id`, `order_config_version`, `order_id`, `position_side`, `price_ticks`, `qty_lots`, `recovery_manifest_hash`, `reduce_only`, `risk_policy_version`, `side`, `strategy_generation`, `trigger_count`, `triggers`, `valid_until_mono_ns` | 不适用 |
| QueryOrder | `authorization_epoch`, `catalog_hash`, `command_kind`, `config_version`, `created_mono_ns`, `economic_intent_id`, `exec_generation`, `expected_owner_revision`, `hedge_group_id`, `instance_id`, `intent_seq`, `leg_id`, `logical_strategy_id`, `order_config_version`, `order_id`, `recovery_manifest_hash`, `risk_policy_version`, `strategy_generation`, `valid_until_mono_ns` | 不适用 |
| CancelList | `authorization_epoch`, `catalog_hash`, `command_kind`, `config_version`, `created_mono_ns`, `economic_intent_id`, `exec_generation`, `expected_owner_revision`, `hedge_group_id`, `instance_id`, `intent_seq`, `leg_id`, `list_id`, `logical_strategy_id`, `order_config_version`, `order_id`, `recovery_manifest_hash`, `risk_policy_version`, `strategy_generation`, `valid_until_mono_ns` | 不适用 |
| CancelAlgo | `authorization_epoch`, `catalog_hash`, `command_kind`, `config_version`, `created_mono_ns`, `economic_intent_id`, `exec_generation`, `expected_owner_revision`, `hedge_group_id`, `instance_id`, `intent_seq`, `leg_id`, `logical_strategy_id`, `order_config_version`, `order_id`, `parent_order_id`, `recovery_manifest_hash`, `risk_policy_version`, `strategy_generation`, `valid_until_mono_ns` | 不适用 |
| IntentStatus | `authorization_epoch`, `economic_intent_id`, `event_mono_ns`, `exec_generation`, `exec_journal_seq`, `instance_id`, `intent_seq`, `logical_strategy_id`, `order_id`, `owner_revision`, `position_side`, `price_ticks`, `quantity`, `reason`, `report_flags`, `report_kind`, `report_seq`, `reservation_id`, `send_state`, `side`, `status`, `strategy_generation`, `value_validity` | 0x009 / 0x000 |
| OrderState | `authorization_epoch`, `cancel_state`, `cumulative_qty`, `economic_intent_id`, `event_mono_ns`, `exec_generation`, `exec_journal_seq`, `instance_id`, `intent_seq`, `leaves_qty`, `logical_strategy_id`, `order_id`, `owner_revision`, `position_side`, `price_ticks`, `quantity`, `reason`, `report_flags`, `report_kind`, `report_seq`, `reservation_id`, `send_state`, `side`, `status`, `strategy_generation`, `value_validity`, `venue_order_id`, `venue_order_id_length` | 0x00f / 0x000 |
| Fill | `asset_exponent`, `asset_version`, `authorization_epoch`, `cancel_state`, `cumulative_qty`, `economic_intent_id`, `event_mono_ns`, `exec_generation`, `exec_journal_seq`, `fee_asset_id`, `fee_units`, `instance_id`, `intent_seq`, `last_price_ticks`, `last_qty`, `leaves_qty`, `logical_strategy_id`, `order_id`, `owner_revision`, `position_side`, `price_ticks`, `quantity`, `reason`, `report_flags`, `report_kind`, `report_seq`, `reservation_id`, `send_state`, `side`, `status`, `strategy_generation`, `value_validity`, `venue_order_id`, `venue_order_id_length`, `venue_trade_id`, `venue_trade_id_length` | 0x07f / 0x030 |
| Balance | `asset_exponent`, `asset_version`, `authorization_epoch`, `aux_version`, `cumulative_qty`, `event_mono_ns`, `exec_generation`, `exec_journal_seq`, `fee_asset_id`, `instance_id`, `leaves_qty`, `logical_strategy_id`, `quantity`, `reason`, `report_flags`, `report_kind`, `report_seq`, `strategy_generation`, `value_0`, `value_validity` | 0x087 / 0x007 |
| Position | `asset_exponent`, `asset_version`, `authorization_epoch`, `aux_version`, `event_mono_ns`, `exec_generation`, `exec_journal_seq`, `fee_asset_id`, `instance_id`, `logical_strategy_id`, `position_side`, `price_ticks`, `quantity`, `reason`, `report_flags`, `report_kind`, `report_seq`, `strategy_generation`, `value_0`, `value_1`, `value_2`, `value_validity` | 0x389 / 0x001 |
| Reservation | `asset_exponent`, `asset_version`, `authorization_epoch`, `economic_intent_id`, `event_mono_ns`, `exec_generation`, `exec_journal_seq`, `fee_asset_id`, `instance_id`, `intent_seq`, `logical_strategy_id`, `order_id`, `owner_revision`, `quantity`, `reason`, `report_flags`, `report_kind`, `report_seq`, `reservation_id`, `status`, `strategy_generation`, `value_0`, `value_1`, `value_2`, `value_validity` | 0x381 / 0x181 |
| GrantState | `authorization_epoch`, `aux_version`, `event_mono_ns`, `exec_generation`, `exec_journal_seq`, `instance_id`, `logical_strategy_id`, `reason`, `report_flags`, `report_kind`, `report_seq`, `status`, `strategy_generation`, `value_0`, `value_1`, `value_validity` | 0x180 / 0x180 |
| ResyncRequired | `authorization_epoch`, `aux_version`, `event_mono_ns`, `exec_generation`, `exec_journal_seq`, `instance_id`, `logical_strategy_id`, `reason`, `report_flags`, `report_kind`, `report_seq`, `strategy_generation`, `value_validity` | 0x000 / 0x000 |
| LedgerAdjustment | `asset_exponent`, `asset_version`, `authorization_epoch`, `aux_id`, `aux_version`, `economic_intent_id`, `event_mono_ns`, `exec_generation`, `exec_journal_seq`, `fee_asset_id`, `fee_units`, `instance_id`, `intent_seq`, `logical_strategy_id`, `order_id`, `reason`, `report_flags`, `report_kind`, `report_seq`, `status`, `strategy_generation`, `value_validity` | 0x040 / 0x040 |
| ExecutionSnapshotRef | `authorization_epoch`, `aux_id`, `aux_version`, `event_mono_ns`, `exec_generation`, `exec_journal_seq`, `instance_id`, `logical_strategy_id`, `reason`, `report_flags`, `report_kind`, `report_seq`, `strategy_generation`, `value_0`, `value_validity` | 0x080 / 0x080 |
| OrderOwnership | `logical_strategy_id`, `instance_id`, `strategy_generation`, `exec_generation`, `exec_journal_seq`, `report_seq`, `order_id`, `origin_strategy_id`, `origin_instance_id`, `origin_generation`, `origin_intent_seq`, `manager_strategy_id`, `manager_instance_id`, `manager_generation`, `owner_revision`, `parent_order_id`, `list_id`, `venue_algo_id`, `venue_algo_id_length`, `relation_kind`, `status`, `reason` | 不适用 |

### 控制操作码

| ID | 操作 | 请求/事件消息体 |
|---:|---|---|
| 1 | HELLO | HelloBody |
| 2 | WELCOME | WelcomeBody |
| 3 | RESOURCE_OFFER | OfferPrefix |
| 4 | MAP_WRITER | ResourceAckBody |
| 5 | READ_READY | ResourceAckBody |
| 6 | ACTIVATE_RESOURCE | ResourceAckBody |
| 16 | JOIN | JoinBody |
| 17 | JOIN_READY | JoinBody |
| 18 | FREEZE | FreezeBody |
| 19 | SWITCH | SwitchBody |
| 20 | QUIESCE | QuiesceBody |
| 21 | DETACH | QuiesceBody |
| 32 | GRANT | GrantBody |
| 33 | REVOKE | GrantBody |
| 34 | PREPARE_HANDOVER | GrantBody |
| 35 | COMMIT_OWNERSHIP | GrantBody |
| 36 | RELEASE_RISK | GrantBody |
| 48 | QUERY | QueryBody |
| 49 | STATUS | StatusBody |
| 50 | HEARTBEAT | StatusBody |
| 51 | ERROR | StatusBody |
| 52 | EXEC_LEASE_CHALLENGE | ExecLeaseBody |
| 53 | EXEC_LEASE_RENEW | ExecLeaseBody |
| 54 | EXEC_LEASE_APPLIED | ExecLeaseBody |
| 64 | RESYNC_REQUEST | ResyncBody |
| 65 | RESYNC_ACK | ResyncBody |
| 80 | CONFIG_PREPARE | ConfigControlBody |
| 81 | CONFIG_APPLY | ConfigControlBody |
| 82 | CONFIG_RUNTIME_APPLIED | ConfigControlBody |
| 83 | CONFIG_FILE_DURABLE | ConfigControlBody |
| 84 | CONFIG_STATUS | ConfigControlBody |

### 枚举

除枚举明确列出的零值，零均无效；字段不适用时按消息规则全零。

| 枚举 | 数值与含义 |
|---|---|
| `snapshot_section_kind` | 1=StateRecords；2=DepthLevels |
| `config_apply_class` | 0=NoOpRevisionAdvance；1=Hot；2=Quiesce；3=RollingRestart；4=Retire；5=ForbiddenRuntime |
| `config_status` | 0=Query；1=Pending；2=DesiredDurable；3=Prepared；4=Quiesced；5=RuntimeApplied；6=EffectiveDurable；7=Retired；8=Stalled；9=Failed；10=Succeeded |
| `config_actor_kind` | 1=Controller；2=Materializer；3=Runtime |
| `config_file_kind` | 0=None；1=Desired；2=Effective |
| `projection_transport` | 0=None；1=AuthenticatedObjectService；2=ImmutableMemfdResource |
| `projection_format` | 0=None；1=TOML；2=JSON |
| `clock_domain` | 1=ClockMonotonic |
| `rounding_policy` | 0=ExactRejectRounding |
| `ledger_adjustment_kind` | 1=Funding；2=FeeCorrection；3=Liquidation；4=ADL；5=ExternalAdjustment；6=TransferAttribution |
| `relation_kind` | 1=Standalone；2=SpotListParent；3=SpotListChild；4=AlgoParent；5=AlgoTriggeredChild |
| `resource_kind` | 1=MarketBank；2=IntentBank；3=ReportBank；4=Cursor；5=MarketSnapshot；6=ExecutionSnapshot；7=InstrumentCatalog；8=TopicSnapshotManifest；9=ConfigProjection |
| `architecture` | 1=Linux_x86_64_le_4k；2=Linux_aarch64_le_4k |
| `endian` | 1=Little |
| `trust_profile` | 1=qualified_sdk；2=isolated_adapter |
| `qos` | 1=StrictTrading；2=LatestRelay；3=ArchiveRelay |
| `venue` | 1=Binance |
| `product` | 1=Spot；2=Usdm |
| `event_class` | 1=Quotes；2=Trades；3=Depth；4=ReferencePrices；5=FeedLifecycle；6=Execution；7=NormalizedMarket |
| `producer_lifecycle` | 1=Initializing；2=Ready；3=Active；4=Frozen；5=Retiring；6=Closed；7=Faulted |
| `cursor_lifecycle` | 1=Initializing；2=Ready；3=Active；4=Quiescing；5=Quiesced；6=Detached；7=Faulted |
| `payload_kind` | 0=None；1=InlineLevels；2=ArenaLevels |
| `feed_state` | 1=Bootstrapping；2=Live；3=Invalid；4=Resyncing；5=Stopped |
| `command_kind` | 1=New；2=Cancel；3=CancelReplace；4=AmendKeepPriority；5=Modify；6=Query；7=CancelList；8=CancelAlgo |
| `side` | 0=None；1=Buy；2=Sell |
| `order_type` | 0=None；1=Limit；2=Market；3=StopLimit；4=StopMarket；5=TakeProfitLimit；6=TakeProfitMarket |
| `time_in_force` | 0=None；1=GTC；2=IOC；3=FOK；4=PostOnly；5=GTD |
| `position_side` | 0=None；1=Both；2=Long；3=Short |
| `stp_mode` | 0=Default；1=None；2=ExpireTaker；3=ExpireMaker；4=ExpireBoth；5=Decrement |
| `working_price` | 0=None；1=ContractPrice；2=MarkPrice |
| `intent_status` | 1=Received；2=Prepared；3=RiskRejected；4=Accepted；5=LocalTerminated；6=MayHaveSent；7=Resolved |
| `order_status` | 1=Pending；2=New；3=PartiallyFilled；4=Filled；5=Canceled；6=Expired；7=Rejected；8=Unknown |
| `send_state` | 0=NotSubmitted；1=MayHaveSent；2=Confirmed |
| `cancel_state` | 0=None；1=Pending；2=Accepted；3=Rejected；4=Unknown |
| `reservation_state` | 1=Prepared；2=Committed；3=Consumed；4=Released；5=Unknown |
| `authority_kind` | 1=StrategyGrant；2=ExecutorRiskAction；3=GuardianEmergencyGrant |
| `grant_phase` | 1=Prepare；2=CommitOwnership；3=ReleaseRisk；4=Revoke |
| `switch_kind` | 1=Prepared；2=Applied；3=ResyncRequired |
| `ack_kind` | 1=WriterMapped；2=ReaderReady；3=Activated |
| `access` | 1=ReadOnly；2=ExistingWriterMapping |
| `seal_profile` | 1=SingleWriter；2=ImmutableObject |
| `role` | 1=Supervisor；2=Market；3=Strategy；4=Executor；5=Guardian；6=Relay；7=Adapter；8=ConfigMaterializer；9=ConfigController |
| `evidence_kind` | 0=None；1=ReaderQuiesced；2=SupervisorProcessExit |
| `query_kind` | 1=Request；2=Operation；3=Resource；4=Membership；5=Grant；6=Intent；7=Snapshot |
| `snapshot_kind` | 1=MarketDepth；2=ExecutionState；3=InstrumentCatalog；4=TopicSnapshotManifest；5=MarketInstrumentState；6=NormalizedMarketSnapshot |
| `control_state` | 1=Received；2=Prepared；3=Applied；4=Rejected；5=Frozen；6=ResyncRequired；7=Closed |
| `errors` | 0=OK；1=Malformed；2=AbiMajor；3=AbiMinor；4=UnsupportedFeature；5=UnsupportedArchitecture；6=Bounds；7=Checksum；8=ReservedNonzero；9=UnknownMessage；10=InvalidEnum；11=Identity；12=StaleControlEpoch；13=StaleChannelEpoch；14=StaleRingEpoch；15=StaleFeedGeneration；16=StaleStrategyGeneration；17=StaleExecGeneration；18=StaleAuthorization；19=VersionConflict；20=IdempotencyConflict；21=SequenceGap；22=QueueFull；23=ArenaExhausted；24=BankExhausted；25=NotReady；26=Deadline；27=RiskRejected；28=InstrumentVersion；29=UnsupportedOperation；30=SealMismatch；31=MissingResource；32=ResyncRequired；33=Ownership；34=RecoveryEvidence；35=RiskGateRevision；36=Permission；37=CommitPending；38=SnapshotExpired；39=ConsumerLimit；40=SequenceExhausted；41=InvalidCursor；42=RateLimited；43=InternalFault；44=VenueRejected |

### 位集合

表中数字是位编号，编码值为 `1 << bit`；未列出的位必须零。

| 位集合 | 位号与含义 |
|---|---|
| `config_flags` | 0=GlobalOperation；1=SuccessorBinding |
| `report_value_validity` | 0=Quantity；1=CumulativeQty；2=LeavesQty；3=Price；4=LastQty；5=LastPrice；6=Fee；7=Value0；8=Value1；9=Value2 |
| `features` | 0=RecordCRC32C；1=ArenaDepth；2=MultiTrigger8；3=ImmutableSnapshots；4=QualifiedReadOnlyAtomic64；5=ConfigControlV1 |
| `record_flags` | 0=SourceTimeValid；1=IngressTimeValid；2=PublishTimeValid；3=CRC32CValid |
| `control_flags` | 0=Request；1=Response；2=Received；3=Applied；4=Error；5=Event |
| `permissions` | 0=NewRisk；1=CancelOwn；2=AmendOwn；3=Query；4=AdoptPending；5=RiskReduce；6=GuardianCancel |
| `intent_flags` | 0=QuoteAmount；1=PriceProtect；2=ClosePosition |
| `depth_flags` | 0=AbsoluteQuantities；1=Snapshot；2=CoverageLimited |
| `trade_flags` | 0=BuyerIsMaker；1=Aggregate |
| `report_flags` | 0=Correction；1=ExternalOrigin；2=RecoveryReplay |
| `coverage_flags` | 0=Limited；1=UnknownOutside |
| `join_flags` | 0=SnapshotRequired；1=Gating |
| `instrument_flags` | 0=TradingEnabled |
| `position_modes` | 0=OneWay；1=Hedge |

## 9. 编码基准样例与验证边界

基准样例保存预先核对并固定的输入和预期字节。测试将编码结果与这些字节逐项比较，避免用同一段被测代码生成预期结果。

向量文件保存完整 hex、明确字段值、byte_length、SHA-256 和“未列字段补零”的规则。它包含 quote、inline depth、arena block header、1024 字节 New 意图、含负费用与负指数的 Fill、多标的 TopicSnapshotManifest 的完整 16 KiB 文件、NormalizedMarket 混合状态与深度的完整 12 KiB 文件、配置应用/运行时/文件/完成控制帧、JOIN、FREEZE、QUIESCE/SWITCH 的结构化响应、exec 短租约、RELEASE_RISK、完整控制帧、instrument 精度和独立 Cursor 页。可以从 JSON 字段直接编码，再逐字节与 hex 比较；完整 control frame 的 parts 指向 envelope/body 两个向量；CompositeFile 的 segments 给出文件偏移与对应向量，未列区间补零。多标的样例同时检查资源头摘要、清单行摘要、两个子对象摘要、各自条目摘要以及完整文件摘要。

例如价格 6500000 ticks、数量零的 DepthLevel 是 16 字节：

```text
a02e6300000000000000000000000000
```

当 price_step_mantissa=1、price_exponent=−2 时，这个价格是 65000.00。New 意图向量 quantity=100 lots、qty_exponent=−5，对应 0.00100；Fill 向量 fee_units=−65、asset_exponent=−8 对应扣费 0.00000065。十六进制不是文本价格字符串。

机械验证覆盖布局完整覆盖/无重叠/自然对齐、atomic 128 字节隔离、嵌套尺寸、默认文件偏移和算术、全部向量的字段解码/重编码与 SHA-256、完整控制帧长度和 CRC32C。它证明这些设计产物内部一致；跨语言生成器、只读原子指令、seals、恶意输入、真实多进程暂停/崩溃、跨核可见性和性能必须由实现阶段按[验证计划](../verification-plan.md)执行。

向量的 structural_valid=true 只表示编码形状有效，不表示此刻可以生产放行。保留原 RELEASE_RISK 样例 valid_from=1 s、valid_until=2 s、control_lease_deadline=3 s；配套应用用例把当前 exec 时间固定为 2.95 s、当前短租约挑战签发于 2.9 s/截止于 3 s、资源会话截止于 5 s，此时短租约有效但策略授权已过期，必须返回 Deadline。它不会通过缩短或改写原向量的时间来伪造准入成功。semantic_cases 另覆盖 Fill 非零字段缺失有效位、禁用字段、缺失 topic 成员、混合状态/深度分段、Cursor 零容量、快照非二次幂条目数，以及配置发送角色错误、旧 runtime revision、未协商 feature、意外 boot、runtime 伪造文件收据和目标未齐仍试图完成。配置样例只验证给定认证/提交上下文下的格式与身份版本判定，不表示已经部署对象服务或写入真实配置文件。

ABI 准入至少包含：非零保留区/未知枚举、溢出 offset、越界 arena、陈旧 allocation_seq、错误目录版本、相同幂等键不同内容、FD 计数/截断、旧授权和旧 control epoch；每类都必须在进入业务 reducer 前拒绝。并发验收覆盖发布前崩溃、发布后暂停、乱序完成的内部借用、JOIN 与 Freeze 交错、S→S+1 切换、全 bank 耗尽、ACK 丢失与旧持有人恢复。所有真实时间预算和低延迟目标仍需在默认完整负载下测量。
