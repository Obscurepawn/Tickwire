# 运行时行情目录扩展

Status: implemented-partial

目标是在 MD、执行和策略进程保持运行时追加交易标的。固定目录继续使用冷建索引；运行时目录追加保留已有本地编号，冷路径完整准备候选，通知热路径仍使用预绑定槽位。首个接线范围是同一已批准 catalog 内向现有 topic 追加 feed；更换 catalog 则同时校验经济规格、执行授权和全部来源重绑定。

## 受信计划与候选

MD 的扩展计划由已经持有真实子进程身份的 launcher 写入私有控制目录。计划绑定非零 operation ID、当前 revision/规范配置摘要、当前精确 bank 描述、目标 revision 和新 bank 模板、完整目标配置、批准消费者的旧/新 Admission、预热期限、新鲜度及峰值存储预算。读取文件和校验摘要不替代 launcher 授权；运行时只处理启动配置明确批准的私有控制路径。

`tickwire_md::expansion::Plan` 最多 4 MiB，预热期限距准备时刻最多 60 秒。`prepare` 核对全部旧 feed 的规范字节与实际参与者；新请求、游标、快照和会话 nonce 不得复用旧身份。`Prepared` 只提供不可变访问，`finalize` 在真实 cut 上填写新 bank 的创建时刻、cutover、first_seq 与描述摘要，再把同一精确 bank 字节交给全部新 Admission。模板中的创建时刻/cutover 为零、first_seq 为一；descriptor_hash 在 finalize 才确定。bank 与会话复用原有语义校验，63 位序号保留边界和倒退时钟在准备/封口阶段拒绝；消费者深度容量保持原值并覆盖目标 topic 的上限。当前实现保持旧 bank 几何布局不变，目标 topic 只允许增加缓存/快照预算与集合版本；退休加活动加候选 SHM 同时计入峰值。

目标配置保留全部旧 feed、事件来源、网络配置、产品及价格数量规格；新 feed 只加入目标 topic，owner 和完整本地 BookKey 不冲突。目标集合严格增加，instrument_set_version、ring_epoch、feed_generation 递增；catalog/version/hash、进程身份、channel 和 host boot 保持不变。相同 operation 重试读取原结果，旧 revision、不同内容复用 operation、遗漏旧成员和更改旧规格均拒绝。活动配置仅在候选完成后替换。

`FeedIndex::expanded` 在独立候选中按原编号顺序复制映射，再按目标配置的首次出现顺序追加新 ID；完整旧集合必须保留。失败不改变活动索引。新深度和事件 owner 通过真实 WSS、REST 与同步逻辑预热，旧 owner 在预热期间继续发布。只创建 socket 或收到订阅 ACK 不算预热完成，候选的每个必需类别及深度均须可用且未过期。

## bank 切换

`TopicPublisher::prepare_expansion` 冷建候选，保留旧行的完整身份、来源绑定、状态和预留容量。新增行只通过实际通知更新；断线、缺口和不合法输入撤销就绪。候选预算计入旧/新缓存同时存在的峰值；MD 另外核对活动、候选和退休 SHM 的合计预算。

提交前停止新的旧 bank JOIN，撤销旧消费资格并等待所有批准消费者的真实 QUIESCED。`JoinOwner::all_detached` 只在所有注册条目都已完成真实退出时成立，Failed、Expired、Pending 和连接 EOF 均不能代替证明。新 bank 的 cutover_seq 在提交点取旧 published，first_seq 为下一序号；候选刷新旧行的最新状态，再核验所有状态和深度新鲜度。完整新基线写入未公开的新 bank 成功后，冻结旧 owner 并交换；旧映射保留至合法回收。

预热、配置或容量错误发生在撤销旧资格前，旧链继续运行；撤销之后失败则保持关闭，不能复用旧代次。普通 `replace_bank` 仍要求集合完全一致，目录扩展使用独立入口。

### MD 控制连接门禁

`Server::active_admissions(topic)` 返回当前真实 Active peer 的完整 Admission，任一同 topic 未完成握手/退出时拒绝；尚未收到 HELLO、无法归属的连接也保守拒绝计划确认。`begin_expansion(topic, operation, expected, owner, now)` 原子复核完整参与者集合及实际旧 bank，再暂停该 topic 新 JOIN；操作 ID 非零，每个 QUIESCE 请求 ID 由 operation 和完整订阅身份派生。进入撤销阶段后任何发送、EOF、租约或期限错误都保留暂停并标记操作失败。

QUIESCE 待发整包由 peer 的有界 outbox 保留，poll 只调用一次 try_send；背压/EINTR 不推进，成功后才等待 QUIESCED。只有 `JoinOwner::quiesced` 核验已注册实际 cursor 的退出状态后才移除 peer。`expansion_quiesced(topic, operation, owner)` 核验原 bank 身份、操作未失败及真实 all_detached，并锁存成功。

`resume_expanded_topic(topic, operation, owner)` 还核对 owner 已换为同 topic/channel/producer 的真实后继 bank，resource 不同、predecessor_ring_epoch 指向旧 ring、ring/feed 代次递增；仅完成旧 quiescence 不能开放旧 bank。暂停期间匹配的 HELLO/Request/Waiting 被拒绝，尚未创建 owner gate 的拒绝不触发其他健康 topic 重建。相同操作和相同参与者重试保持原状态，不重复生成撤销请求；不同内容复用操作拒绝。

恢复 JOIN 时后继还须 `needs_bank == false` 且完整状态为 live；提交后故障或尚未预热的后继不能记录为恢复成功。重复恢复也检查当前可用性：后继已故障则返回错误并保持操作关闭，已记录的后继身份只保留历史完成事实，不清除当前故障门禁。

控制服务最多保留 128 项扩容历史，每项最多 128 个参与者；历史只存完整规范 Admission 的 SHA-256 摘要及 bank 身份，避免按操作重复持有全部配置。活动 Admission 仍按原完整对象返回给计划校验。

HELLO 只为计划识别连接归属，尚未为 owner 注册游标；该阶段及等待 JOIN 请求阶段断连不触发行情重建。进入实际 prepare 后的失败继续保留既有关闭规则，owner gate 仍受追踪。

## 消费者与可重放基线

共享控制对象位于 `tickwire-market-source::transition`。`Approval` 绑定完整旧 ConnectConfig、新模板、operation、完整 MD Plan 摘要、目标 revision 和本机单调期限；`Commit` 绑定同一操作及最终 ConnectConfig。对象按 2 MiB 上限解码，校验只证明内容匹配，调用者仍须通过启动批准的控制入口认证请求。新 bank 模板与最终描述仅允许创建时刻、cut、first_seq 和 descriptor_hash 不同；旧消费水位必须位于真实 cut 以内。

消费者先核验当前实际 Client 并返回 `Armed` 收据，MD 收齐全体收据后才发 QUIESCE。实际 cursor 退出和完整响应成功发送后进入 `Quiesced`，此时仍保留旧消费水位；最终 ConnectConfig 到达后驱动 PendingJoin。`Activated` 收据绑定真实新 Client 的描述摘要及激活水位，重试比较完整 Approval 摘要、实际 PID 和阶段。`ShmSource` 在新 Client 激活后暂停观察，调用方同步 effective 与收据并显式 release 后才恢复向执行端报告行情；错误保持失效。策略端在同一位置先同步 MarketTransition，再保留合并恢复闭包所需的行与检查点，之后才允许新回调。

### MD 运行时

MD 的 `Runtime::poll_once` 与常驻 `run` 共用实际网络、quota、Publisher 和 Server。新增 owner 通过单任务冷 worker 完成证书读取、DNS 和客户端构建；主循环只接收完整结果，配额申请、连接启动、轮询与发布仍由同一线程独占。冷任务失败或过期时旧行情继续推进，已有 quota 历史保持。

每个 depth owner 保存固定 stream/snapshot token，每个事件 owner 保存固定 token；同进程只递增分配，候选取消不复用 token，重连沿用原值。旧客户端和 BookKey 保留。新增 owner 先将实际通知送入私有 TopicExpansion，全部必需状态新鲜后才进入控制准备。提交前按候选目录为目标 topic 全部旧、新 depth/event owner 重新绑定 InstrumentSlot，使路由匹配按 ID 重排后的目录；其他 topic 的路由不变。

扩容控制只能从启动配置显式启用的私有目录读取，默认关闭。计划、操作摘要、revision、精确旧 bank 和实际参与者绑定；Arm/Commit 为两阶段准备，全部真实消费者 Armed 收据匹配后才允许调用 begin_expansion。暂停 JOIN 后持续驱动真实 QUIESCED；最终 cut 取当前 published，候选刷新旧行，再提交同一进程的新 bank。旧 bank 持续保留并计入上限，未完成扩容的 topic 不走普通故障 replace_bank 分支。

运行时最多保存 128 个已解码操作的规范计划摘要与结果，包括准入前被拒绝的计划。每个 operation ID 绑定唯一规范内容，修正内容后使用新 ID；文件排版变化不改变规范摘要。历史达到容量后拒绝新操作，始终保留既有内容绑定。

旧 Admission 可保留启动时 descriptor_hash 为零的模板；与实际 base_bank 比较时只补全该摘要，其他字段仍逐字节一致，非零错误摘要拒绝。原 Admission 和 Approval 的规范字节不被改写；最终 Commit 使用真实完整非零摘要。

提交后换入预计算路由、目标 FeedIndex 和配置，写入实际新 bank 及消费者准入，等待全部新消费者真实激活。只有运行时应用与 effective 文件具有相同 revision/hash，且消费者完成真实重新 JOIN，才记录成功。撤销前失败丢弃候选并保留旧链；撤销后任何失败保持暂停，不伪造文件或运行时收据，不自动恢复旧代次。

消费者从计划取得精确批准的新 ConnectConfig，并使用有界 `PendingJoin` 驱动认证、快照、增量追平和激活。每次 poll 限制协议步骤或记录数，不等待 socket 可读写；快照映射、校验和副本构造仍是按容量上限约束的冷步骤。旧阻塞 `Client::connect` 兼容入口驱动同一状态机，执行轮询不得调用该入口。

策略切换只适用于已注册完整输入重放的 owner。`MarketTransition` 输入绑定 operation/approval 摘要、精确旧消费游标、新流身份、真实 JOIN 基线、消费时刻及实际追平记录；整体遵守既有 16 MiB 类型化输入预算。新基线和追平作为一次结构转换，不执行用户回调、不产生命令。运行时先核对重建副本与新 Client 的实际副本和游标一致，持久保存输入及 HEAD 后才替换旧 Client，并允许后续回调。

来源重放、schema3 continuation 和安装沿同一市场流谱系处理转换；旧格式数据保持可读。重放恢复到相同盘口及策略状态，只证明确定性状态重建，运行资格仍由当前 JOIN 和执行授权决定。执行进程也需自己的真实重新 JOIN，新增行情不会创建账户资产、资金或交易权限。

## 验收

实验场景 `spot-market-expansion` 沿用自定义 V1→V2 schema3 交接，先固定 S 并启动真实 Shadow。两策略均 READY 后向原 topic 追加已经存在于账户 catalog 的 ETHUSDT 高位 ID，执行订阅、旧 Active 与新 Shadow 都参与 Arm/QUIESCE/Commit。三者的新描述与实际水位、MD effective revision/hash 及未改变的进程身份核对完成后，才继续来源 S→C 重放、安装、回滚和迟到成交。交易所 fixture 为各标的维护独立深度序号，保持真实 TLS/WSS/REST 编解码。

- 同一 MD、执行与策略 PID，从一个标的追加高位 `u64` 标的，经过真实 TLS/WSS、REST、SHM 与认证 JOIN；旧本地编号稳定，新编号追加。
- 新快照包含完整旧/新成员，增量从其快照切点后连续消费；真实持久输入可重放得到同一盘口和策略状态。
- 预热超时、失效事件、目录冲突、容量不足或伪造 admission 不产生成功切换；QUIESCED 超时保持关闭。
- 重复操作幂等，旧 revision/旧 admission 拒绝；操作成功要求运行时与同 revision/hash 的 effective 收据均完成。
- 同 catalog、同进程运行时装配与跨 bank schema3 交接已通过原生 Linux release 场景；[阶段证据](../evidence/20261028/market-expansion-runtime.json)绑定源码、二进制、实际水位和完整输入前缀。跨 catalog 变更与进程重启后的扩容恢复尚未实现；启动检查不会把历史 Activated 当作当前资格。

## 执行进程装配

`Deployment.market_transition` 显式批准私有 consumer/effective 目录、操作数量和轮询预算，要求已有 `control`；可管理直接 MD 行情源，也可只管理实际托管的远程策略。`ControlInbox` 只读取当前执行者及实际托管远程策略的 `<pid>-<instance>.arm.json` / `.commit.json`；目录须为真实 0700 目录，输入必须是有界普通文件，拒绝符号链接和 FIFO。共同 Approval/Commit 还须绑定完整旧消费身份、时钟域、操作、批准摘要和递增 revision。该入口复核当前 `control::Binding`，先同步完整 MarketDecision 内容寻址对象，再将其长度和摘要引用同步到原控制 WAL，之后才交给实际消费者，不占用常规控制命令的外部 sequence。

Sources 在同一运行线程保留当前 ConnectConfig，通过共享冷控制句柄接收批准，逐帧消费不加锁、不分配控制对象。没有有效 Arm 的旧 QUIESCE 保持原故障关闭语义；已 Armed 的消费者必须先取得实际 mapped cursor QUIESCED，Commit 才能启动有界 PendingJoin。新 JOIN 完成后，执行消费者先暂停新观察；执行进程同步 effective 与阶段 receipt，再调用本地 release_transition 放行。远程策略同步 Live 输入与 HEAD、成功保留合并恢复行和检查点并同步回执对象后返回 Activated；`retain_merged` 本身是有界内存追加，外层恢复闭包在封口或准备时同步。执行进程收到真实回执并同步本地 effective 后，才对外报告完成。

Armed/Quiesced/Activated 收据使用共同 Receipt，原子替换 `<pid>-<instance>.receipt.json`。MD 必须核对全部批准消费者的当前 Armed，之后才撤销旧资格。重复输入核对完整内容摘要，不重复启动 JOIN；同 operation 不同内容、旧 revision、身份变化或收据持久失败保持关闭。恢复只恢复已消耗操作的历史，不把旧文件、决定或 Activated 收据重放为当前活跃资格。新增行情不修改账户目录、资金或交易授权。

执行进程在 MarketDecision 对象与 WAL 同步后、effective 同步后、写共享阶段收据前，以及共享收据同步后的本地 release 前，重新采样实际单调时钟并核对当前 Binding。跨过批准期限或身份变化时不再 dispatch/放行，也不把操作记为完成。Receipt.observed_mono_ns 保留真实消费者观察到阶段的时刻，不以文件完成时间改写；若收据 rename 已完成而后续同步跨过期限，已可见文件只保留历史事实，本地保持关闭，MD 用自身当前时钟拒绝超期收据。

MarketDecision 的 WAL `at_ns` 是 Controller 已验证完整决定的真实准入时刻，随决定显式传入持久边界；后续实际采样控制是否仍可 dispatch，不覆盖该准入时间。已准入决定同步后过期仍消耗 operation 历史，重启按原准入时刻验证历史，但不自动重放或重新授予执行资格。

执行配置字段为 `consumer_directory`、`effective_directory`、`maximum_operations`（1..128）和 `maximum_per_poll`（1..32）。两个目录须分离，输入和输出文件权限为 0600。effective 文件为 `<pid>-<32位小写十六进制instance>.json`，包含 `schema:1`、完整 `approval_hash`、最终 `commit` 与实际 `receipt`。共享 receipt 仍是共同 Receipt 原始对象。MarketDecision 对象存入控制 journal 目录下 `market-objects`；恢复同时校验对象摘要、长度、Binding、时钟和操作顺序，缺失或损坏立即拒绝。对象和 WAL 共同受 64 MiB 总预算约束，普通控制命令保持 32 KiB 上限，物理 WAL 的 64 KiB 单批上限保持不变。

执行入口的文件扫描固定间隔至少 10 ms，每次最多处理配置数量的当前消费者；未到扫描时刻只比较单调时间和故障状态，不创建集合、不读文件。已经处理的 Arm 以文件设备/inode、长度、mtime 与 ctime 缓存，文件未变时不重读或重新编码完整对象；原子替换触发重新校验。文件读取、对象/WAL 和 effective 同步属于这些有界冷轮询，仍会占用主线程，尚未宣称硬实时冷控制隔离。

## 实际进程实验

`tickwire-lab generate --scenario spot-market-expansion` 使用独立 counting v1/v2 worker。账户目录在启动前已包含 ETHUSDT（外部 ID 为 `u64::MAX`），初始 topic 仅订阅 BTCUSDT。先从真实持久 S 启动 Shadow，收到实际恢复回执后 ARM；实验室再从实际 MD active 描述及三个消费者 Admission 构造受信 Plan，新增 ETH 的 REST/WSS 仍使用本地 TLS 对端的真实协议栈。

扩容验收同时核对 MD 运行时结果与 effective、每个消费者的真实 Activated 和各自 effective、MD/执行/旧 Active/Shadow 的 PID 与进程启动时刻不变。全部 Activated 后，还须从旧 Active 的真实 HandoverSeal/保留输入闭包读取新流游标，确认其覆盖三个消费者的最大实际激活水位，才允许固定 C 并继续原 schema3 来源重放、安装、回滚探针与管理权交接。`market-expanded.json` 只记录这些已核对事实，不能单独作为成功证明；最终 `run-result.json` 还必须通过原账户、费用、镜像和自定义策略计数验证。

实验室同时验证完整历史前缀：扩容观测 cut 必须由固定 S 的真实检查点通过保留输入哈希链推进，最终来源重放 C 必须包含该观测 cut。固定 S 的封口摘要与完整检查点对象、观测封口摘要及完整 cut 均与最终已验证的来源闭包和本次进程内证据一致；同序号或更高序号的另一分支不能通过验收。
