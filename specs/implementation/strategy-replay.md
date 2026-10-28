# 策略合并输入与 schema3 交接

接口约束固定旧 binary/旧配置从 S 重放至实际 C，随后迁移、应用新配置并安装。实现使用 TWINP002 完整输入顺序、记录时间、timer/RNG 和输出摘要；历史回调没有 IntentWriter。SDK 真实回调结果、输入对象和完成检查点先同步，随后才允许实时命令发布。

`tickwire-release::replay` 提供共享对象校验，SDK 负责实际回调和持久文件，Host 负责固定 executable、真实子进程、认证控制消息和游标；app 重新核对完整账户前缀。来源 S、原 signed Prepare 和完整 Operation/member 贯穿所有阶段。

`CommittedHistory` 核验持久对象内容、输入顺序及切点连接，兼容 schema1 完整前缀接 schema2 增量段。泛型业务状态是否与保存端点一致，仍由旧工厂实际回放并比较完整检查点确定；历史校验结果不授予恢复运行或输出权限。

## 运行中行情换代

合并输入增加 `MarketTransition`：非零 `operation_id/approval_digest` 绑定受信切换计划，`previous` 保存旧流的完整 Cursor，`next` 保存新 bank 的 StreamKey，`baseline_sequence/activated_sequence` 保存快照与实际激活水位。`baseline` 使用既有 TWSB0001，保留真实 JOIN 描述、快照原文、基线验证时间和盘口容量；`catchup` 按消费顺序保存 512 字节记录、payload 与实际应用时间。旧输入编码与检查点 schema 保持兼容；不支持该变体的旧 executable 必须拒绝新尾部。

SDK 从已完成认证 JOIN 的 Client 与旧 consumer 在 QUIESCE 后的实际 applied 水位构造记录。旧 Cursor、副本和消费水位一致，新 bank 必须绑定同一 topic/channel/catalog_version 与进程、时钟域，其 ring/feed 代次递增、predecessor/cutover/first_seq 连续。旧稳定 instrument ID 与版本全部保留；新基线经既有快照验证器校验，再顺序应用 catchup，完整副本字节与新 Client、激活消费水位一致。旧检查点不含 catalog_hash，前后 hash 相同由运行时受信计划准入核对并绑定 approval_digest；有效字节不赋予配置或交易授权。

整个基线与追平作为一个不调用用户策略、不产生 effects 的结构输入；实时与历史重放都只替换市场副本与运行时 Cursor。记录、检查点、提交与 HEAD 完成持久化后，运行时才能替换旧 Client 并恢复回调。持久失败停止 owner；超出类型化 16 MiB 预算的基线或 catchup 在切换提交前拒绝。Legacy owner 不支持运行时扩容，计划准入先检查 replay 能力。

schema3 的 Shadow 保留段、来源重放、安装续跑沿 MarketTransition 的单一前后流关系寻找 C，并保留 C 后的实际换代输入。逐条按规范 Input 重算 Cursor.content_digest，在 C、换代 previous 和保留段末尾比较完整 Cursor；SDK 另外比较完整物化视图，安装端点同时比较实际市场 Cursor。原始输入不同但最终盘口相同的历史不能替换 C。快照跳过 C、丢失中间换代、错旧 Cursor 或重复新流均拒绝。续跑不重放 Shadow 输出，换代基线追平仍不调用用户回调。

新运行时转换采用 `cursor_basis=1`：最后一条真实 catchup 记录的 sequence 等于 activated_sequence，将其按 `MarketApplied{next,sequence,received_ns=record.ingress_mono_ns,record,payload}` 规范编码后散列为市场 Cursor 摘要。两个独立消费者的 JOIN 快照资源、游标和本地时间不同，但消费同一生产者记录时得到相同 Cursor；记录原文或 ingress 不同仍得到不同摘要。完整转换、所有 JOIN/快照身份及本地应用时间继续进入原 merged row/hash 链与全量重放验证，不从历史中删除。

旧转换省略 `cursor_basis` 等价于 0，保持原完整 Input 散列和编码；未知 basis 拒绝。basis1 必须含非空真实 catchup。新 JOIN 完成时尚无 catchup，worker 持有新 Client 并保持暂停，有界轮询一条真实市场记录、应用副本和真实 cursor 后再持久转换；未得到记录且到达期限则关闭。基线已跳过 C 的情况继续拒绝，协调器取 C 时需覆盖全部新 JOIN 的激活水位。

## schema3 持久化与安装

### 持续 worker 与 Host 的行情切换

远程 MD 消费者通过既有认证报告通道接收两阶段命令：41/42 的 `replay_kind=6` 表示 Arm，`7` 表示 Commit，包体布局不变。两种命令的 `expected_sequence=0`、`expected_cut_digest=approval_digest`，绑定的是批准计划而非消费游标；operation/revision、对象长度摘要、挑战、实际子进程身份和期限仍逐项校验。对象使用既有报告 CAS，类型化请求至多 2 MiB。Host 统一分发认证报告帧，处理新收据时保留 READY/preflight 的现有校验。

Arm 核对实际旧 ConnectConfig、批准的新模板、recorded replay 能力及生命周期互斥，持久保存批准对象后返回 Armed。取得全部消费者 Armed 后 MD 才能撤销旧订阅。已批准的真实 QUIESCE 结束借用并保存实际 applied，QUIESCED 通过有界待发整包重试；worker 保持运行并暂停用户回调。未批准的 QUIESCE 保留原退休行为。切换期间控制轮询和会话验证继续，Seal/Install/退休与扩容互斥。

Commit 绑定同一批准操作及真实切点确定的精确新 ConnectConfig，驱动有界 PendingJoin。实际新 Client 完成后先持久保存 MarketTransition/HEAD，再通过 retain_merged 与 merged_checkpoint 向有界恢复记录追加完整行和末端检查点，闭包沿既有 prepare/seal 同步；全部成功后才交换消费对象、恢复回调并回复 Activated。追平记录不执行用户回调。审批重试和完成重试返回原决定，操作相同而内容不同拒绝；撤销后任一步持久化、认证或期限失败保持关闭。执行侧共享 Approval/Commit/Receipt 类型，新增行情不增加交易资格。

Arm 前的能力、生命周期和输入校验拒绝使用 kind6/7 的 opcode42 flags18，status 限 Unsupported=1、Busy=2、Invalid=3；体内回显原请求，不生成成功收据，旧消费者继续运行。SDK 的恢复回调边界由自身 durable HEAD 与保留输入建立；App 在发 Commit 前持久完整控制决定，MD 在开放新 JOIN 前同步 effective。App 的最终 effective/receipt 决定整体操作成功；执行 ShmSource 的本地 release 门禁不扩展为第三个跨进程协议。

schema3 完成闭包中的恢复依赖（包括旧策略二进制）沿恢复对象预算校验：单对象最多 64 MiB，完整闭包最多 256 MiB，仍逐个核对长度和摘要。启动、安装、回滚及其嵌套读取使用相同预算；不能用 replay 消息的 16 MiB 上限误拒绝合法二进制依赖。检查点、输入尾部、控制请求及其他类型化 replay 消息继续保留各自的 16 MiB 上限，闭包对象数和递归深度限制不变。

schema3 启动只完成 S 的 restore/migrate，保存 `replay_restore_digest`。Prepare 关闭旧转发后取得 C，启动旧 binary 的 `--source-replay`；目标真实 41/42 安装后保存 `replay_installation_digest`。唯一固定 rollback probe 在 C 的 ConfigApply 输出上执行；Host 观察其真实成功退出后，目标同步 `replay_rollback_digest` 并返回新的 READY。三个根保留在 HandoverSeal v3。schema2 保持原流程。

执行侧依次同步对象与生命周期 State。安装同步返回不确定时，内存不把该阶段当作完成；恢复可以发现完整尾部，但只保留事实并关闭许可。完成回执的精确同会话重试返回原签名；新的消费刷新不再次调用旧回放、迁移、安装或 probe。Commit 必须带协调器采纳的精确 ReplayAdoption，并重新核对实际 READY、完整账户记录、当前订单能力及 gate。

schema3 早期跨进程验收曾因输入累计重写而积压；每次 flush 现以上一持久检查点为锚点写入新增输入，保留固定 S→C 的完整历史。2026-10-28 原生 Linux release 的自定义 V1→V2 场景通过：S=2、C=10，旧 binary 回放、目标安装、独立回滚、Commit/Release、旧 PID 退出及迟到成交均完成。另将两份策略 ELF 副本补齐至 16 MiB＋1 字节后重新生成身份并运行，S=2、C=22 的完整链路同样通过；两轮镜像和目标消费水位均为 12，终态费用闭合。期限与门禁没有放宽，[运行摘要与源码/二进制身份](../evidence/20261028/summary.json)保留实际范围。

`registered::run` 提供静态用户工厂的实际启动、配置解码、旧版本回放和回滚探测入口；`FactoryBinding` 将工厂 ID、配置路径、对象摘要/长度及 revision 绑定到发布资料。内建 worker 拒绝未知工厂，不自动切换策略。跨时钟域转换、目标 Shadow 保留段内的 Config/Model 改动保持拒绝。

Linux 来源重放和独立回滚兼容封印 memfd 启动：实际可执行映像从 `/proc/self/exe` 校验，原始部署制品仍作为独立的完整恢复依赖保留。SDK 来源进程、注册示例 V1 来源进程与独立 rebuild probe 的定向测试分别运行磁盘和封印映像，比较真实 PID、成功退出、回调结果及持久切点；启动路径删除/替换测试另外核对实际映像不变、错误长度/摘要拒绝和恢复制品缺失拒绝。该覆盖不把新的启动方式等同于整条发布流程验收。

独立 `tickwire-strategy-example` 提供两个实际可执行文件，分别使用 V1/V2 状态。其计数和动作状态由真实 SDK 回调改变，C 上迁移保留原计数和已发生动作，新 ConfigApply 后继续处理输入而不重发历史命令。当前已完成 4 项单元测试，以及实际 V1 子进程重放持久 S→C、核对 PID/退出与完整检查点的测试；使用该工厂的完整跨进程交接仍待实际场景验收。

证据：阶段报告。形式化验证按系统统一安排进行，本阶段未新增或运行 TLA。

恢复新增 `ReplayObserved`：新 Query 的当前签名状态携带已保存安装结果。协调器保存历史事实后，只能在全域查询完成和显式 resume 后重新 Prepare，刷新 READY 后再 Commit。旧结果过期不能作为新准入；当前成员期限、gate 或执行会话失效仍拒绝。CLI 和执行侧生命周期 WAL 已接同一显式变体。

## 安装对象与 READY 重复验证修复

第四轮真实 Linux 运行完成旧策略回放后，安装控制对象被 Host 写成 `.bin`，worker 的报告对象读取器按 `object-<摘要>` 查找而失败。Host 现经 `ReportWriter::put_object` 复用既有独占对象存储；单元覆盖实际 writer/reader 一致、重复同步及失败不返回摘要。证据见 对象存储修复。

同轮保留的 34 对象闭包用于工作量回归：相同认证 READY 的三次访问，完整加载从 3 次降到 1 次。Host 仅缓存一份已验证字节，绑定完整 body/目录/进程/会话；身份或期限失效撤销缓存，使用时继续核账户摘要和实际游标。真实文件变化、body/context替换及容量拒绝均有定向用例，见 READY 缓存证据。该阶段尚未代替下一轮 Linux 完整交接结果。

第五轮已读到标准命名的安装对象，但安装控制发送时达到原 30 秒协调器期限。捕获循环中两个不同 body（中间 READY4 和最终 READY5）此前各加载一次完整闭包。实际保留对象的定向测试先得到 2 次加载的 Red，修复后为 1 次；仅最终挑战回执经过完整对象验证后才能成为全局 READY。中间帧保留身份、会话与四类序号单调检查，失败不保留全局水位。证据见 封存中间通知验证；完整 Linux 交接仍未通过。

## 安装后的增量提交链修复

第六轮真实目标安装已同步完成，但旧安装验证器只读取最后一个 schema 2 提交，误把其局部起点与旧策略 C 比较并拒绝。现将有界完整提交链校验下沉到 release，SDK 重开 HEAD、安装验证与 RecoveryWriter 共同调用；保留 schema 1 升级边界、C 后实际输入及完整市场/账户视图比较。实际原对象 Red→Green、恢复写入器入口及负例见验证记录。
