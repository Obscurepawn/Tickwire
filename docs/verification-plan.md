# Tickwire 生产验证方案：多进程、多策略与源码对应的 TLA+

版本：v0.3 · 2026-10-20 · 生产目标的验证契约；以下 Rust、多进程与 TLC 检查尚未执行。

这份方案落实[系统设计](design/binance-ultra-low-latency.md)中的 INV-1～INV-14，以及 v0.3 的持久化、IPC 字节布局、风控计算、配置和部署契约。每个实现 PR 都必须提供相应的单元测试、集成测试和模型检查证据；文档检查通过不代表交易系统已经通过这些验证。

默认 `LocalDurable+AsyncReplica` 与可选 `SyncReplica` 分别验收：前者等待本地完整恢复闭包持久化，后者还等待指定镜像持久化。控制 Raft 不进入逐单提交路径。配置接口只有在固定目标集合的运行时应用、权限投影和本机文件持久收据全部匹配后，才能返回 `Succeeded`；202、控制提交或一次文件 rename 都不是这个完成点。

## 1. 验证对象与职责

| 层次 | 验证对象 | 运行条件与输出 |
|---|---|---|
| 单元测试 | 一个 reducer、计算函数、解析/编码器或同步器 | 无网络、无密钥、确定性输入；保存失败最小用例 |
| 性质测试 | 定点运算、额度守恒、订单排列、重复与迟到事件 | 固定种子可重现；最小反例进入回归集，包括发布/释放、代次与交接状态 |
| 本地集成测试 | 真实独立 md/strategy/exec/supervisor/guardian + mmap + journal + 本地交易所模拟器 | 两产品都覆盖，模拟器控制响应、故障与时间 |
| 官方测试环境 | 真实握手、权限、请求/响应、回报、schema 与条件结构 | 单独显式运行，使用专用测试凭证；生产域名默认拒绝 |
| TLA+ / TLC | 源码中的状态转换与可发生的并发/故障交错 | 每个 action 对应真实代码位置；记录配置、状态数、覆盖和反例 |
| 性能与长跑 | 完整循环、排队、网络、分配、恢复稳定性 | 固定主机和负载；保存分位数、吞吐、丢样和构建指纹 |

交易所模拟器可控制“接收订单”和“发送 ACK”之间的间隔，允许成交先于 ACK、ACK 丢失、查询暂时查不到等情形。测试不能靠真实 sleep 猜测先后顺序，应使用显式调度点、虚拟时间和事件屏障。真实 socket/TLS 的集成用例仍有有限超时，防止挂死。

## 2. 必须先编写的测试清单

清单共有 30 项单元用例（U-01～U-30）和 65 项集成/资格用例（I-01～I-65）。一个编号可以包含多组参数和故障切点；报告同时列出编号与实际执行的子用例数，不把计划条目数当成已执行测试数。

| 用例 ID | 场景与断言 | 不变量/阶段 |
|---|---|---|
| U-01 | 价格/数量步长、舍入、极值、乘法溢出；不得用浮点误差通过过滤器 | INV-1，P1 |
| U-02 | Spot 两个标的争用同一 USDT；第二次预留必须看到第一次占额 | INV-1/9，P1 |
| U-03 | Futures 全仓/逐仓、单向/双向、多资产保证金、未决挂单最坏成交、资金费与手续费；不匹配配置拒绝激活 | INV-1/9/10，P1 |
| U-04 | Spot 重叠范围与边界 `L, L+1`；Futures `U/u/pu`；过期/缺口/零数量删除 | INV-5，P1 |
| U-05 | SBE 变长块、null sentinel、未知模板/版本、短包、长度溢出；坏包不能 panic 或越界 | INV-5，P0/P1 |
| U-06 | 成交早于 ACK、重复成交、旧 ACK、撤单失败、终态后迟到费用 | INV-2/3/4，P1 |
| U-07 | 现货 STP 导致的 prevented quantity/过期，合约强平/自动减仓等无本地意图事件 | INV-1/3/4，P1 |
| U-08 | 请求 ID、clientOrderId、exchange order ID、trade ID 和 algo ID 的命名空间与关联 | INV-2/3/9，P1 |
| U-09 | 单次 not found、超时、连接断开不释放预留；确定未交给传输层才可本地释放 | INV-2，P1 |
| U-10 | 限流窗口/订单计数不同维度，共享账户/IP 预算、优先撤单额度 | INV-1，P1/P2 |
| U-11 | 同事件边界切换 metadata/risk 配置；旧版本意图不能在新规则下直接发送 | INV-8，P1 |
| U-12 | 同一事件序列回放两次，订单/资金/持仓/动作及状态哈希一致 | INV-1～4，P1 |
| U-13 | 同一成交的账户推送/查询快照不重复扣账，旧快照不恢复已消耗额度 | INV-1/3/7，P1/P3 |
| U-14 | 先到成交 102、检查点、再到 101；缺口内真实成交不因最大 ID 被跳过 | INV-3/4，P1/P3 |
| U-15 | IPC 固定布局、大小/对齐/字节序、偏移上界、未知标签、非法 cursor，不能越界或无限循环 | INV-11/14，P0/P1 |
| U-16 | ring 满/空/环绕、批量发布、释放缺口、动态 gating，最后访问前不复用 | INV-11，P1 |
| U-17 | 逻辑策略/实例/授权与 exec/feed/ring 代次分别校验，旧 ACK 不修改新状态 | INV-12/14，P1/P3 |
| U-18 | 策略归属总和与账户事实一致，交接不重复分配资金，跨策略 STP | INV-1/9/13，P2/P3 |
| U-19 | 执行 risk-vectors 的全部数值向量，逐字段精确相等；不同 scale、负数 floor/ceil、`i128::MIN`、乘除溢出均保持定义的方向，失败不改变账户状态 | INV-1/9，P1 |
| U-20 | 接收币/第三币费用情景、账户级独占收费池、逐笔舍入上界和终态 fee_tail；同池不重复预留，抵扣失效/价格无界不能释放费用责任 | INV-1/3/4/9，P1 |
| U-21 | free/locked 覆盖票据、部分成交后撤单、交易所可用额影子上限、费用分项核销；同一现金流和覆盖票据只核销一次，成交或本地资本下降不提前释放影子额度 | INV-1/3/7/9，P1/P3 |
| U-22 | 保证金档位边界、notionalCoef 未知、抵押分档/负债/过期折扣、空仓压力损失和成交后退出费；账户最坏敞口与策略毛敞口分别限制，未确认输入不增加可用资本 | INV-1/9/10，P1 |
| U-23 | 永续与线性交割合约按到期实例分开，交割 funding=NONE 仍保留结算费；预估结算价不入账，到期计时器不释放保证金，参考盈亏只核对真实账户事实 | INV-1/3/4/10，P1/P2 |
| U-24 | WAL 帧长度/对齐/CRC/哈希链、批次 seal 与各连续水位；区分逻辑前缀哈希和物理分段，拒绝错域/错 epoch/错画像的提交证明及封口历史损坏 | INV-2/7，P1/P3 |
| U-25 | RecoveryManifest 为无环内容闭包，当前意图叶子不自引用；输入覆盖收据、依赖 pin 和持久释放一致，只有哈希/URL/策略自报水位不能构成持久证明 | INV-2/7/12/13，P1/P3 |
| U-26 | IPC 全部黄金编码与 semantic_cases：逐字节编解码、字段有效位/白名单、实际 opcode 对应响应体、分资源容量规则；完整 topic 清单共同截面/集合/子哈希/偏移/预算均匹配 | INV-5/11/14，P0/P1 |
| U-27 | TOML/JSON 同内容得到相同规范化哈希；拒绝重复键、浮点、未知字段、单位混淆和非法 scoped-patch；显式默认值在哈希前固定，数组顺序不变 | INV-8/14，P0/P1 |
| U-28 | 配置更改分类和最终 diff 一致；三种降限额政策覆盖持仓、挂单、pending、Unknown 和条件子单，撤单请求不释放预留；风险放宽不能伪装为 Hot | INV-1/2/8/9/10，P1 |
| U-29 | 全量配置、ProcessProjection、ProjectionObject 独立校验；全局 hash、权限 payload hash、交付字节 hash 不混用，投影 policy/目标/issuer/boot 与收据一致且收据不自引用 | INV-8/12/14，P0/P1 |
| U-30 | 固定目标集合中的 planned successor、一次性绑定、实例 slot 与 locator generation 状态转换；计划外 boot/工件/主机、旧收据及回退代次不能完成目标或恢复旧权限 | INV-6/7/8/12/14，P1/P3 |
| I-01 | Spot 和 Futures 各跑行情→决策→预留→下单→部分成交→撤单→对账 | INV-1～5，P2 |
| I-02 | 模拟器已接受订单但不返回 ACK；恢复查询后最多记一笔经济成交，不重发意图 | INV-2/3，P2 |
| I-03 | 在 TLS 接收明文、socket 部分写、完整写和 ACK 回来前分别断开连接 | INV-2/7，P2 |
| I-04 | 私有流重复/跨通道乱序/断连，REST 快照落后，分页边界重复成交 | INV-2/3/4/7，P2/P3 |
| I-05 | 连续旧流、新流同时活跃；shadow book 未完成不得切换，旧 generation 不污染新簿 | INV-5，P2 |
| I-06 | 队列满、writer 暂停、磁盘写/flush 失败，验证停止增险并保留回报处理空间 | INV-1/2/7，P3 |
| I-07 | 每个日志/复制边界注入进程终止/断电；默认画像本地 flush 前不 Accepted/发送，本地提交已发而尚未复制时从完整原盘恢复原 ID/预留；未提交尾部不触发外部动作 | INV-2/7，P3 |
| I-08 | 旧 owner 仍可运行时，新 owner 不获得执行权；实际隔离后仍有旧请求到达交易所，保持 Unknown/预留，完成对账与恢复后才发送 | INV-2/6/7，P3 |
| I-09 | cancel-replace 两腿独立成功/失败、Spot 减量保优先级、Futures 改单保留/重排/触发撤销与成交竞争 | INV-1/2/4，P2 |
| I-10 | Spot 成功而 Futures 拒绝/未知，保留两腿最坏预留并执行有限补偿 | INV-1/2/9，P2 |
| I-11 | markPrice 停更、margin mode 不匹配、既有 CM/算法订单、账户外部变动 | INV-1/5/9/10，P2/P3 |
| I-12 | 因背压/等待 durable watermark 延迟的增险单，发送前再次检查 kill、配置、行情/账户、意图期限和恢复代次 | INV-7/10，P3 |
| I-13 | 心跳/PONG、24h/维护轮换、私有订阅续期、限流/退避、时间戳越界 | INV-5/7，P2 |
| I-14 | 查询 openAlgoOrders/openOrderList 前后触发、父单撤销与触发竞争、FINISHED/working 完成但子单仍存活 | INV-1/2/7，P2/P3 |
| I-15 | 运行中首次出现陌生 ALGO_UPDATE/listStatus，必须立即禁增险并保留父子敞口 | INV-1/7，P2/P3 |
| I-16 | supervisor 崩溃/重启、控制 ACK 丢失、过时代次与重复 request ID；SIGSTOP 后迟到续租沿原本机挑战期限失效，热循环在短租约失效后停止增险，旧 enable 不解除隔离 | INV-6/7/8，P2/P3 |
| I-17 | recorder 堵塞/退出不阻塞交易，录制缺口可见且不被计为完整回放 | INV-5，P2/P4 |
| I-18 | 共享 IP/UID bucket 的额度租给多个线程后总量不超额，窗口/代次/expiry 绑定授权，重启不能重复授予旧窗口额度 | INV-1，P1/P3 |
| I-19 | 真实共享 ring 1/2/4/8/16/32 个消费者均读到正确顺序和完整内容；消费者没有复制整份公共流 | INV-11/14，P1/P5 |
| I-20 | producer 在写 payload/发布序号之前或之后退出，读者不消费半写记录 | INV-11/14，P1 |
| I-21 | 读者持有借用时 SIGSTOP，健康者迁到新 bank；旧 bank 不被改写，确认退出后才回收 | INV-11，P1/P4 |
| I-22 | freeze(S)、注册新 bank gating=S、发布 S+1、迁移 ACK 的各步崩溃/重试 | INV-11/14，P1/P4 |
| I-23 | 加入/退出与 bank 切换并发，JoinSeq/快照/历史覆盖一致，旧代次 ACK 不影响新 membership | INV-11/12/14，P1/P3 |
| I-24 | 独立部署新策略与兼容升级时 md/exec PID 和订单连接保持；存量订单继续成交，交接守恒 | INV-12/13，P3 |
| I-25 | 旧队列尾部在撤权后到达，未发送意图分别测试 terminate/AdoptPending；Unknown 不重发 | INV-2/12/13，P3 |
| I-26 | 多 exec 发布在准备/提交/放行阶段部分失败，回滚分配新代次；无未获全体证明的增险 | INV-12/13，P3 |
| I-27 | exec 卡死或控制 quorum 不可用时，guardian 按原有独立应急授权查询/取消并审计；无法确认隔离时不授权新主，取消父单不遗漏子单/未知意图 | INV-2/6/7，P4 |
| I-28 | 主机故障与网络分区；默认异步画像原机及原盘永久丢失、备机缺尾部时进入 EvidenceLost/Frozen，即使隔离完成、查询可用或复制延迟曾很小也不自动提升 | INV-6/7，P4 |
| I-29 | 兼容策略独立更新、ABI major 双 namespace 迁移、配置/字段单位不匹配 | INV-8/12/14，P1/P3 |
| I-30 | payload 写权限、seal、防 truncate、只读映射原子加载/屏障、FD 泄漏、fork/exec 继承和 PID 复用的实际 OS 测试 | INV-11，P1/P4 |
| I-31 | banks/快照/借用/策略实例并存达到上限，拒绝新准入；耗尽只失效关联 topic/QoS | INV-5/11/14，P1/P5 |
| I-32 | 精确回放的策略检查点/输入水位落后时不能放行外部动作；declared_rebuild 按批准契约预热 | INV-7/12/13，P3/P4 |
| I-33 | Shadow 落后不误撤同逻辑策略旧 Active；回报队列满时 exec 继续入账且策略恢复可补齐 | INV-3/12/13，P2/P3 |
| I-34 | 轮询/通知模式切换、生产者先于休眠发布、重复 eventfd 通知，不出现 lost wakeup | INV-11/14，P1 |
| I-35 | 写者映射 ACK→封印→读者能力→Ready 的故障/乱序；每消费者不能写其他 cursor | INV-11，P1 |
| I-36 | qualified_sdk 发布后不能保留可写引用；isolated_adapter 无 SHM 写能力且畸形输入不能绕过 exec 校验 | INV-11/12，P1/P2 |
| I-37 | 策略撤权后 exec 的风险撤单仍生效，Guardian 隔离前 CancelOnly 不被当成接管/新主授权 | INV-2/6/10/12，P2/P4 |
| I-38 | 订单 WAL 已复制但策略输入/检查点/交接依赖缺失时，RecoveryManifest 不能放行增险 | INV-7/12/13，P3/P4 |
| I-39 | Halt/补偿后延迟到达的未过期 ReleaseRisk 凭据，因 operation/gate revision 失配被拒绝 | INV-12/13，P3/P4 |
| I-40 | 绕过 exec 的外部资金使用触发 ExternallyMutated；受控外部来源与策略共用同一预留入口 | INV-1/7/9，P2/P4 |
| I-41 | SyncReplica 本地完成而镜像 flush/ACK 未完成时不 Accepted/发送；镜像确认后丢原盘，仍须镜像封口、实际隔离、对账和新镜像就绪才增险，不自动降级异步 | INV-2/6/7，P3/P4 |
| I-42 | 复制批次重复/乱序/缺口、ACK 丢失、receiver 重启与同 epoch/seq 不同内容；received 不冒充 durable，冲突隔离，旧 boot ACK 不能替代重新验证介质 | INV-2/7，P3/P4 |
| I-43 | 快照正文/依赖、SnapshotPublish、CURRENT rename/目录同步各切点失败；只安装同一完整边界，不混装状态/applied/ConfState；尾部半批可丢弃，封口中部损坏须隔离 | INV-2/3/7，P3/P4 |
| I-44 | 控制 Raft Ready 批次异步持久完成乱序、term/vote/commit 落盘失败及旧任期多数副本；连续持久前缀之前不发肯定响应，本任期 barrier 提交并应用前不签发控制证明 | INV-6/7/8，P3/P4 |
| I-45 | 控制成员 `{A,B,C}→{A,B,D}` 的 learner 安装/重启、joint/leave-joint 与分区交错；A/C 互通不足以离开 joint，旧 voter 在离开记录提交应用前不退役 | INV-6/7/8，P3/P4 |
| I-46 | 复制延迟越过 100/1,000 ms 与磁盘 80%/90% 保护线；告警/禁增险按画像执行，事实与取消继续，保留未复制尾部或固定完整快照及后缀，不按年龄删活依赖 | INV-1/2/7，P3/P4 |
| I-47 | 检查点、实际输入 sequencer、对象存储、依赖释放与快照压缩并发；缺一输入或 blob 时本地/镜像都不出完整收据，Unknown/发布/安装快照的 pin 阻止回收 | INV-2/7/12/13，P3/P4 |
| I-48 | 多标的 topic 持续更新时 JOIN 与 Freeze 并发；清单完整捕获同一 S，集合变化拒绝旧计划，全部成员通过校验才 Ready，子对象不藏在对象/字节预算之外 | INV-5/11/14，P1/P3 |
| I-49 | 实际控制通道按 opcode 解码 QUIESCE/SWITCH/EXEC_LEASE/配置响应；资源会话、exec 短租约和策略期限独立，结构有效但已过期 RELEASE_RISK 被拒绝 | INV-6/8/11/12/14，P1/P3 |
| I-50 | 通过文件导入和配置 API 分别提交同一候选，所有固定目标 desired/runtime/effective 的全局版本及预期投影一致；两个文件重解析并与持久收据匹配后才 Succeeded | INV-7/8/14，P2/P3 |
| I-51 | 并发同 ETag、同键同/异内容、提交后响应丢失与身份撤权；只建一个 revision/operation，重试重新授权；watch 重复/重连可去重，压缩游标返回 410 后重读状态 | INV-8/12/14，P2/P3 |
| I-52 | ConfigPrepared、desired write/fsync/rename/目录同步、ConfigApply、runtime 切换、effective 同步和收据/ACK 每一步崩溃；按日志恢复同版本，不重复经济动作，不凭 mtime 选版本 | INV-2/7/8，P3/P4 |
| I-53 | 注入 ENOSPC、EIO、只读盘、权限漂移和目录同步失败；desired 未持久不应用，runtime 已应用但 effective 失败则 Stalled/冻结，补写同版本后才可完成 | INV-1/7/8，P3/P4 |
| I-54 | 配置部分主机离线/超时、控制 leader 变化、目标退休和操作被替代；固定目标不被悄悄删除，逐目标版本可查询，跨域屏障保持，resume/supersedes 覆盖残留版本与旧屏障 | INV-6/7/8/12，P3/P4 |
| I-55 | 单账户身份使用 head/scoped-patch 及查询视图；无权对象存在/旧值/secret 引用不泄露，共享政策的间接影响须获全部依赖授权；NoOp delivery 不扩权，成功仍按完整集合而非可见子集计算 | INV-8/12/14，P2/P3 |
| I-56 | 向每类进程交付权限投影并攻击 CONFIG_PREPARE/APPLY/RUNTIME_APPLIED/FILE_DURABLE/STATUS；错误 policy/目标/对象、未封印 FD、冒充 issuer 或单侧收据不能扩权或完成更新 | INV-7/8/11/12/14，P1/P3 |
| I-57 | RollingRestart 旧实例 quiesce/退休、计划内 SuccessorBound、新 staging effective 与 owner cutover 的各切点崩溃；旧新文件分 slot，旧权不复活，任意新 boot 不可冒充计划继任者 | INV-6/7/8/12/13，P3/P4 |
| I-58 | 根目录迁移的 Prepared/Copied、固定 locator rename/目录同步、Activated 与旧根退役各切点崩溃；重启核验 generation/manifest/固定 journal，只保留一个写者，目录同步结果不明时冻结，不按 mtime 选根 | INV-6/7/8/14，P3/P4 |
| I-59 | symlink/hardlink、路径替换、挂载/属主漂移、伪造 mtime 和手改生成文件；安全相对打开拒绝越界，漂移被隔离，不反向导入；候选/错误/日志/文件均不含秘密字节 | INV-7/8/12/14，P2/P4 |
| I-60 | 回滚旧配置时已有新成交/新 schema/撤销秘密/账户模式变化；生成新 revision 并重验依赖和权限，订单、费用、去重、ID 高水位与代次均不回退；生产模板占位/fixture 不准入 live | INV-1/2/3/7/8/12，P2/P3 |
| I-61 | 实际降限额分别执行 reject_if_exceeded/freeze_until_within/cancel_then_freeze；全部潜在暴露参与判断，撤单未终态继续占额，文件完成不替代恢复增险所需的新授权 | INV-1/2/8/9/10，P2/P3 |
| I-62 | 交割窗口叠加时钟误差、挂单/条件单、结算延期、公开价格与账户事实乱序；进入 ExpiryReduceOnly/AwaitingSettlement 后不按时间清仓，净额合并记录不重复扣费，差额冻结待解释 | INV-1/2/3/7/10，P2/P4 |
| I-63 | 参考部署的物理核/NUMA/SMT、cgroup、锁页/预触页、SHM/快照/投影/并存实例和磁盘预算真实准入；物理页只计一次，额外发布或跨 NUMA 画像不能借用未核算容量 | INV-5/7/11/14，P1/P5 |
| I-64 | 默认与同步画像各测完整明文行情至 socket 接收的分布，并覆盖本地输入闭包/flush/镜像等待；按负载记录全部尝试、过期、拒绝、背压和丢样，不相加分段分位数 | INV-5/7/11/14，P5 |
| I-65 | 72 小时共置长跑覆盖 32 策略实例、规定突发、发布/回滚、慢消费者、复制/磁盘扰动及恢复；有界恢复负载之外或证据不足保持冻结，不因 30 秒目标到期强制放行 | INV-1～14，P4/P5 |

Property tests 生成的是有效业务事件及明确标记的异常事件，不随意给同一 TCP 连接注入字节级乱序。TCP 分片/合包与多个独立连接的业务乱序是不同的测试维度。

### 2.1 交易所协议契约

每个 adapter 保存脱敏的原始样本、预期领域事件、适用环境、来源日期/schema hash、关键边界用例。订单/成交里的账户身份须脱敏，API key、签名、认证头永不进入测试录制。

必须覆盖 Spot JSON、Spot SBE WS、候选 FIX SBE、Futures public/market/private 与交易 WS。现货 FIX SBE 的 SOFH/消息头、SBE null 删除语义、不同 API 的 SBE schema 分开建 golden fixtures。JSON 增加未知字段可按兼容规则忽略，但缺少必要字段或未知枚举不能默认变成“无风险”。

官方测试环境与生产的 schema/权限不完全一致时，保存两套 fixture 与 conformance 结果。任何未验证的协议候选都不能自动被部署配置选成生产主通道。

### 2.2 v0.3 来源与覆盖映射

下面的设计文档定义行为，配套 contracts 固定字段、数值和测试输入。实现用同一用例覆盖共同要求；来源中的 RF/CFG 编号用于追踪，不另计为新增测试。

| 来源 | 配套契约 | 对应验证 |
|---|---|---|
| [持久化与接管](design/replication-and-failover.md) | [replication-profile-v1.json](../specs/contracts/replication-profile-v1.json) | U-24/25；RF-01、RF-02→I-07，RF-03→I-28，RF-04→I-38/47，RF-05→I-44，RF-06→I-45，RF-07、RF-08→I-43，RF-09→I-16，RF-10→I-08，RF-11→I-27，RF-12→I-12，RF-13、RF-14→I-41，RF-15→I-42，RF-16→I-46 |
| [IPC ABI](design/ipc-abi.md) | [ipc-wire-v1.json](../specs/contracts/ipc-wire-v1.json)、[ipc-vectors-v1.json](../specs/contracts/ipc-vectors-v1.json) | U-15～17、U-26/29；I-19～23、I-29～36、I-39、I-48/49/56；全部编码与语义向量进入回归集，新增向量不得只做 hex/hash 自洽检查 |
| [风控计算](design/risk-calculations.md) | [risk-vectors-v1.json](../specs/contracts/risk-vectors-v1.json) | U-01～03、U-06～14、U-18～23；I-10/11/14/15/40/61/62；十进制字符串按精确单位解析，向量预期不能由待测函数自己生成 |
| [配置文件与控制 API](design/configuration.md) | [system-config.schema.json](../specs/contracts/system-config.schema.json)、[control-api.openapi.json](../specs/contracts/control-api.openapi.json) | CFG-01→U-27，CFG-02→I-51，CFG-03→I-50，CFG-04→I-52，CFG-05→I-53，CFG-06→I-54，CFG-07→U-28/I-29/60，CFG-08→I-61，CFG-09→I-59/60，CFG-10→I-59，CFG-11→I-60，CFG-12→I-31/49/62/63，CFG-13→配置阶梯模型及 I-52，CFG-14→I-55，CFG-15→U-29/I-56，CFG-16→U-30/I-57，CFG-17→U-30/I-58 |
| [部署与完整链路 SLO](design/deployment-and-slo.md) | [deployment-reference-v1.json](../specs/contracts/deployment-reference-v1.json)、[reference.toml](../config/reference.toml)、[production.template.toml](../config/production.template.toml) | I-19/28/31/41/46/63～65；配置/容量静态核算与目标主机真实准入分别保存结果；生产模板按预期拒绝占位值 |

对配置成功的观察必须同时保存控制完成记录、固定 target membership、目标 runtime receipt、desired/effective 文件字节与持久收据、预期 projection/policy，以及实际 boot/slot/locator generation。仅从 API 状态字符串推断成功，或仅比较所有目标相同的全局 hash，不能验证权限投影和本地持久状态。

性能资格按部署契约执行：预热 10 分钟、基线至少 1 小时，每个 p99.9 分组至少 1,000,000 个有效样本。默认完整路径目标为 p99 ≤ 300 µs、p99.9 ≤ 1 ms；同步画像分别为 2 ms、5 ms。记录 `t_plain→t_socket_accepted` 和 `t_risk_done→t_commit`，附负载、画像、拓扑、构建、时钟校准与全部失败/等待统计；跨机单调时间不直接相减。I-65 的 30 秒恢复目标只适用于契约中的 4,096 个活动订单、64 个 Unknown 和 100,000 笔待核查成交及可用查询条件，异步缺尾部的预期结果仍是冻结。

## 3. vibe-tla 工作方式

使用本机的 [vibe-tla 技能](/home/obscure/.codex/skills/vibe-tla/SKILL.md)。它要求每个模型转换都锚定实际 `file:line`，因此当前不把设计伪代码当生产源码建模。

当 P1 出现实际 Rust 状态机后，在 Agent 中执行以下技能命令；这些不是 shell 命令：

```text
/vibe-tla init /home/obscure/Tickwire
/vibe-tla check order risk reservation --effort high
/vibe-tla review
/vibe-tla coverage order
```

指定本地仓库路径，避免全局安装的技能误用其他 active repo。模型工作区为该仓库的 `tla/`。初始化会创建 PROFILE/MEMORY/LESSONS/ROADMAP，并验证 Java/TLC 工具链；当前交付没有运行这一步，也没有声称完成 smoke test。

以后触及已建模逻辑时执行 `/vibe-tla sync <component>`；新阶梯层使用 `advance`，中断工作用 `resume`，发现真实问题后走 `review → fix → 重跑`。candidate 不等于 confirmed；独立 review 必须尝试推翻反例的代码可达性和环境前提。

### 3.1 订单/风险阶梯

| 层 | 增加的真实行为 | 重点性质 | 初始有限域建议 |
|---|---|---|---|
| L0 | 单写 reducer：预留、接受、拒绝、成交、撤销 | 额度守恒、终态合并、数量单调 | 2 个订单、2 个资产、数量 0～2 |
| L1 | 私有流与 ACK 的不同到达顺序、重复成交 | 最多一次经济入账，ACK 不回退状态 | 2 个连接来源、1～2 次重复 |
| L2 | 传输可能已发、超时、取消与成交竞争 | Unknown 保留潜在敞口，不自动重发 | 2 个订单、1 次连接故障、1 次取消 |
| L3 | enqueue/flush/send/crash 的真实边界 | 默认本地提交、可选指定镜像提交；崩溃后意图可枚举，不完整副本不能自动接管 | 两种画像分别建 cfg，有限日志槽与 ID 区间；数据域不引入 Raft |
| L4 | 旧发送者、实际隔离、查询/流合并和恢复 | 单活、恢复屏障、未决状态不误释放 | 2 个 owner、1 次接管、有限历史页 |

这里的数量是建模起点，实际边界必须按源码确认。每增加一个故障维度先证明动作确实可达，再扩大有限域，避免靠过强前提把错误路径排除。

### 3.2 多进程与多策略的必需阶梯

| 阶梯 | 增量层次 | 关键断言 |
|---|---|---|
| `book_generation` | snapshot 桥接 → 缺口 → 来源轮换 → 快照并发加入 | 只有连续、有效且来源明确的行情用于策略 |
| `budget_lease` | 单授权 → 多 exec/策略 → 失联 → 窗口/协调器重启 | 资金/请求额度不重复授予，过期授权不能消费 |
| `broadcast_ring` | 发布/释放 → 多读者 → 借用缺口 → 动态 gating | 最后访问结束前槽位/payload 不被复用 |
| `bank_rotation` | Prepare/Freeze/Activate → 迁移 → 中途崩溃 → banks 耗尽 | S 是连续完整发布边界，新旧 gating 独立，S+1 无重复遗漏 |
| `membership` | JoinSeq/快照 → Shadow/Active → 退出 → PID/instance 变化 | 不读已复用历史，不把迟到 ACK 或超时当成释放证明 |
| `strategy_handoff` | 撤旧权 → 一致快照 → 追平 → 原位接管 → 回滚 | 订单/持仓/预留归属守恒，旧代次不发新意图 |
| `multi_exec_activation` | 全体准备 → 部分提交 → 放行 → 失败/重启 | 缺少全体证据时禁增险，回滚不复用授权代次 |
| `intent_delivery` | SHM 提交 → Received/Accepted → ACK 丢失 → 通道换代 | 原意图可查询去重，传输成功不被当成交易所确认 |
| `guardian_fencing` / `exec_fencing` | exec 失活 → 独立应急授权 → 持续隔离证明 → 取消/核查 → 新主接管 | 无双主，租约到期不代替实际隔离，旧在途请求与条件子单不被遗漏 |
| `cross_product_hedge` | 双腿成功 → 一腿拒绝/未知 → 补偿/发布交接 | 未对冲敞口不提前释放，补偿不突破声明上限 |
| `local_commit` | L3 具体化：Prepared → 完整闭包/批次同步 → Applied → 最终发送门禁 → 崩溃 | 本地未提交不发送；提交后缺少发送回调也按潜在已发恢复；I-07/12/43 |
| `async_replica_promotion` | received/durable/applied 分离 → 断连/原盘恢复 → 原盘永久丢失 → 源封口证明 | 备机水位和低延迟不证明完整；缺尾部保持 EvidenceLost/Frozen；I-28/42/46 |
| `sync_mirror_commit` | 镜像指定 → 双端闭包同步 → ACK 丢失 → 原机故障/镜像替换 | Accepted/发送要求同身份、assignment epoch、前缀和闭包；旧 epoch 封口，新镜像就绪前不增险；I-41 |
| `dependency_closure` | 输入 sequencer/检查点 → manifest → pin → 快照安装/压缩/释放 | 缺闭包不确认 durable；活意图/Unknown/待安装快照引用不被回收；U-25、I-38/47 |
| `control_ready_persistence` | Ready 排序 → 条目/HardState/快照同步 → 肯定响应 → barrier/apply | 持久完成不可越过缺口；旧任期多数不能单独签发新提交证明；I-44 |
| `control_membership_joint` | learner 追平/重启核验 → joint → 分区 → leave-joint → 旧成员退役 | joint 同时满足新旧多数；完整配置状态与快照边界一致；I-43/45 |
| `config_materialization` | control commit → desired durable → ConfigApply/runtime → effective durable → 全目标完成；插入崩溃/重试 | Succeeded 蕴含全部匹配收据；运行时先应用后的文件失败不被当成功，旧 revision 不复活权限；I-50～54/61 |
| `config_scope_projection` | scoped-patch → mutation/dependency 授权 → 固定 delivery → NoOp 与权限投影 | 读取/写入权限不因传播全局 revision 扩大；隐藏目标仍计入完成集合；U-29、I-55/56 |
| `config_successor_cutover` | planned slot → 一次性 SuccessorBound → 旧实例撤权/退休 → staging → owner cutover | 仅计划内新 boot 完成原目标；旧新 effective 与发送资格不混用；U-30、I-57 |
| `config_root_migration` | 唯一写者 quiesce → Prepared/Copied → locator 持久切换 → Activated → 旧根退役 | 一个 locator generation 对应一个可写根；未确认目录同步保持冻结，旧 root 不自行恢复写权；U-30、I-58 |
| `risk_baseline_fee_pool` | 共享收费池/预留 → 多笔成交与迟到费 → 基线覆盖 → 重复票据 | 真实资金与费用责任守恒，影子额度只在闭合证据下释放；U-20～22、I-40 |
| `delivery_settlement` | 到期窗口 → 条件子单/取消 → 结算延期 → 账户事实闭合 | 时间/公开价格不释放预留；净额结算不重复入账，未闭合差额禁增险；U-23、I-62 |

这些都是拟建阶梯，当前没有对应的已执行模型结果。先按源码拆出真实原子边界，再以两个目标、两个 revision、一次崩溃/重试、两个实例或根目录等有限域起步；配置读取隔离还需真实 API/FD 权限测试，模型不能代替字节级检查。实现顺序可以按依赖推进，不能将必需阶梯标成“以后支持”却宣称系统已经满足多策略与独立发布要求。

### 3.3 建模与证据要求

每个 `.tla` 顶部建立变量、源码位置与含义的映射，列出源码中的原子步骤及真实异步边界。网络模型允许断连、延迟、独立通道乱序和重复业务回报；不假设同一 TCP 字节流可以随意重排。磁盘模型明确区分页缓存与 durable watermark，允许丢失未提交尾部，但模型必须证明发送依赖已提交水位。

至少校验以下蕴含式的源码对应版本：

```text
PossibleNewOrderSend(o) => RiskWasReserved(o) && RecoveryManifestCommitted(o)
UnknownPotential(o)    => PotentialExposureStillAccounted(o)
EconomicApply(fill)    => fillKey was not applied before
RiskIncreasingSend    => Ready && MarketValid && AccountValid && ConfigConsistent && IntentFresh && AuthorizedGeneration
ReuseSlot(slot)       => EveryPotentialReaderReleased(slot)
ActivateBank(B)       => B.FirstSeq = Frozen(A).LastSeq + 1 && InitialGatingInstalled(B)
ActivateStrategy(g)   => HandoffEvidenceComplete(g) && OldGenerationRevoked(g)
TransferOwnership    => OrdersPositionsReservationsConserved
CrashSinceReconcile   => NoRiskIncreasingSend
NewOwnerCanSend       => OldOwnerActuallyFenced && ReconcileComplete
Accepted(o)           => LocalClosureDurable(o) && Applied(o) && (SyncReplica => DesignatedMirrorDurable(o))
AsyncPromotion        => CompleteSourcePrefixProven && OldOwnerActuallyFenced
EvidenceLost          => NoRiskIncreasingSend
ConfigSucceeded       => EveryFixedTargetHasMatchingRuntimeAndDurableFilesAndProjection
ConfigReceiptAccepted => ExpectedIssuerBootSlotAndLocatorGenerationMatch
ProjectionDelivered   => EveryDeliveredFieldAllowedByBoundPolicyAndTarget
SuccessorCanApply     => PlannedSuccessorBound && BoundProjectionMatches
SuccessorCanSend      => PredecessorRightsRevoked && RecoveryAndAuthorityReady
ConfigWriterEnabled   => ExclusiveWriter && VerifiedActiveLocatorAndManifest
ReleaseSettlementRisk => AccountSettlementFactsClosed && PendingOrdersResolved
```

模型中“冻结最终解除”的活性性质只能在查询最终可用、私有事实/历史最终可获取、writer 最终推进等明确环境公平性条件下检查。安全性质不依赖超时后“假设已经没事”。未知的交易所最终性不得作为本地代码的保证。

TLA+ 的状态模型不替代 Rust 内存模型、硬件原子行为或真实操作系统语义的验证。ring 的 release/acquire 和成员缓存另用 loom/编译目标压力测试；mmap、封印、pidfd 和 fork 约束必须运行真正的多进程用例。

每层交付 `00_input.md`、`01_reasoning.md`、真实 `.tla/.cfg`、`03_run_log.md`、必要的反例及结论文件，并维护阶梯 PLAN/ROADMAP/TODO。记录 TLC 版本、源码 commit、模型 hash、每个 cfg 的精确命令、状态数/深度/运行时间、覆盖和结论。每个提交 cfg 都要执行；对非违规基线运行 coverage，确认每个预期 action 触发。

真实反例应转成确定性的 Rust 回归用例；如果模型假设与源码不一致，修模型并明确记录 refuted，不能靠禁用检查消除失败。修正后同时重跑相应 Rust 测试、所有相关 cfg 和源码锚点核验。

## 4. CI 与合并门禁

设计阶段先运行现有产物检查。Python 3.11 及以上环境需要 `jsonschema`，完整接口检查还需要 `openapi-spec-validator`：

```bash
python scripts/verify-design-contracts.py --openapi
```

记录 TOML/schema、OpenAPI、IPC 布局与向量、风险数值和跨文档容量/期限检查的实际输出，并确认生产模板仍按预期被拒绝。该命令检查设计产物；不能用它充当 U/I 的 Rust 实现、真实配置 API、文件持久化、进程通信或 TLC 证据。

P1 建立 Cargo workspace 后，才启用下面的实际工作流；当前仓库没有 Cargo manifest，不能把“命令没报错”或“没有发现测试”作为通过。

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

本地协议与共享内存进程集成测试纳入默认无凭证测试集；官方测试网测试采用独立显式任务，不能在正常开发测试中偶然访问生产。Miri/loom/fuzz 按受影响组件执行并记录工具链，而不是强行让所有网络依赖跑在 Miri 上。

TLC 使用技能自带 `run_tlc.sh` 的固定版本和显式 `.cfg`；工作流运行实际模型后，再用 `anchors.sh check tla` 检查代码锚点。只运行 anchors 不等于重跑模型，只有内容哈希也不能识别未锚定调用者的语义变化。PR 评审必须检查所有影响状态转换的上下游改动。

为 CI 固定技能脚本与 TLC 发行版的版本/校验和，不能依赖某个开发者的绝对 home 目录。模型、cfg 和 anchors.lock 纳入版本控制；Java/TLC 二进制和搜索状态目录按工具链缓存处理。CI 不自动调用付费模型进行语义评审。

每个 PR 提供测试用例 ID、实际输出与失败数、受影响不变量、模型结果和性能变化。测试数量为零、测试被全部忽略、只运行无关测试、TLC 超时/未完成、关键 action 从未触发，都不能算完成门禁。

## 5. 生产资格证据包

- P0：双产品协议/schema、IPC ABI 与黄金向量、配置 schema/API/投影与生产输入、风险数值向量、每类身份/代次/完成点、两种提交画像、完整链路 SLO 和容量清单。
- P1：真实独立进程的 mmap/权限/广播/点对点链路、slot/borrow/gating/切换故障矩阵、相关 TLC 与动作覆盖。
- P2：单元/性质测试、双产品交易协议、共享账户/策略归属、STP、条件结构、回报恢复和持久意图记录。
- P3：正常、失败、回滚的策略发布演练；exec 不重启的证据；订单/持仓/预留连续性；配置运行时/文件双收据、固定集合、权限投影、计划内 successor 与目录迁移证据。
- P4：两种画像的持久化/复制 crash-cut、异步缺尾部冻结、控制 Raft 持久顺序与成员变更、旧 sender 持续隔离、guardian 应急取消、主备与异机恢复、备份实际恢复。
- P5：参考容量与 72 小时共置长跑；两种画像分别报告真实多进程、多消费者、发布/故障并发下的完整延迟分布、吞吐/分配/内存/缺口；测试网、生产账户能力与受控上线记录。

CI 的普通功能任务在无生产凭证环境运行；硬件/内核相关资格任务在声明的目标环境执行，不能以线程模拟器或跳过测试代替。高强度压力和长期运行记录关联同一不可变构建。

设计稿完成与生产资格通过分别登记。没有实际运行输出的条目保持待实现；不能从本方案的完整程度推断这些测试或 TLC 已经通过。
