# 声明重建：输入窗口、同进程安装与发布

接口对应 `tickwire-release::rebuild`、`tickwire-strategy-sdk::replay::rebuild`、`tickwire-strategy-host::{capture,rebuild}` 与 App 生命周期装配。固定窗口、真实工厂预热、原进程持久安装、后窗连续消费和独立回退资格分别产生可核对的事实；App 在实际账户历史、当前 READY 与协调器签名一致后推进发布。

`Plans` 的单一根覆盖发布操作的完整有序成员列表，各成员精确绑定目标实例、代次、时钟、binary、工厂、schema、配置和模型。`Contract` 固定输入范围、行情类别、已知价位、预热数、逻辑时间跨度与记录/字节预算。`Inputs` 保存实际 MD 资源与检查点、完整账户基线/记录和本次窗口的输入顺序；不借用旧实例的虚构检查点。要求旧跨来源处理顺序时明确拒绝。

SDK 从规范账户记录重建隔离 AccountReplica，从真实快照检查点恢复 MarketReplica，再调用已匹配工厂的 `begin`，用 Application/Engine 执行输入和真实策略回调。每个行情类别按已应用记录序号计算预热次数，重复 callback 不增加有效观察数；Unknown 不当作已知零数量。最终检查实际 cursor 向量、逻辑时间、覆盖和预算。

首个动作契约仅接受有完整 Admit 命令证明的固定经济动作集合，并要求覆盖账户前缀中该逻辑策略的全部已接纳动作。SDK 比较原经济动作键、逻辑策略和命令完整字节，逐次解码实际业务检查点，要求这些动作仍为 consumed。遗漏动作、缺记录、重复键和预热窗口新出现的未列入 Admit 均拒绝；Unknown、终态或中止均不重置已接纳动作。预热输出接口关闭，实际回调尝试输出命令时失败。该实现没有根据缺失记录推定“动作未发生”的分支。

Candidate 保存实际输出检查点、输入与回调顺序。`persist` 先同步八类内容对象，再同步完成对象；任一步返回错误都不产生 `Persisted`。只读对象和本次持久计算结果不含进程资格或增险许可。认证 READY、C 后接续与发布由下述同进程安装路径负责；进程重启保留事实并关闭执行资格。

### 保留真实预热实例与安装历史

`execute_running` 与既有 `execute` 共用同一预热执行器，返回不可反序列化的 `RunningCandidate`，持有刚完成真实回调的 Application/Engine。既有内容计算入口显式丢弃运行实例，保留原返回类型；运行安装入口不重新调用 begin、restore 或预热回调。解除预热输出关闭包装时只移动同一 Application，原 Source、RuntimeState、cut、完整行链、字节预算、计时器与 RNG 均保留。

`RunningCandidate::persist` 先保存原完成闭包，再同步空 genesis commit 和指向它的完整 warm delta commit，返回持有同一引擎的 `PersistedRunning`。它公开原 anchor、genesis/末尾 commit、完成根和 HEAD 内容供安装收据绑定。`Live::from_rebuilt` 接收该实例，核对实际市场与账户集合并同步 HEAD；不重设序号、不重放账户 Baseline，也不授予输出权限。下一条真实输入从 warm 末尾序号加一开始，后续 delta commit 必须以前一持久 commit 为前驱。任何持久失败都不返回可继续运行的实例。

静态注册工厂新增显式、默认拒绝的前向重建方法。只有已认证安装请求及完整固定窗口验证通过后才调用 configure；参数、模型、factory ID、状态 schema 和实际 binary 必须与已验证输入一致。重建策略由 `registered::Running` 包装并保留原 Configured 工厂，供后续恢复/迁移使用。Counting 示例的前向重建复用既有动作证明逻辑，已接受的经济动作保持 consumed；没有接受证明不能初始化为可重发。

原 schema3 生命周期继续保留自己的回放与安装语义。开发期间执行定向和进程回归，新增形式化验证统一留到功能开发完成后。

回退资料现支持两种显式方法：旧版本精确恢复，以及独立批准的旧版本重建。前者先用旧工厂回放到 C、逐字节比较完整检查点，再真正恢复 C；后者以旧合同和旧参数运行实际预热回调。两者都核对该逻辑策略的完整已接纳动作集合，并保留输出关闭。具体对象、错误方法和缺资料测试记录于 `rollback-plan-*`、`rollback-exact-*`、`rollback-declared-*` 日志。

`replay::rebuild::probe::run_with` 已提供独立探测执行器。它先验证完整 operation/member、固定对象、实际旧 binary 和文件，再持久写入一次性 attempt，调用真实工厂，核验固定切点，依次同步输入对象、anchor、完整回调记录、输出检查点，最后写 `REBUILD_PROBE.json`。已有 attempt 不重新运行回调，完成标记写入失败保持关闭。静态注册入口新增 `--rebuild-rollback`，未注册对应工厂行为时拒绝。

该入口已有真实独立子进程测试，父进程检查实际 PID、成功退出与同步输出；这项测试使用编译进测试可执行文件的确定性工厂。Host 已接实际 Registry 身份、成功退出、同一 attempt 与完整输出重读，并有针对真实子进程的测试。同进程目标安装通过下述 kind9/kind10 与独立探测完成件衔接。

回退探测的 `Job/Receipt` 与 Host 的成功退出 `Completion` 由 `tickwire-release::rebuild::probe` 共享，既有规范编码保持兼容。恢复入口 `Verified::verify` 从实际 Host Completion 根开始，核对固定 Operation/member、forward 输入、rollback Plans、attempt、probe 身份、PID 以及完整 anchor/transcript/checkpoint/cut；全部 Job 对象和策略输出按摘要读回，类型对象限制 16 MiB，二进制等不透明依赖沿用 64 MiB，最多 512 对象、总计 256 MiB；递归旧版本图深度限制为 16。Job 中原文件路径只作为已承诺内容保留，恢复不访问这些路径、不调用用户工厂。历史成功字段只说明已保存的 Host 事实；新执行资格仍来自当时真实持有的子进程与成功 ExitObservation。


schema4 的独立 `RebuildPrepared`、`RebuildObserved` 和 `RebuildAdoption` 已接入真实 Coordinator 的 WAL。结果绑定完整原 Prepare、当前挑战、operation/member、七个固定对象根、实际 cursor 向量、安装后 READY、gate/state 版本和成员时钟期限。旧 Applied Prepared 不能使该版本进入 PreparedAll；两域结果都持久保存后才生成绑定 adoption 的 Commit。刷新只推进实际消费证明，固定窗口、预热、安装和回退探测根保持不变。

协调器恢复先发新 Query。保存的历史完成结果通过 RebuildObserved 取得，全部查询完成且显式 resume 后再发新的 Prepare 刷新；过期历史结果不直接成为新 Commit 资格。当前协议测试包括准备结果同步失败、不重复采纳、丢失采纳结果后恢复、真正 Commit/Release 后再恢复，以及 gate、状态、成员、挑战和根替换拒绝。

`MemberAuthority.readiness` 为可选值。schema1–3 仍要求 Some，已有 JSON 和签名字节不变；schema4 可以没有初始业务 READY，安装结果始终要求真实 READY。App 对 schema4 使用独立重建分支，取得实际安装、独立回退完成与当前 READY 后才响应 RebuildPrepared；普通恢复分支仍拒绝这类操作。

## 输入窗口采集

`replay::rebuild::capture::CaptureOnly` 持有隔离的行情和账户视图，使用 SDK 固定观察器核验输入，不接受或执行用户工厂。创建时绑定 capture 身份、binary、逻辑策略、账户风险域、时钟域、配置和模型摘要；行情资源的 `host_boot_id` 必须与采集时钟域一致，当前只接受 `clock_domain=1` 的本机单调时间；跨域时间不能用于新鲜度计算。种子只能包含真实行情检查点和完整账户前缀，`warm` 必须为空，采样时间必须等于窗口起点。要求旧进程处理顺序的合同保持拒绝。

`push` 接收同一时钟域中单调的行情应用、行情观察标记和账户记录。它复用真实行情和账户解码、连续序号、资源身份与已接纳动作集合检查，按合同限制时间跨度、记录数和完整输入对象字节数。每次先验证候选视图及窗口，再整体发布；任何错误均保留上一有效窗口，后续正确输入仍可继续。配置、模型、用户定时器和额外外部输入在本采集入口返回 `Unsupported`。

`finish` 消费采集 owner，拒绝空窗口、跨度不足、过期或不连续视图，以及未达到每类最小观察数的窗口；同一已应用记录的重复观察标记只计一次。成功返回实际 `Inputs` 和 `window::Seal`，绑定初始/最终 cursor、合同、种子、完整输入、逐行输入哈希链及账户输入摘要，不构造用户检查点、READY 或交易许可。实际进程采集使用下述独立协议；发布操作在输入窗口封口后固定。

## 独立采集进程协议

`registered --capture-only` 使用独立 `capture::Bootstrap`，只建立已批准 MD JOIN 和执行拥有的账户 report 通道，不创建 intent 通道，也不调用用户工厂的 configure/create。启动绑定窗口 Identity、Contract、目标配置/模型、初始账户位置、报告资源与输出目录；Host 注册真实 PID 后才开放 MD 准入。实际 Baseline 与完整账户前缀恰好收到初始位置后，以同一真实 consumer.applied 的当前市场副本建立 seed。后续账户记录属于 warm 输入，不能在封口时改写 seed。

认证 report 41/42 的 kind8 为 CaptureSeal。请求对象固定 capture ID、完整有效 Bootstrap 摘要、Contract、最终账户位置与期限；`expected_sequence` 为该账户位置（0 合法），`expected_cut_digest` 为 Contract 摘要，revision 固定 1。Host 发出封口后停止发布超过该位置的账户记录。worker 已越过位置则明确拒绝；未到位置继续消费到精确位置。资格尚未满足保持采集，达到窗口边界后才封口。拒绝使用 flags18 与既有 Unsupported/Busy/Invalid；认证错误、资源失效或期限届满关闭进程。

成功前同步一次性 attempt、完整窗口对象与完成标记，再将实际 Bootstrap、SealRequest 及绑定实际 PID、请求、报告 ring cursor 和真实市场 applied 的 Receipt 保存到同一输出目录；文件 I/O 后重新采样绝对单调期限与会话，最后发送成功对象。输出目录不允许重启覆盖。封口后同一 PID 不再消费数据，继续验证控制会话和截止时间，重复同请求只返回原收据；在有效 kind9 安装请求到达前没有业务对象、READY 或交易输出。窗口闭包由固定观察器重算验证；文件内容校验不替代 Host 的真实子进程身份与报告 cursor 核验。

### 窗口重算与持久化

`capture::VerifiedWindow` 从明确提供的 identity、Contract、空 warm 种子、完整 Inputs 和 Seal 开始，重新运行 SDK 固定观察器。逐行检查时钟、资源、序号、动作集合、观察覆盖与预算，重算完整起止 cursor、输入链和账户摘要；仅结构合法或摘要自洽的 Seal 不能通过。`validate_plan` 进一步核对 Plan 的目标身份、成员风险域和时钟、合同、输入与终点；它不代替完整 Plans/Operation 的签名与授权校验，capture ID 与目标 input_stream 保持不同语义。

`CaptureOnly::qualified` 在不消耗 owner 的情况下执行与 finish 相同的封口条件，供实际采集进程决定何时请求封口。`store::Attempt::begin` 在采集前创建新的 0700 输出目录并同步 ATTEMPT.json，绑定 identity、合同与种子；已有目录一律拒绝，失败尝试不覆盖或重跑。完成时先重算窗口，再同步合同、种子、完整输入与 Seal 四个有界类型对象，最后同步 CAPTURE_WINDOW.json。任何写入或同步失败均不返回完成结果。每个对象最多 16 MiB，完整闭包仍受恢复总预算约束。

`store::read_back` 核对预期完成对象、目录中的 attempt 和全部对象长度/摘要，再执行同一完整重算；缺对象、替换 attempt、修改 Seal 或漏行均拒绝。内容结果不认证实际 PID、控制挑战、报告游标或当前期限，这些事实由 worker/Host 的认证会话核对。App 只消费认证 owner 产生并绑定完整固定操作的重建结果。

### Host 采集 owner

Host 的独立 `capture::Owner` 持有真实 Registry 子进程，只分配 report bank 并登记该 PID 的 MD Admission。启动模板经实际报告描述符、父进程身份和私有控制路径装订后才计算 Bootstrap 摘要；没有 intent lane、用户工厂或业务 READY。调用方提供明确批准的策略二进制与身份，Host 仍核对可执行文件摘要及内核 peer credentials。

封口固定实际 `ReportWriter::last_position`，成功排队后禁止继续发布账户记录。认证 kind8 回复必须精确匹配挑战、窗口、合同、请求位置、代次和期限；Host 按有界 CAS 对象读取 Receipt，再重算整个 WindowSeal，核对实际 report applied/released 游标覆盖该报告位置。Host 还从 worker 输出 CAS 重读 Receipt 引用的 Bootstrap 与 SealRequest，按长度/摘要及实际发送的规范字节逐项比较；缺任一根即拒绝，不以内存对象补成持久完成。文件读取完成后重新检查当前时钟、会话及 Registry 持有的同一存活 PID，才产生 `Captured` 事实。仅存在完成文件、进程曾存活或游标尚未应用均不成功。

任何协议、读取、身份、资源或期限错误使本次 owner 进入不可恢复的失败态并终止、回收子进程，之后不接纳回执或发布报告。成功封口后保留同一子进程等待后续阶段；正常 `stop` 通过 report QUIESCE 要求 worker 先真实 detach MD，再返回报告 cursor 的 Applied；Host detach report 后等待同一子进程成功退出。单纯 kill 或 EOF 不算正常退休。Drop 由 Registry 杀死并回收未退出子进程，异常死亡不会伪造 MD detach。该采集 owner 不授权交易；提升为运行实例及发布由 `rebuild::Hosted` 与 App 负责。

Capture 模式沿用 Host 已有 qualified-SHM launcher 的调用者安全契约，`Owner::launch` 显式为 unsafe；它不把任意 LaunchSpec 当作安全凭证。报告 FD/cursor 建立复用从 remote launcher 提取的原接缝，没有新增底层映射访问。

## App 成员发布与执行责任

`HostConfig.capture` 显式指定采集 Bootstrap 和 0700 publication 目录，普通 Bootstrap 保留为最终运行模板。该目录保存 CAS 对象及 `forward-files.json`、`rollback-files.json` 两份部署位置表。批准的 owner 先发布 `capture-seal.json`（schema、capture ID、精确账户位置）；Host 按实际报告位置封口并写出完整窗口后，协调器才固定 Plans 和签名 Operation。Prepare 不临时选择或改写输入窗口。

schema4 的初次 Prepare 先验证已封口窗口、完整 Plans/清单/回退计划及真实 Capture owner 事实。窗口的账户 baseline 和全部规范记录必须是当前账户 owner 的逐字节前缀；固定动作集合绑定该逻辑策略全部已接纳命令。窗口之后出现未列入合同的新 Admit 时，本操作不能进入 Prepared，调用者须重新采集并批准新的操作。已有订单的确认、成交、终态与费用变化只通过真实账户回报推进目标实例，不改写固定窗口或动作集合。

App 随后关闭当前 handover gate，令旧、新实例停止命令转发，终止旧实例尚未发送的命令；已发送或 Unknown 意图继续占用原资金和执行责任。目标安装运行在原 Capture PID 内，保持实际预热引擎及其输入链；独立旧版本 probe 成功退出后，App 才核对目标的认证 READY、当前账户日志位置、账本序号、订单摘要与完整账户输入闭包。

每个成员的生命周期 WAL 保存原签名 Prepare、固定输入根和终点，再依次记录预热完成、目标安装、独立回退完成以及签名 RebuildPrepared。对象同步先于引用它的状态事件；阶段不能跳过，已记录事实不能删除或换根。同步失败不推进内存状态，恢复保留可审计事实并关闭执行门禁，不重跑用户工厂、安装或 probe。重复请求仅在同一进程会话及同一挑战下返回原完成结果。

后续 Prepare 刷新只取得同一目标进程的当前 READY，七固定根、窗口终点和原挑战保持不变。Commit 要求 Coordinator 精确采纳已持久结果，再检查当前 READY 与实际账户历史，向账户 WAL 追加唯一 StrategyHandover。Release 继续核对所有成员的真实 Committed 收据与签名 GroupRelease，旧实例正常退出后才启用新实例。新 Query 可报告保存的历史结果，重启或过期历史不直接恢复交易许可。

## 目标安装协议

封口后才能固定 schema4 Operation、完整 Plans 和发布 Manifest。安装请求逐项绑定原采集 completion、WindowSeal、目标运行 Bootstrap、operation/member 和绝对期限；目标 Bootstrap 保留原 MD/report 身份与游标，并增加执行端认证的真实 intent lane。Capture ID 与目标 input_stream 分别校验，不要求相等。安装控制使用 op41/42 kind9，固定布局不变，expected_sequence 为窗口 warm 行数，expected_cut_digest 为 WindowSeal 摘要；失败沿用 flags18/status1..3。

SDK 使用一次真实工厂调用得到窗口末端的运行实例，保留原始 anchor、warm 行链、账户视图、行情视图、计时器与 RNG。持久化先写空 genesis，再写引用 genesis 的 warm delta commit；末端 checkpoint 的 cut.sequence 保持 warm 行数。同步 HEAD 后写 Installation，对象绑定原请求、窗口、预热 completion、完整历史根、目标 Source、实际 PID 与完成时间。任一同步失败不生成安装成功。内容验证器核对完整对象图及每个原始输入；Host 另核对当前受监督的同一 PID、认证控制会话和实际 cursor。

安装请求只能选中一次；同一请求的重试不重新执行工厂或 warm 回调，不同请求拒绝。同一 worker 保留原 MD/report 通道进入持续循环，第一条新输入从窗口末端之后开始。已消费的账户基线和记录只用于恢复内部镜像，不再次调用用户策略。首次 READY 来自已同步的真实业务状态，即使账户没有新记录也能生成。发布前保持原门禁，安装收据本身不授权输出或交易。等待安装期间仍受固定截止时间、共享 ring 容量及连续性门禁限制；容量耗尽或缺口时失败，不能跳过数据继续。

## 同进程安装与持续接续

采集封口后的安装请求必须在原认证会话与期限内固定操作、成员、完整 Plans/Manifest、采集 Completion/Seal/Receipt 以及已装订的目标 Bootstrap。运行中的 MD JOIN、report bank、订阅、cursor 和真实 PID 保持不变；安装不得重新 JOIN、重复消费账户基线或调用会把 merged cut 归零的普通 `Live::new`。

worker 的持续循环接收已有资源与实际 `Live`，普通启动和声明重建安装共享该循环。注册入口只在完整安装请求通过后配置真实用户工厂，运行声明重建并保存原 anchor、完整 warm 行和最终 checkpoint/commit。实际实例、账户镜像、恢复输入与双方游标一致后才接入新 intent 通道；Host 通过内核 peer credentials 确认它来自同一 Registry 子进程，转移原 report/Registry 所有权，保持提交关闭。没有实际 intent lane 的采集完成不能作为目标发布成功。

安装后的 READY 必须来自实际业务检查点、真实账户 InputSeal、保留输入与已同步安装闭包。后续输入从 C 的下一行连续处理并持久；重复安装或 Prepare 不能重跑工厂。安装事实、READY 和交易授权分别核验，最终 Commit/Release 沿既有执行门禁与真实旧实例退休顺序处理。

### 重建后的恢复闭包

HandoverSeal v4 使用独立的 `rebuild_installation_digest`，初始 READY 可以尚无 `rebuild_rollback_digest`；既有 preflight/replay 字段保持为空，v1–3 编码不变。v4 保留原 warm 起点、完整 warm 行链和窗口后输入，当前业务检查点与完整账户报告逐字节核对。账户管理权交接、复制恢复和后续版本把该实例作为旧源时，要求独立回退完成根已存在并绑定同一 Operation/member。

回退探测完成后，Host 经原认证 report 通道发送 kind10（RebuildRollback）。请求固定原 Installation、真实独立探针 Completion、完整依赖列表和期限；worker 只验证并同步内容，不在当前实例执行旧工厂。同步后才写回执、更新 v4 根并生成新 READY。同一请求重试返回原结果，替换回退根拒绝。前向安装与回退图的类型对象上限仍为 16 MiB，完整恢复依赖沿用 256 MiB 总预算。

### Host 同进程提升的受信输入

`rebuild::Hosted` 沿用 qualified launcher 的调用者安全义务。启动时固定普通 Bootstrap 原字节与 Capture Bootstrap 的报告端点、真实父进程 PID/UID/GID、规范 report header 和 MD admission 路径；不在窗口封口后改写已批准 Bootstrap。报告与行情资源在采集期建立，意图资源仅在认证 kind9 安装后建立；第二连接核对原 Registry 子进程的真实内核身份，提升时转移原 Registry、report writer、控制会话与目录所有权。

私有 `publication_directory` 为规范绝对路径、0700。`objects.json` 是有界、按摘要唯一排序的对象索引，内容通过标准 CAS 按长度和摘要读回；`forward-files.json` 与 `rollback-files.json` 是按 Role 排序的实际部署位置表，全部绝对路径仍须通过固定 Manifest/回退计划的文件校验。安装请求还逐项绑定 StrategyConfig/RecoveryTemplate 的部署路径与目标 Bootstrap 实际读取路径；恢复模板只使用清单已认证的原始字节，不能在安装后另读一个未校验版本。它们不产生发布或交易权限。

协调器在签署依赖窗口的 Operation 前，先写 `capture-seal.json`（`schema=1/capture/account_position`）选择历史账户 cut。Host 按 10ms 冷扫描、拒绝符号链接和超限文件，只有实际已发布账户位置等于选择位置才请求封口；超过该位置即拒绝。真实 Captured 生成后方可构造 Plans/Manifest。公开受信 owner 也可直接调用 `seal_capture`；marker本身不证明窗口成功。

首次 kind9 完成核对真实 PID、报告游标、窗口、安装闭包后，普通远程监督继续取得真实 READY；预热实例、原始行情 Client 与报告游标一起交给持续循环，消费 C 后真实输入。相同 kind9 挑战重试只返回原持久回执，不重跑工厂。独立回退 probe 成功退出后，Host 用 kind10 将完整完成闭包同步回同一 worker，worker保留该依赖再发新的 v4 READY；最终交接必须包含该回退根。任一阶段失败保持关闭，不能由 Host 模拟 READY。

App 在每次冷安装、闭包同步和回退探测后的 fresh 单调时钟采样点复核实际账户 `valid_until`、冻结状态与发送 owner 状态。签署和发布 Prepared 前再次检查；即使操作租约仍有效，账户期限已到也必须当场持久 Halt，不得先发一份过期账户的 Prepared 等待下次主循环关闭门禁。同步冷步骤的耗时属于当前账户预算，实验室不延长该期限以取得成功。

### 实际进程实验：声明窗口安装与发布

`tickwire-lab spot-rebuild-custom` 独立验收 schema4。旧 counting-v1 策略经实际执行链准入一次订单并收到交易所 ACK；新 counting-v2 子进程只采集实际 MD/账户输入，窗口封口后才派生并签署 Operation、Plans、Manifest 与显式旧版本重建回退策略。实验室不得调用用户工厂生成完成收据。

签名协调器经 App 的真实 Prepare/Commit/Release 推进：同一 capture PID 安装窗口、持续消费后窗记录，独立旧二进制探针成功退出，kind10 绑定回退根后取得当前 READY。验收同时核对七个固定根、实际 PID/准入身份、v4 完整依赖闭包及安装 C 到最终 D 的哈希前缀。只有 Released 且旧子进程真实退出后，交易所才释放迟到成交；账户 WAL、复制前缀、策略报告和费用必须闭合。静态配置生成不产生窗口、安装、探针或成功证据。已有四个场景保持各自语义。

Host把输入闭包写入capture输出CAS后，在发送kind9前必须释放其独占FileBlobStore写锁；worker随后独占写入安装结果与回执。两个进程不得同时持有同目录writer，读回使用只读FileBlobReader。worker进入持续循环前亦释放该冷安装writer。

独立probe attempt由域分隔哈希（`tickwire.rebuild.probe.v1`、完整canonical安装Request与Installation根）派生，绑定revision/member/窗口与安装结果。派生值保证非零；相同作业不变，新revision或成员不会仅因共享operation_id复用同一个attempt。一次性probe注册仍拒绝重新执行既有选择。

恢复验证还遍历原采集 Completion→ATTEMPT/seed/Inputs/Seal 与 Receipt→Bootstrap/SealRequest 的全部边，核对原身份、PID、合同、完整种子、账户终点和期限，并从逐行 warm 输入重算 WindowSeal 的输入链。种子必须等于固定 Inputs 去除 warm 的原始起点；缺任一对象即拒绝，导出的 v4 闭包必须带齐这些原始资料。完整市场观察器重算仍在 SDK/Host 原采集核验执行，纯恢复层不重新调用用户工厂。
