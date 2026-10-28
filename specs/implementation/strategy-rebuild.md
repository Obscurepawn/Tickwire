# 声明重建：资料校验与 SDK 预热

接口对应 `tickwire-release::rebuild`、`tickwire-strategy-sdk::replay::rebuild` 与 `tickwire-strategy-host::capture`。内容校验、SDK 回调和真实子进程的输入采集已实现；目标安装、执行端发布装配及安装后的连续接续仍待完成。

`Plans` 的单一根覆盖发布操作的完整有序成员列表，各成员精确绑定目标实例、代次、时钟、binary、工厂、schema、配置和模型。`Contract` 固定输入范围、行情类别、已知价位、预热数、逻辑时间跨度与记录/字节预算。`Inputs` 保存实际 MD 资源与检查点、完整账户基线/记录和本次窗口的输入顺序；不借用旧实例的虚构检查点。要求旧跨来源处理顺序时明确拒绝。

SDK 从规范账户记录重建隔离 AccountReplica，从真实快照检查点恢复 MarketReplica，再调用已匹配工厂的 `begin`，用 Application/Engine 执行输入和真实策略回调。每个行情类别按已应用记录序号计算预热次数，重复 callback 不增加有效观察数；Unknown 不当作已知零数量。最终检查实际 cursor 向量、逻辑时间、覆盖和预算。

首个动作契约仅接受有完整 Admit 命令证明的固定经济动作集合，并要求覆盖账户前缀中该逻辑策略的全部已接纳动作。SDK 比较原经济动作键、逻辑策略和命令完整字节，逐次解码实际业务检查点，要求这些动作仍为 consumed。遗漏动作、缺记录、重复键和预热窗口新出现的未列入 Admit 均拒绝；Unknown、终态或中止均不重置已接纳动作。预热输出接口关闭，实际回调尝试输出命令时失败。该实现没有根据缺失记录推定“动作未发生”的分支。

Candidate 保存实际输出检查点、输入与回调顺序。`persist` 先同步八类内容对象，再同步完成对象；任一步返回错误都不产生 `Persisted`。只读对象和本次持久计算结果不含进程资格或增险许可。实际重启、认证 READY、C 后接续以及回滚策略的业务含义将在后续接线单独实现。

证据见定向报告。当前不修改 schema3 生命周期和正在运行的 Linux 场景，也未新增形式验证。

回退资料现支持两种显式方法：旧版本精确恢复，以及独立批准的旧版本重建。前者先用旧工厂回放到 C、逐字节比较完整检查点，再真正恢复 C；后者以旧合同和旧参数运行实际预热回调。两者都核对该逻辑策略的完整已接纳动作集合，并保留输出关闭。具体对象、错误方法和缺资料测试记录于 `rollback-plan-*`、`rollback-exact-*`、`rollback-declared-*` 日志。

`replay::rebuild::probe::run_with` 已提供独立探测执行器。它先验证完整 operation/member、固定对象、实际旧 binary 和文件，再持久写入一次性 attempt，调用真实工厂，核验固定切点，依次同步输入对象、anchor、完整回调记录、输出检查点，最后写 `REBUILD_PROBE.json`。已有 attempt 不重新运行回调，完成标记写入失败保持关闭。静态注册入口新增 `--rebuild-rollback`，未注册对应工厂行为时拒绝。

该入口已有真实独立子进程测试，父进程检查实际 PID、成功退出与同步输出；这项测试使用编译进测试可执行文件的确定性工厂。Host 已接实际 Registry 身份、成功退出、同一 attempt 与完整输出重读，并有针对真实子进程的测试。目标安装与完整发布服务仍待接线。


schema4 的独立 `RebuildPrepared`、`RebuildObserved` 和 `RebuildAdoption` 已接入真实 Coordinator 的 WAL。结果绑定完整原 Prepare、当前挑战、operation/member、七个固定对象根、实际 cursor 向量、安装后 READY、gate/state 版本和成员时钟期限。旧 Applied Prepared 不能使该版本进入 PreparedAll；两域结果都持久保存后才生成绑定 adoption 的 Commit。刷新只推进实际消费证明，固定窗口、预热、安装和回退探测根保持不变。

协调器恢复先发新 Query。保存的历史完成结果通过 RebuildObserved 取得，全部查询完成且显式 resume 后再发新的 Prepare 刷新；过期历史结果不直接成为新 Commit 资格。当前协议测试包括准备结果同步失败、不重复采纳、丢失采纳结果后恢复、真正 Commit/Release 后再恢复，以及 gate、状态、成员、挑战和根替换拒绝。

`MemberAuthority.readiness` 为可选值。schema1–3 仍要求 Some，已有 JSON 和签名字节不变；schema4 可以没有初始业务 READY，安装结果始终要求真实 READY。app 在真实目标安装与发布闭环接通前显式拒绝 schema4 执行，避免误走旧恢复分支。生命周期库 34 项单元测试与定向 Clippy 通过，app/lab all-targets 编译通过；该检查没有运行完整 Rebuild 发布场景。

## 输入窗口采集

`replay::rebuild::capture::CaptureOnly` 持有隔离的行情和账户视图，使用 SDK 固定观察器核验输入，不接受或执行用户工厂。创建时绑定 capture 身份、binary、逻辑策略、账户风险域、时钟域、配置和模型摘要；行情资源的 `host_boot_id` 必须与采集时钟域一致，当前只接受 `clock_domain=1` 的本机单调时间；跨域时间不能用于新鲜度计算。种子只能包含真实行情检查点和完整账户前缀，`warm` 必须为空，采样时间必须等于窗口起点。要求旧进程处理顺序的合同保持拒绝。

`push` 接收同一时钟域中单调的行情应用、行情观察标记和账户记录。它复用真实行情和账户解码、连续序号、资源身份与已接纳动作集合检查，按合同限制时间跨度、记录数和完整输入对象字节数。每次先验证候选视图及窗口，再整体发布；任何错误均保留上一有效窗口，后续正确输入仍可继续。配置、模型、用户定时器和额外外部输入在本采集入口返回 `Unsupported`。

`finish` 消费采集 owner，拒绝空窗口、跨度不足、过期或不连续视图，以及未达到每类最小观察数的窗口；同一已应用记录的重复观察标记只计一次。成功返回实际 `Inputs` 和 `window::Seal`，绑定初始/最终 cursor、合同、种子、完整输入、逐行输入哈希链及账户输入摘要，不构造用户检查点、READY 或交易许可。实际进程采集使用下述独立协议；发布操作在输入窗口封口后固定。

## 独立采集进程协议

`registered --capture-only` 使用独立 `capture::Bootstrap`，只建立已批准 MD JOIN 和执行拥有的账户 report 通道，不创建 intent 通道，也不调用用户工厂的 configure/create。启动绑定窗口 Identity、Contract、目标配置/模型、初始账户位置、报告资源与输出目录；Host 注册真实 PID 后才开放 MD 准入。实际 Baseline 与完整账户前缀恰好收到初始位置后，以同一真实 consumer.applied 的当前市场副本建立 seed。后续账户记录属于 warm 输入，不能在封口时改写 seed。

认证 report 41/42 的 kind8 为 CaptureSeal。请求对象固定 capture ID、完整有效 Bootstrap 摘要、Contract、最终账户位置与期限；`expected_sequence` 为该账户位置（0 合法），`expected_cut_digest` 为 Contract 摘要，revision 固定 1。Host 发出封口后停止发布超过该位置的账户记录。worker 已越过位置则明确拒绝；未到位置继续消费到精确位置。资格尚未满足保持采集，达到窗口边界后才封口。拒绝使用 flags18 与既有 Unsupported/Busy/Invalid；认证错误、资源失效或期限届满关闭进程。

成功前同步一次性 attempt、完整窗口对象与完成标记，再将实际 Bootstrap、SealRequest 及绑定实际 PID、请求、报告 ring cursor 和真实市场 applied 的 Receipt 保存到同一输出目录；文件 I/O 后重新采样绝对单调期限与会话，最后发送成功对象。输出目录不允许重启覆盖。封口后同一 PID 不再消费数据，继续验证控制会话和截止时间，重复同请求只返回原收据；在后续真实目标安装实现前始终没有业务对象、READY 或交易输出。窗口闭包由固定观察器重算验证；文件内容校验不替代 Host 的真实子进程身份与报告 cursor 核验。

### 窗口重算与持久化

`capture::VerifiedWindow` 从明确提供的 identity、Contract、空 warm 种子、完整 Inputs 和 Seal 开始，重新运行 SDK 固定观察器。逐行检查时钟、资源、序号、动作集合、观察覆盖与预算，重算完整起止 cursor、输入链和账户摘要；仅结构合法或摘要自洽的 Seal 不能通过。`validate_plan` 进一步核对 Plan 的目标身份、成员风险域和时钟、合同、输入与终点；它不代替完整 Plans/Operation 的签名与授权校验，capture ID 与目标 input_stream 保持不同语义。

`CaptureOnly::qualified` 在不消耗 owner 的情况下执行与 finish 相同的封口条件，供实际采集进程决定何时请求封口。`store::Attempt::begin` 在采集前创建新的 0700 输出目录并同步 ATTEMPT.json，绑定 identity、合同与种子；已有目录一律拒绝，失败尝试不覆盖或重跑。完成时先重算窗口，再同步合同、种子、完整输入与 Seal 四个有界类型对象，最后同步 CAPTURE_WINDOW.json。任何写入或同步失败均不返回完成结果。每个对象最多 16 MiB，完整闭包仍受恢复总预算约束。

`store::read_back` 核对预期完成对象、目录中的 attempt 和全部对象长度/摘要，再执行同一完整重算；缺对象、替换 attempt、修改 Seal 或漏行均拒绝。内容结果不认证实际 PID、控制挑战、报告游标或当前期限，这些事实由 worker/Host 的认证会话核对。App 在目标安装与完整发布闭环接通前继续拒绝 schema4 执行。

### Host 采集 owner

Host 的独立 `capture::Owner` 持有真实 Registry 子进程，只分配 report bank 并登记该 PID 的 MD Admission。启动模板经实际报告描述符、父进程身份和私有控制路径装订后才计算 Bootstrap 摘要；没有 intent lane、用户工厂或业务 READY。调用方提供明确批准的策略二进制与身份，Host 仍核对可执行文件摘要及内核 peer credentials。

封口固定实际 `ReportWriter::last_position`，成功排队后禁止继续发布账户记录。认证 kind8 回复必须精确匹配挑战、窗口、合同、请求位置、代次和期限；Host 按有界 CAS 对象读取 Receipt，再重算整个 WindowSeal，核对实际 report applied/released 游标覆盖该报告位置。Host 还从 worker 输出 CAS 重读 Receipt 引用的 Bootstrap 与 SealRequest，按长度/摘要及实际发送的规范字节逐项比较；缺任一根即拒绝，不以内存对象补成持久完成。文件读取完成后重新检查当前时钟、会话及 Registry 持有的同一存活 PID，才产生 `Captured` 事实。仅存在完成文件、进程曾存活或游标尚未应用均不成功。

任何协议、读取、身份、资源或期限错误使本次 owner 进入不可恢复的失败态并终止、回收子进程，之后不接纳回执或发布报告。成功封口后保留同一子进程等待后续阶段；正常 `stop` 通过 report QUIESCE 要求 worker 先真实 detach MD，再返回报告 cursor 的 Applied；Host detach report 后等待同一子进程成功退出。单纯 kill 或 EOF 不算正常退休。Drop 由 Registry 杀死并回收未退出子进程，异常死亡不会伪造 MD detach。该 owner 不授权交易，也不改变 App 对 schema4 的拒绝。

Capture 模式沿用 Host 已有 qualified-SHM launcher 的调用者安全契约，`Owner::launch` 显式为 unsafe；它不把任意 LaunchSpec 当作安全凭证。报告 FD/cursor 建立复用从 remote launcher 提取的原接缝，没有新增底层映射访问。
