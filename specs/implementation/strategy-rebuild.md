# 声明重建：资料校验与 SDK 预热

接口对应 `tickwire-release::rebuild` 和 `tickwire-strategy-sdk::replay::rebuild`。这是声明重建的内容与回调实现，Host、worker 启动协议、执行端和协调器接线尚未完成。

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

`MemberAuthority.readiness` 为可选值。schema1–3 仍要求 Some，已有 JSON 和签名字节不变；schema4 可以没有初始业务 READY，安装结果始终要求真实 READY。app 在实际 WindowSeal/目标安装接通前显式拒绝 schema4 执行，避免误走旧恢复分支。生命周期库 34 项单元测试与定向 Clippy 通过，app/lab all-targets 编译通过；该检查没有运行完整 Rebuild 发布场景。

## 输入窗口采集

`replay::rebuild::capture::CaptureOnly` 持有隔离的行情和账户视图，使用 SDK 固定观察器核验输入，不接受或执行用户工厂。创建时绑定 capture 身份、binary、逻辑策略、账户风险域、时钟域、配置和模型摘要；行情资源的 `host_boot_id` 必须与采集时钟域一致，当前只接受 `clock_domain=1` 的本机单调时间；跨域时间不能用于新鲜度计算。种子只能包含真实行情检查点和完整账户前缀，`warm` 必须为空，采样时间必须等于窗口起点。要求旧进程处理顺序的合同保持拒绝。

`push` 接收同一时钟域中单调的行情应用、行情观察标记和账户记录。它复用真实行情和账户解码、连续序号、资源身份与已接纳动作集合检查，按合同限制时间跨度、记录数和完整输入对象字节数。每次先验证候选视图及窗口，再整体发布；任何错误均保留上一有效窗口，后续正确输入仍可继续。配置、模型、用户定时器和额外外部输入在本采集入口返回 `Unsupported`。

`finish` 消费采集 owner，拒绝空窗口、跨度不足、过期或不连续视图，以及未达到每类最小观察数的窗口；同一已应用记录的重复观察标记只计一次。成功返回实际 `Inputs` 和 `window::Seal`，绑定初始/最终 cursor、合同、种子、完整输入、逐行输入哈希链及账户输入摘要，不构造用户检查点、READY 或交易许可。Host 发布协议和实际进程采集仍待接线。
