# 策略合并输入尾部重放接口

状态：待审，尚未实现。本接口接续[策略生命周期设计](../design/strategy-lifecycle.md#5-可独立恢复的策略状态)和[发布预检](release-preflight-interface.md)，用于解除当前“最终业务状态必须与固定迁移来源相同”的限制。现有 schema2 发布、恢复与回滚探测保留原义，新能力采用版本化对象和明确的能力声明。

## 默认迁移顺序

固定迁移来源为旧实例在输入边界 **S** 的检查点。旧实例撤权并完成正在执行的回调后，封存最终边界 **C**。S/C 都是策略实际合并输入的序号和链摘要，同时携带各行情流及账户的独立水位。

默认流程固定如下：

```text
旧 binary + S 时的旧参数/模型 + 旧 state schema
          │ restore S；按实际历史 ConfigApply 更新旧参数
          ▼
按原合并顺序重放 (S, C]，使用记录的时间、定时器和 RNG
          ▼
StateAtC，与旧实例实际封存的 C 状态及辅助状态核对
          │ 目标 binary 执行已批准迁移函数
          ▼
目标候选状态 → ConfigApply(新参数) → 同步 TargetInstalled
          ▼
从 C 后的真实输入接续；取得新授权后才允许真实输出
```

恢复旧状态的进程使用固定的旧 binary、旧模型及历史参数；迁移函数在目标 binary 中执行，输入是已核验的 `StateAtC`。直接先迁移 S、再用新策略逻辑重放旧尾部，只有发布清单明确批准独立 `ReplayContract`、规定新语义和动作处理时才可选择。默认契约不提供该选项。

旧实例仍可响应时，Host 对 C 发新鲜 challenge，取得实际 checkpoint、已完成回调位置和同步输入对象。旧版本的隔离重放结果需与这份 C 逐项一致，包括业务状态、市场视图、有效配置、timer/RNG 和历史输出摘要。旧实例已故障时，只能采用持久资料能够证明的最后完整 cut，并按故障接管流程重新核对账户；缺失的内存尾部不会被补成空输入。

全局 Operation 继续固定发布清单、来源 S 和批准的迁移方法。C、旧版本重放回执及最终安装结果作为该 Operation 下的新完成对象保存，不改写原三根，也不把刷新后的 READY 当作固定迁移来源。

## 检查点、输入与切点

拟新增封闭验证类型 `VerifiedReplayAnchor` 和 `VerifiedTail`。检查点除策略业务字节外，还保存下列实际状态：

| 内容 | 绑定及恢复方式 |
|---|---|
| 来源身份 | 逻辑策略、实例/代次、旧 binary、参数/模型和 state/input/output schema 的摘要 |
| 合并输入 | stream、最后已完成序号、链摘要；尾部首序号为 S+1，前链必须等于 S 的最后摘要 |
| 行情 | 每个 topic 的资源/目录/代次、已应用序号、精确盘口和 coverage；可引用实际历史快照及完整前缀重建，不能换成当前 JOIN 快照 |
| 账户 | 真实 baseline、连续账户位置、完整记录及 InputSeal；与 exec 独立读取的 WAL 字节核对 |
| 时间与 timer | 逻辑时钟域/时间、已登记 timer 的 ID/代次/期限、取消状态、待交付事件；保存实际触发顺序 |
| RNG | 固定算法版本、完整状态、已使用位置；重播时核对消耗次数，外部重新播种作为明确输入保存 |
| 输出责任 | 已产生动作的来源关联、原命令摘要、意图身份及最后输出位置；接纳状态另由 exec WAL 给出 |

输入日志拟使用版本化的 `MergedInput`，外层保留有界长度、单调序号与逐条链摘要。其正文至少包括：

- `MarketApplied`：实际 512 字节记录、完整外置 payload、topic 身份，以及策略可见的原始接收和应用信息。数据应用与 `MarketCallback` 分开记录，保留实际回调次数、被新鲜度规则抑制的结果和逻辑时间。
- `AccountCallback`：原规范 baseline/record、账户位置、原回调时间。多账户来源逐域绑定，比较完整风险域与序号。
- `TimerFired`：timer ID/代次、计划期限、实际逻辑触发时间。登记、重新登记和取消由同一 SDK timer 状态机处理；重放只交付日志中的实际触发事件。
- `ConfigApply` / `ModelApply`：旧/新 revision、完整内容对象、批准的用途与实际应用边界。S→C 期间只使用旧运行实际收到的配置变化。
- `ExternalInput` / `EntropyInput`：固定 schema、来源身份及完整内容。需要这些输入却没有实际持久生产者的策略，预检返回 Unsupported。

所有回调通过同一个 sequencer 获得合并序号。同一时间戳下仍按序号执行，不按来源或时间重新排序。输入先进入有界记录，再调用策略；只有回调、辅助状态和输出收集都成功，完成位置才推进。回调报错导致该候选失效，cut 只引用最后成功完成的回调。

当前 `TWINP001` 的长度和链校验可以复用，但现有 kind 3 是行情决策时间、kind 4 是初始化状态，并非完整 timer/config 事件。旧格式只按原能力读取。新对象须明确版本、类型和未知字段拒绝规则；`replay_fixed_demo` 不直接升级为通用恢复资格。

## 执行接口与动作处理

以下为拟定接口，相关私有类型只由共同验证器和真实 Host 完成流程构造：

```rust,ignore
struct ReplayCut;             // 合并位置、各来源水位及辅助状态摘要
struct VerifiedReplayAnchor;  // 固定旧代码/配置与 S 状态
struct VerifiedTail;          // 严格连续的 (S, C] 实际输入
struct VerifiedStateAtCut;    // 旧版本真实重放完成并与 C 核对
struct ReplayContext;        // 记录的逻辑时间、RNG、只读市场/账户视图
struct ReplayEffects;        // 有界模拟输出；无 IntentWriter 或发送接口

trait SourceReplayFactory {
    type Running: ReplayableStrategy;
    fn restore_source(&self, source: &VerifiedReplayAnchor)
        -> Result<Self::Running>; // 默认 Unsupported
}
trait ReplayableStrategy {
    fn apply(&mut self, input: &MergedInput, context: &mut ReplayContext,
             effects: &mut ReplayEffects) -> Result<()>;
    fn checkpoint(&self) -> Result<StrategyState>;
}
fn replay_source(factory: &impl SourceReplayFactory,
                 source: &VerifiedReplayAnchor, tail: &VerifiedTail,
                 expected: &ReplayCut) -> Result<VerifiedStateAtCut>;
fn migrate_at_cut(factory: &impl TargetFactory,
                  source: &VerifiedStateAtCut,
                  release: &VerifiedMemberRelease) -> Result<TargetCandidate>;
```

`SourceReplayFactory` 在受监督的旧 binary 子进程执行；该进程有新的探测身份，只接收已校验文件及认证完成通道，没有生产意图、行情、账户 FD 或交易凭据。目标迁移在目标 binary 的隔离候选中执行。SDK 不向恢复回调提供真实时钟、外部写者或发送能力；策略资格还需遵守其确定性契约，不允许策略绕过 SDK 自行读取时钟、熵源或外部文件。

历史动作的记录可用 `(source_instance, input_stream, input_sequence, output_ordinal)` 作 `EffectId`，同时保存原命令摘要和原意图身份。这只是**同一旧版本执行内**的关联键。新版本可能增加、删除或重排输出，ordinal 跨版本不天然稳定；迁移如需改变业务动作键，必须注册明确映射并验证冲突。

旧版本重放的输出与原输出记录核对，并从 exec WAL 区分已接纳、已确认无字节中止、Unknown 和未接纳。所有历史输出都进入禁止真实发布的集合；未接纳的旧输出在撤权后也不自动改成新意图。目标迁移和 `ConfigApply` 不提供下单输出接口。目标候选只能保留对真实存量订单的管理状态和已经发生动作的记忆，不能携带“安装后补发”的历史输出队列。存在无法解释的待决动作或动作键映射时，安装失败。

显式允许新逻辑重放的契约同样关闭真实输出。新逻辑在历史输入上新产生的模拟动作也属于历史输出，完成时必须消除其待发布资格；禁止把它延迟到第一个实时回调再次发送。契约需提供可核对的动作状态转换，默认实现拒绝该模式。

资金和订单责任始终由 exec 的账户 WAL 决定。重放使用隔离只读账户镜像，不追加账户交易结果、不重复扣款或释放预留。C 后的迟到成交和费用照常进入真实账户，再由新实例按连续回报位置观察；Unknown 不因策略恢复成功而解除。

## JOIN 覆盖、新配置与安装

新实例的行情和账户订阅先建立实际保留约束，回调保持暂停。Host 固定各订阅已有 JOIN 的有限水位，再要求旧实例至少消费到这些水位，在完整回调边界封存 C。这样新订阅能够覆盖 C 之后的区间；不追逐持续移动的 MD 最新水位。

新实例可提前把实际读到的记录复制到有界缓冲并释放槽位，但在最终安装前不把它们应用到候选策略。已有 Shadow 演练对象可以丢弃，随后从本次 `StateAtC` 构造新候选；禁止往一个已经按其他顺序前进的对象上追加旧尾部。缓冲中不超过 C 的记录仅在完整身份、序号和内容与来源一致时跳过，C 之后连续接续。JOIN 已越过 C、缺少中间记录、arena 内容缺失、bank 切换无完整衔接或缓存溢出，均撤销此次 Ready。

若 C 后记录在新实例安装前已经排队，它们仍属于实际接续输入。安装和后续每次消费都由新 sequencer 按真实交付顺序记录；不根据交易所时间戳重排。新 input stream 通过 `StreamStart(parent_cut)` 绑定旧 C，使用新的实例/代次，避免复用旧写者身份。

目标新参数在明确的 `ConfigApply` 完成点生效：

1. 目标 binary 的迁移函数接收旧 `StateAtC`、旧有效配置，以及批准的新配置，输出待应用参数的目标候选。
2. 对候选执行实际 `apply_config`，核对 state schema、timer/RNG 转换和动作状态；它不发布真实意图。
3. 同步新的 `StreamStart`、完整 ConfigApply 内容与结果、候选 checkpoint 和 `TargetInstallPrepared`。这一步失败时旧运行对象和关闭的增险门保持原状。
4. 原子替换业务及辅助状态，保持回调暂停；随后同步 `TargetInstalled` 并发送认证完成消息。替换后的同步失败使该实例停止，保留已发生的内存状态，不返回完成，也不回退成获准运行的旧对象。恢复根据 Prepared/Installed 记录重建新候选，重新取得实际进程资格。
5. Host 确认安装后才处理 C 后的首条输入；真实输出还要等待 exec 的新授权。

历史逻辑时间与新进程实际时间分别保存。相同时钟域可继续其单调关系；跨主机/启动代次的原始纳秒不能直接作新 timer deadline。首个实现对缺少批准时钟转换的跨域恢复返回 Unsupported。后续 `ClockTransition` 需绑定真实新时钟域、计时契约和已记录的转换依据，并明确停机期间 timer 是继续流逝还是暂停，完成转换后才建立新 timer 调度器。

Host 拟新增 `capture_tail`、`run_source_replay`、`install_at_cut` 三个异步步骤，各自使用唯一 challenge、真实进程身份及有界期限。尾部以内容寻址对象传送，控制包只引用长度与摘要；共享解析器按对象数、单对象和累计字节上限验证后，才准许实际回调。容量耗尽返回明确错误，保留仍被发布操作、意图或镜像引用的对象。

`SourceReplayReceipt` 绑定完整 Operation/member、原预检 Completion、旧代码/参数、S、C、实际旧版本重放输出以及输出核对结果。`TargetInstalled` 再绑定迁移方法、新配置、目标进程身份、输出检查点和新输入流起点。两份回执都由真实回调与同步完成产生；Host 校验实际认证消息和文件，exec 重新校验闭包与真实账户前缀。新的 HandoverSeal 引用这组对象，本地恢复与镜像共用同一验证器。

Commit 前重新核对账户 cut、仍需管理的订单能力及目标进程存活。C 后账户推进导致旧 READY 过期时，新实例连续消费回报再出具新 READY；固定 S、C 和已完成迁移证明保持不变。历史安装记录只证明某次安装完成，exec 重启后仍需新进程认证、冻结恢复、对账及新授权。

## TDD 实施阶段

| 阶段 | 先取得的真实行为 Red | 随后实现范围 |
|---|---|---|
| 1. 规范输入与完整 cut | 同时刻不同来源换序、缺行、错前链、旧配置被新参数替换、缺 timer/RNG 导致被错误接受 | 版本化 anchor/tail decoder、共同验证器、范围与容量检查；保留旧格式读取 |
| 2. 旧版本实际重放 | 包含行情、timer、配置、账户及 RNG 的状态策略，S→C 重放输出与连续旧执行不一致；错误 binary/schema 仍获成功 | 通用回调、虚拟时间、SDK timer/RNG；实际旧 binary 工厂及输出核对；回调错误不推进完成 cut |
| 3. 动作与迁移 | 同序号跨版本不同 ordinal 被错误去重；已接纳/Unknown/未接纳历史动作安装后再次发布；C 状态尚未重建就迁移 | 原动作与账户 WAL 关联、禁止历史发布集合、StateAtC 迁移、新 ConfigApply 的真实完成点 |
| 4. 订阅接续 | 新 JOIN 越过 C、缓存缺口、旧 arena 重用、C 后重复账户回报、已有 Shadow 状态被叠加旧 tail | 实际保留下界、有限 cut 请求、候选重建、严格重叠校验及连续接续；失败保持门禁关闭 |
| 5. 持久与进程 | checkpoint/回执同步失败仍 Ready；错误 challenge 或缺对象仍 Commit；安装中断后复用旧授权 | 认证协议、旧版本探测进程、目标安装、Host/exec/镜像共同闭包；逐完成点故障回归 |
| 6. 完整交接 | 旧策略在 S 后确实改变业务状态，现有“state 必须不变”检查拒绝该次合法交接 | 实际旧进程封 C→旧 binary 重放→迁移→新配置安装→接续→新授权；迟到成交只入账一次，首个新动作只能来自获准的实时输入 |

每阶段以真实生产入口或被生产调用的共同函数取得 Red，之后补实现。只有全部输入种类的实际记录和消费入口齐全，才移除对应策略的“不支持尾部重放”限制；声明重建继续使用独立契约，不作为重放失败的替代路径。
