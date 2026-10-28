# 策略账户与行情输入范围的实际消费

Status: implemented

本模块把全局投影的完整 `strategy.account_ids/topic_ids` 与策略实际消费的账户报告、行情 JOIN 连接起来。参数消费者核对工厂配置，本模块进一步核对实际输入范围。本阶段针对现有每账户执行进程和单外部行情订阅的普通 registered Active/Shadow 首启；多账户、多 topic、内置市场、capture、预恢复及轮换整体拒绝。完整全局 Applied、运行中投影变更和其他配置字段消费者仍待接入。

## 批准输入与完整集合

`HostConfig.projection_inputs` 显式批准完整投影文件、`profile::Binding/Scope`、查询期限，以及账户名称、执行者名称和行情发布者名称到实际身份的映射。部署方显式提供全局名称到交易所账户及进程身份的映射，名称与数字、路径之间的对应关系均以这份批准为准。

输入采用严格 JSON 配置值，字段未知或重复时拒绝。账户映射固定全局 account/venue/risk-domain 标识、完整规范基线对象、实际账户 scope、当前 exec 名称和完整报告 peer 身份。市场映射固定数值 topic、当前 publisher 名称、完整 peer 身份及批准的 bank/目录/成员全集。身份校验要求当前 owner，standby 名称在此入口拒绝。

在创建目录和子进程前检查 `account_ids`、`account_views`、账户映射集合完全一致；`topic_ids`、`ipc.topics` 与市场映射集合完全一致；`venue_views` 精确覆盖账户引用。核对实际 runtime 基线、账户 journal、命令绑定、Bootstrap risk domain、intent/report peer 与报告 bank producer。市场核对完整 ConnectConfig、bank 描述符、产品、命名空间、生产者及目录成员。完整 64 位标的 ID 和版本保持原样。只容忍既有零 descriptor 模板的规范化，非零错误摘要拒绝。

产品与环境采用显式对应：Spot 对应实际 Spot；Usdm 对应线性永续或线性交割；simulated 对应实际 Test，production 对应 Production。投影参数、启动资源与输入范围同时启用时，完整 ObjectBinding 和 Scope 必须相同，任一约束失败均终止本次启动。

当前 NormalizedMarket bank 的 venue/product 字段可为零，产品身份因此由实际执行 route、命令绑定和规范账户目录的 InstrumentBinding 共同核对；市场成员逐项按完整 ID/版本关联该目录。本阶段拒绝账户目录没有提供经济身份的额外市场成员。

`allowed_symbols`、风险预算以及 `instrument_catalog_ref` 制品的经济规格仍待各自消费者接入。制品摘要、运行时 catalog hash 与基线摘要分别保存。此处实际目录成员与批准 ConnectConfig 精确匹配，其全局经济规格来源由后续目录制品消费者绑定。

## 当前消费事实

SDK 增加只读的持久应用视图检查器，不执行策略工厂、回放或用户回调。它从当前检查点恢复完整账户和市场视图，核对重复/遗漏/额外 stream、ResourceHeader 到 stream 的绑定，以及各视图与 RuntimeState cursor 的一致性。账户基线、历史位置及 InputSeal 均由实际保存字节重算。

Host 的 `FreshObservation::inputs()` 返回字段私有的 `ObservedInputs`。它与现有配置事实来自同一个经过完整封口核验的当前合并检查点，保存 Source、Cut、检查点对象及完整输入列表；账户再与已验证报告历史交叉核对。报告事实取自实际 ReportWriter 的 header/identity 和本次已核对的共享消费 cursor。Bootstrap 模板只用于批准值比较。

市场当前视图可证明完整 bank、实际生产者实例/代次、topic、ring/feed 代次及成员 ID/版本。catalog hash 和 instrument-set revision 只有存在真实已保存 JOIN 证据、且该证据与当前 bank 相符时才可作为观察事实；缺失时返回不可用。MD 完整 peer 身份来自认证启动批准，其内核映像的独立核验留到后续资格验证。App 要求全部事实来自当前视图且匹配，旧 anchor 和无关同摘要文件均拒绝。

参数和输入范围使用同一次内部新鲜查询、同一个实际进程及当前检查点；期限取已启用约束的较早截止点，冷推进仍最多每 10 ms 一次，重试不续期。两项都通过后一次性发布各自证明，才交付首条 intent。内部查询期间，外部 begin/poll/cancel 返回忙碌。恢复启动重新查询，历史证明仅供追溯。最终发布证明前再次采样时钟，核对原截止点。

`replay::application::observed::inspect(&Checkpoint)` 返回字段私有的 `InputViews`。完整 stream 列表、水位、视图编码和账户历史必须一致；非零 bank descriptor 摘要必须正确。`ReportWriter::header()` 与 `identity()` 读取实际发布 owner 的绑定。

`ObservedInputs` 提供 `source/cut/checkpoint/accounts/markets/report`。账户提供 `baseline/cursor/position/input_digest`，其中 baseline 保留完整规范字节和 Scope；市场提供 `bank/cursor/instruments/join`，标的表示完整的 `(u64 ID, u64 version)`。报告提供 `header/identity/subscription/sequence`，沿本次 READY 已核对的真实共享 cursor 绑定。`ObservedJoin` 提供 `catalog_hash/instrument_set_version/instruments/snapshot/baseline_sequence`，成员三元组另含实际保存的 `required_states`。

本阶段 JOIN 提取仅使用完整封口命名的原始 `TWSC0001→TWSB0001`，逐字段匹配当前 bank 并重验快照。换 bank 或其他恢复格式没有此证据时返回 `join=None`。合法当前视图尚无账户或有多个账户时，单报告 Host 返回 `inputs=None`；存在但畸形的视图仍失败。该可选边界保留原配置观察用途，启用范围约束的 App 必须要求完整事实可用。

## 持续门禁与不支持的变更

门禁显式区分未启用、等待证明、已核验和关闭。完成 Prepared 消费后仍保留批准的账户、基线和实例身份；Prepared 已消费与未启用是不同状态。门禁通过后，`next` 新增的输入身份检查仅比较缓存摘要与固定身份，不读取文件、`/proc` 或分配内存；Host 原有的行情、报告与控制工作继续执行。

同一规范基线的重复投递保持幂等，普通账户记录继续沿原证明推进。不同基线或完整账户 scope 在交给 Host 前即关闭门禁并终止子进程；排队命令保持关闭，失败状态锁存。目标按精确实例路由，错误目标在触及 owner 前拒绝。

本阶段的输入范围固定于首次启动。运行中行情 Arm/Commit、回放安装/完成及交接 quiesce/activate 在 Host 写控制对象或改变输入前拒绝，实际 market consumer 仍可见。拒绝保持旧输入，并返回失败而非成功收据。转向未启用约束的 Shadow 时，同样要求旧实例交接成功。Multi 可能已将双方转为 Shadow 或暂停，但后继激活与命令交付仍关闭。省略配置时保留原有扩容、回放和交接行为。

纯检查覆盖完整集合、同风险域换账户、错环境/产品/报告 owner、错误 MD producer、同符号异 ID/版本、错误目录及所有受限模式。SDK/Host 反例覆盖旧启动范围替代当前视图，以及损坏或部分持久链被误判为缺失的情况；这些输入均拒绝。正常字段省略的序列化和原调用行为保持兼容。

真实 App 装配、独立 worker、父线程实际 MD 服务和共享内存用于正例、排队意图门禁、超期回收、基线替换、扩容/交接拒绝及精确实例路由。参数与范围同时启用时必须共享同一挑战与检查点；通过后正常记录继续推进。另以固定 release 程序运行原 Spot 完整业务兼容场景。范围证明仅记录输入消费事实；实际下单授权、完整全局 Applied、生产性能与跨域组合资格分别验收。新增 TLA/TLC 留到功能开发完成后。

本阶段最终固定产物验收已通过：8 项 App 输入进程测试、6 项原参数进程回归、1 项 Host 持续查询，以及完整 Spot 交接业务流程。全 workspace、设计契约与 OpenAPI 检查通过。[阶段报告](../evidence/20261029/strategy-inputs.json)保存源码、程序摘要与实际闭包字节；[离线复核器](../evidence/20261029/verify-strategy-inputs.py)重算这些字节的引用关系，并保留历史进程断言的证据边界。
