# Tickwire 配置文件与控制 API

版本：v0.3 · 2026-10-20 · 状态：生产设计契约，接口与运行时尚未实现

**系统以版本化配置描述进程、交易接入、共享内存、策略、账户风险、持久化和运行资源。文件导入与控制 API 使用同一套校验、计划和提交流程；API 修改成功后，每个目标进程本机的期望配置和生效配置都必须持久更新。** 控制组提交只是操作的开始。只有目标运行时已经应用，且本机 `effective.toml` 已持久保存相同全局 revision/hash 和该目标预期的权限投影，操作才能成为 `Succeeded`。

应用程序编程接口（Application Programming Interface，API）在这里指通过 HTTPS 访问的配置管理接口。它管理低频配置和操作状态；正常订单仍由 exec 使用本地已应用配置处理，不经过配置 API 或控制组的 Raft 共识往返。

本文与[主设计](binance-ultra-low-latency.md)、[策略生命周期](strategy-lifecycle.md)、[持久化与接管](replication-and-failover.md)、[共享内存契约](shared-memory-ipc.md)及[部署与服务目标](deployment-and-slo.md)共同生效。

## 1. 配置产物与权威来源

| 产物 | 用途与权威范围 |
|---|---|
| [system-config.schema.json](../../specs/contracts/system-config.schema.json) | JSON Schema 2020-12 数据模型；TOML 解析后按相同模型校验，未知字段拒绝 |
| [control-api.openapi.json](../../specs/contracts/control-api.openapi.json) | OpenAPI 3.1.1 请求、响应、状态码、认证与操作状态契约 |
| [reference.toml](../../config/reference.toml) | `TW-LD-ASYNC-16x2-v1` 合成资格配置；可解析并通过结构校验，使用模拟交易接口和 fixture 引用 |
| [production.template.toml](../../config/production.template.toml) | 生产输入模板；`REQUIRED` 项故意无法通过校验，填入实际部署证据后再计划和提交 |
| [deployment-reference-v1.json](../../specs/contracts/deployment-reference-v1.json) | 技术部署、容量与目标的参考清单，不覆盖运行配置的版本协议 |
| 控制组 `ConfigCommitted` | 集群期望配置、不可变 revision、内容哈希、目标集合及操作的权威事实 |
| 每个 exec 的已提交日志和 applied 状态 | 本执行域实际采用的配置及事件边界；控制组不得把自己的 commit 当成 exec applied |
| 其他进程的本地配置 journal 和 applied 状态 | 对应进程已应用版本与恢复证据 |
| 本机 `desired.toml` / `effective.toml` | 从上述事实生成的进程权限投影；分别表示收到的已提交期望版本与已应用版本 |

TOML（Tom's Obvious Minimal Language）用于人工编写候选文件；JSON（JavaScript Object Notation）用于 API 数据模型。两种输入都先转为同一个严格类型树。策略参数容器深度最多 16 层，并受请求字节上限和随发布文件提供的 schema 双重约束。金额、费率和其他精确小数使用十进制字符串；时限和容量使用字段名注明单位的整数。禁止浮点数、NaN、无穷大、TOML 原生日期时间、重复键、未知字段和隐式单位转换；JSON 整数限定在精确安全范围内。revision 和可能达到 64 位的日志序号以十进制字符串传输，语义上必须小于 `2^63`。

全局配置内容哈希为 `SHA-256(JCS(full_config))`。JSON 规范化方案（JSON Canonicalization Scheme，JCS）固定键序、字符串和数字编码，使 TOML 排版、注释和对象键顺序不改变内容身份。数组顺序保留；计划器不得在哈希后重排数组。显式默认值在哈希前补全，计划内保存最终完整树，之后不再依赖版本变化的隐式默认值。完整候选不含 `_materialization`；生成的进程文件使用独立的 `ProcessProjection` schema，元数据不进入权限 payload 的哈希。另记录最终 TOML 字节的 `render_hash`。[JCS 规范](https://www.rfc-editor.org/rfc/rfc8785.html)

同一集群的 revision 严格递增且不复用。回滚也生成新 revision。ETag（HTTP 实体标签）绑定完整期望配置，格式为 `"cfg-<revision>-<64位小写十六进制哈希>"`；所有修改必须带精确的 `If-Match`，不接受通配符或弱 ETag。版本、身份、内容哈希以及操作 ID 是不同字段，不能互相替代。

### 1.1 文件和 API 的同一路径

操作者编辑的是候选文件，例如版本库中的部署输入；提交文件的工具读取该文件，通过 `validate → preview/plan → commit` 上传相同内容。API 客户端可以直接提交 JSON，后续过程完全相同。候选文件本身不因修改而自动成为生效版本，也没有绕过控制版本的启动参数覆盖、环境变量覆盖或配置目录自动加载。

自动生成的 `desired.toml` 和 `effective.toml` 只有本机配置物化服务可写。文件监视器只检测漂移，不将手改内容反向导入。需要修改本机文件中的值时，先复制为候选，再提交正常计划。控制 API 更新必须自动刷新目标主机的生成文件；不能把“请操作员再保存一次配置”作为成功后的补充步骤。若用户希望候选文件仓库同步更新，应由独立导出流程保存已提交版本，并记录 revision；该仓库不构成第二个运行时写者。

初次引导必须在进程未获得交易权时，用经过签名和校验的集群引导包创建第一条控制配置记录，再生成本机文件。引导包只包含集群身份、控制节点发现和信任根引用；恢复控制组按复制契约操作，不凭本机 mtime 较新的配置建立一个新权威。控制组不可用时，已有进程按缓存配置与授权期限工作；授权到期停止增险，不能靠手改文件恢复授权。

## 2. 配置分域与实际部署输入

`additionalProperties=false` 约束所有固定结构。策略 `params` 的键和值先经过基础类型限制，再按策略发布文件绑定的不可变参数 schema 校验；缺少该 schema 时不能提交或激活。`artifact://...@sha256:...` 指向内容寻址的不含敏感信息的对象，`secret://<provider>/<path>/<version>` 指向外部凭据存储中的固定版本；引用变更本身也进入配置 diff。

| 顶层域 | 关键字段与作用 | 参考配置 |
|---|---|---|
| `identity` | schema、cluster、namespace、运行模式、交易环境、部署配置 | qualification / simulated；配置方案 `TW-LD-ASYNC-16x2-v1` |
| `hosts` | 主机、故障域、物理核与内存、NUMA、SHM 预算、系统核 | A/B 各 64 核、256 GiB；C 8 核、32 GiB；交易主机 SHM 各 8 GiB |
| `processes` | 稳定进程 ID、角色、主机、Active/Standby/Shadow、发布文件、操作系统身份、CPU、内存、重启策略 | A 为两行情、两执行域与 16 Active＋16 Shadow；B 预热行情和只读执行恢复 |
| `venues` | Binance 产品和环境、协议、REST/订单/行情/私有流地址、协议和标的目录、请求窗口、心跳/轮换、凭据引用 | Spot 与 Usdm 独立；资格配置使用 mock 和 `.invalid` 地址 |
| `ipc` | ABI、架构配置、页和隔离尺寸、ring/arena/bank、批量上限、topic、资源会话 | 两个 NormalizedMarket topic，各 4 banks、65,536×512 字节、每 bank 256 MiB arena、40 consumers |
| `strategies` | 逻辑 ID、进程、Active/Shadow、账户域、订阅、状态/参数/恢复 schema、参数、逐账户 risk_budgets、信任模式 | 16 组逻辑策略，每组一个 Active 和至多一个 Shadow，可订阅两个账户域 |
| `accounts` | 真实账户身份、产品风险域、主备 exec、guardian、凭据、模式快照、标的、限流政策 | 现货和合约两个域；不以产品名推导实际账户权限 |
| `risk_policies` | 单笔/总毛敞口/订单数/日损限额，行情年龄、费用和保证金缓冲，费用/档位/交割证据，降限额策略 | 合成测试金额；真实金额必须由部署输入给出 |
| `journal` | 提交模式、目录、分段/帧、合批阈值、pending 容量、快照、磁盘保护线 | LocalDurable+AsyncReplica；32 条/64 KiB/25 µs 合批上限；80% 告警、90% 冻结增险 |
| `replication` | 接收主机、协议、认证、批量、在途上限、复制落后阈值、同步等待 | 100 ms 告警、1,000 ms 冻结；禁止自动降级异步 |
| `control` | 三 voter、地址和身份、Raft 参数、角色绑定、计划期限、操作期限、幂等保留、授权期限 | tick 10 ms、heartbeat 50 ms、选举 500～1,000 ms；配置操作默认 120 s |
| `local_config` | 生成文件根目录、权限、投影政策版本/发布文件、固定引导 locator、保留版本、持久化、漂移与恢复政策 | 0600 文件、0700 目录、至少两份 last-good |
| `observability` | metrics 地址和认证、诊断等级、采样、直方图、审计目录和保留 | 有界标签、引用脱敏；审计不受诊断采样丢弃 |
| `timeouts` | 意图期限、私有流新鲜度、校时偏移、quiesce/退出、未知单查询、重连 | 单位在字段名中固定，不能靠加长超时绕过账户未知状态 |
| `qualification` | 部署清单、权限、主机、掉电/隔离/风险验收证据和完整发送目标 | 默认 p99 300 µs、p99.9 1,000 µs；SyncReplica 候选 2,000/5,000 µs |

进程间通信（Inter-Process Communication，IPC）使用共享内存（Shared Memory，SHM）；应用二进制接口（Application Binary Interface，ABI）固定共享字节布局。单生产者单消费者（Single Producer Single Consumer，SPSC）意图/回报通道每方向两个 bank，分别为 `4,096×1,024` 和 `8,192×512` 字节。每个策略实例与每个执行域的连接单独计数；Shadow 预分配通道也计入预算，即使没有增险权。两个行情 topic 内部使用不同消息类型表达簿、报价、成交和参考价，recorder 经独立 QoS 中继接入，新增流另行计入资源。

非统一内存访问（Non-Uniform Memory Access，NUMA）会影响 CPU 与内存的访问成本。参考配置里的 CPU 编号只是合成拓扑：0～7 系统和控制服务，8～9 行情，10～11 执行，12 guardian，13～14 journal/复制服务，15 recorder，16～31 Active，32～47 Shadow，48～55 输入持久化和快照，56～63 为监控和余量。生产必须根据实际物理核、操作系统 CPU 编号、NUMA 和网卡位置重新绑定；同一个 Hot loop 核不得分配给两个独立热循环。

### 2.1 三种期限各自独立

| 字段 | 参考值 | 到期的含义 |
|---|---:|---|
| `ipc.resource_session_ms` / `resource_heartbeat_ms` | 3,000 / 500 ms | 资源控制会话失效；进入 IPC 恢复流程，不凭心跳超时回收尚有引用的 bank |
| `control.exec_grant_lifetime_ms` / `exec_grant_renew_ms` | 100 / 25 ms | exec 的短期执行资格失效，禁止增险；期限按本地挑战发送时间计算 |
| `control.strategy_grant_lifetime_ms` | 60,000 ms | 策略代次授权的上限；撤权、代次变化或 exec 短期资格到期可提前使它无效 |
| `control.strategy_progress_interval_ms` / `strategy_suspect_after_ms` | 10 / 50 ms | 主循环缺少有效进展后进入 suspect，撤销该实例增险；后台线程心跳不代替主循环进展 |

配置这些期限不会合并其安全语义。每次动作仍同时验证执行资格、策略代次、主循环新鲜度、operation 屏障与风险状态。

### 2.2 生产必填内容

`mode=live` 必须同时为 `environment=production`，所有 venue 也必须一致。生产校验器拒绝 `REQUIRED`、fixture 引用、`.invalid` 地址、mock transport、未解析发布文件和未经授权的凭据引用。至少补齐：

1. 实际 cluster/namespace、主机与故障域、CPU/NUMA/网卡/存储清单、隔离设备与身份。控制 voter 的三个故障域必须独立；C 不存订单 WAL。
2. 经固定协议清单验证的 Binance 地址、协议、schema、心跳、轮换和请求期限；Spot FIX SBE 另要求 `fix_session_ref`，其中固定会话地址、端口、身份、认证和序号恢复政策，不能仅替换 WS URL。
3. 真实账户/子账户身份、产品权限、单向/双向持仓、全仓/逐仓、单资产/多资产模式、执行所有者和 guardian 权限；只读查询核对，不能自动修改交易所模式使检查通过。
4. 实际标的目录、精度/过滤器、资产指数、费用、风险档位、资金费和结算规则，以及账户/IP/UID 限流预算。费用和档位可存于不可变 schedule 对象，API 传引用和哈希，exec 校验实际内容与来源新鲜度。
5. 账户与每策略的明确风险预算、策略二进制/参数 schema/状态迁移/恢复所需文件、受审计 SDK 资格和操作角色。
6. 固定版本凭据引用与信任根，以及部署、掉电、隔离、风险计算和完整链路资格证据。引用存在且哈希正确只证明对象身份，不能代替报告中的验收结果。

每个策略的 `risk_budgets` 按 account ID 和 quote asset 明确计价，由对应账户 exec 独立执行；跨账户总暴露不能借用一个没有单写所有者的共享预留。每策略预算是上限，不是预分配可用资金；即使策略上限合计高于账户限额，每次实际预留仍必须同时通过账户总限额。

`reference.toml` 的 fixture 哈希是合成名字的内容标识，仓库没有为这些引用提供可交易的账户、凭据或发布文件。因此它通过的是配置解析与静态一致性校验；`validation_level=deployment` 仍会要求解析真实依赖。生产模板里的技术容量也要对实际机器验证，不能只替换字符串占位。

### 2.3 永续与交割合约

Usdm 标的目录区分 Perpetual 和 Delivery。`accounts[].instrument_overrides` 可以给出固定 `contract_kind/expiry_epoch_ms/delivery_guard_seconds/settlement_schedule_ref`，必须与版本化交易所目录一致；Spot 和 Perpetual 的 expiry 为 0，Delivery 的 expiry 必须大于 0。空 overrides 表示使用经校验目录，不能把缺失的到期时间解释为永续。

`risk_policies[].delivery_guard_seconds=1800` 是参考本地禁增险窗口，可配置，不是交易所规定。进入到期前窗口后禁止该交割合约新风险，允许政策许可的减仓和撤单。费用和结算规则分别由 `settlement_fee_schedule_ref` 和 `settlement_schedule_ref` 固定。到期不自动清零持仓、预留或未决订单，必须处理交易所结算事实；`expiry_resets_positions=false` 与 `auto_rollover=false` 是当前合同常量。换月是新的显式策略/执行操作，不由配置热更暗中平旧开新。

### 2.4 结构校验之后的语义校验

Schema 的 `x-semantic-checks` 列出 CFG-S01～CFG-S12；这些是实现必须执行的约束，普通 JSON Schema 库不会自动实现扩展字段。关键检查如下：

- 所有 ID 唯一、引用完整；账户共用资金时归同一执行风险域；策略最多一个 Active 和一个 Shadow；角色、实例上限与订阅消费者上限相符。
- ring 容量为二的幂；布局等于固定 ABI；arena 实际 class 分配、headers、游标、全部 live/frozen banks、SPSC、快照和 relay 合计不超过主机预算。改变配置不能省略仍被 pin 的旧 bank。
- CPU 和 NUMA 存在；热循环核不重叠；内存和文件描述符预算可容纳；主备故障域分离；三控制 voter 的身份与故障域唯一。
- 心跳小于所属会话期限，续期小于短期执行期限，progress 小于 suspect；选举、复制告警/冻结、磁盘告警/冻结和重连上下界顺序正确。
- 请求中的来源、发布文件、凭据、账户模式和策略参数匹配固定 schema；含凭据内容的字段或参数拒绝。修改 classification 由服务端从旧值、新值与运行事实计算，客户端不能自报为 Hot。
- 计划保存 base ETag、候选哈希、目标成员集合及其代次、资格证据和作用域；commit 和 apply 时重新验证当前事实。预览时的余额、订单、凭据可用性和磁盘余量不能作为未来应用一定成功的保证。

## 3. 运行时更改分类

Schema 上的 `x-change-class` 是字段族的保守默认值；下表对具体字段进一步限制。一个计划包含多项修改时，选择其中要求最高的流程；任何 ForbiddenRuntime 变更使整个运行时计划拒绝。未知字段或未知分类默认拒绝。

| 分类 | 允许的典型更改 | 应用规则 |
|---|---|---|
| `Hot` | 诊断日志级别、采样率；参数 schema 明确标为 Hot 且不改变状态布局的策略参数；可立即安全收紧的风险限额 | 单写 reducer 在一个事件边界替换不可变配置视图；保存旧/新哈希和水位。改变诊断等级不删除交易审计 |
| `Quiesce` | 放宽风险预算；需暂停的策略参数/订阅/权限；费用和档位引用；限流预算；凭据版本轮换；账户会话切换；提交模式变更 | 关闭受影响域增险入口，排清或保守保留未决意图，完成领域屏障/对账/依赖检查后应用；按计划条件恢复资格 |
| `RollingRestart` | 二进制与 SDK、CPU/NUMA、内存与文件描述符、journal/审计/配置目录、metrics/控制监听地址、IPC 容量/布局配置、影响连接器结构的配置 | 创建替代实例或新资源，校验恢复和接管；按生命周期切代次；不得同时重启主备或失去控制多数派 |
| `ForbiddenRuntime` | cluster/namespace、live/qualification 或生产/模拟环境切换；账户身份/资金风险域重解释；原地修改 wire ABI major、金额单位和历史精度；关闭持久化/审计/fencing；回退序号/代次 | 运行时 API 拒绝。需要另建部署/namespace 和迁移计划，经过离线准入；不能强制标成 Hot |

补充规则：

- 增加策略或修改 Active/Shadow 意向触发[策略发布协议](strategy-lifecycle.md)，配置字段不直接颁发交易权。Shadow 没有增险权限，API 改为 Active 仍须预热、状态迁移、管理权提交与风险释放。
- `journal.commit_profile` 从默认模式改到 SyncReplica 要先冻结、追平并验证指定镜像和闭包；反向修改需要 risk-admin 明确接受新的故障语义，再通过 Quiesce。禁止镜像失联时自动降级。
- `control.voters` 走复制文档的成员变更流程；不能将整个数组热替换为不相交的多数派。控制监听地址和信任根轮换先预置重叠信任/双地址，再逐个切换。
- endpoint 变更至少 Quiesce，涉及连接器/监听绑定时 RollingRestart；账户产品、资金模式或环境不能通过换 endpoint 偷渡。交易所账户模式只校验，真实模式切换是独立管理操作。
- ABI 1.0 的 record size 是 schema 常量。容量变更也需新 bank 与剩余容量证明；major 变化需新 namespace 或经资格验证的桥接，不能修改正在映射的头部。
- 投影政策、物化目录属主和 bootstrap 发现规则属于集群权限边界；更改投影政策或属主必须有 cluster-admin，并重新生成/验证每个目标的预期投影。普通账户补丁不能修改这些字段。
- 所有风险放宽要求 `config-operator` 与 `risk-admin`，即使数值变更很小。超时、行情年龄、费用缓冲、交割保护窗口等字段若削弱保护，也按风险放宽处理。

### 3.1 降低限额时处理已有暴露

plan 返回当时的持仓、已挂单、pending/Unknown 预留、条件结构、策略与账户总暴露；exec 在真正应用边界重新计算。任何已有经济事实都保持原 ID、数量和归属，不能为通过新限额删除预留。

| `exposure_policy` / `on_limit_decrease` | 已有暴露超过新限额时的行为 |
|---|---|
| `reject_if_exceeded` | 拒绝应用，保持旧有效版本并返回 `RiskExposureExceeded`；已提交 desired 保留，操作 Stalled，等待重新计划或显式修复 |
| `freeze_until_within` | 接受更严格限额，禁止增加相应风险，继续成交入账、查询及可证明减险的操作；已有挂单不自动取消；降至限额内并经资格屏障后才恢复增险 |
| `cancel_then_freeze` | 先冻结，再按已批准计划发起可取消订单的撤单；撤单动作拥有独立经济动作键并持久化。收到终态前不释放预留，Unknown 继续占额；不自动平持仓 |

只有 `cancel_then_freeze` 计划明确列出取消范围并获相应账户操作权限，才允许取消；客户端通过 `accepted_actions` 承认计划列出的动作。配置限额更新可以完成而风险域仍处于 `Frozen`，响应明确报告这一状态。`Succeeded` 证明版本和文件收敛，不表示敞口已经下降或交易已恢复。

## 4. 控制接口与并发语义

表中路径相对已验证的控制服务地址。OpenAPI 固定请求体，所有写入都有审计原因；secret bytes 不接受为任何接口输入或输出。查询使用控制组线性一致读；无法取得安全读取屏障时返回 503，不能以旧 leader 缓存冒充当前 desired。

| 方法与端点 | 输入与返回 | 操作语义 |
|---|---|---|
| `POST /v1/config/validate` | `candidate={format,document}`，`validation_level=structural/deployment`；200 `ValidationResult` | 无副作用；200 只表示完成校验，必须查看 `valid`、issues、completed/pending checks |
| `POST /v1/config/preview` | 候选、原因、apply ceiling、exposure policy、If-Match；200 `Plan` | 不持久化的 diff/预检查；preview ID 不能 commit |
| `POST /v1/config/plans` | 同上，加 Idempotency-Key；201 `Plan` 和 Location | 保存不可变计划、diff、依赖闭包、目标集合和期限；未改变 desired |
| `GET /v1/config/plans/{plan_id}` | 200 `Plan` | 查询分类、动作、必需角色、截止时间及问题 |
| `GET /v1/config/plans/{plan_id}/diff` | 200 `DiffEntry[]` | 字段路径、前后值、分类、目标和前置条件；凭据只显示引用 |
| `POST /v1/config/commits` | plan ID、candidate hash、accepted actions、原因、If-Match、Idempotency-Key；202 `Operation` | 原子提交 desired 和 operation；随后执行分发、应用与文件同步 |
| `GET /v1/config/head` | 200 `Head`、ETag | 只读 revision/hash/ETag/提交时间；窄账户编辑者据此取得基线，不读取其他账户配置 |
| `GET /v1/config/desired` | 200 `Desired`、ETag | 当前控制组已提交完整配置及 commit 引用 |
| `GET /v1/config/revisions/{revision}` | 200 `Desired` | 不可变历史配置；用于核对和回滚计划 |
| `GET /v1/config/effective` | 可按 process ID 筛选；200 `Effective` | 每个目标的 desired/runtime/file 版本与哈希、代次、观察时间和错误；逐目标状态不压成单个伪全局版本 |
| `GET /v1/config/operations/{operation_id}` | 200 `Operation` | 仅 `state=Succeeded` 宣告更新完成；200 本身不是更新成功 |
| `GET /v1/config/operations?idempotency_key=...` | 200 原 Operation 或 404 | 丢响应后的调用者范围查询，不允许枚举别人的键 |
| `POST /v1/config/rollback-plans` | restore revision、原因、apply ceiling、exposure policy、If-Match、Idempotency-Key；201 `Plan` | 以旧内容创建新计划，重新检查今天的风险与依赖；仍用 commits 提交 |
| `POST /v1/config/operations/{id}/resume` | 原因、If-Match、Idempotency-Key；202 `Operation` | 只恢复 Stalled 操作，继续相同 revision；计划外身份或代次变化先重新计划 |
| `GET /v1/config/watch` | Last-Event-ID；200 `text/event-stream` | 服务端事件流（Server-Sent Events，SSE）报告提交、目标进展、完成、失败和漂移；不是逐笔交易通道 |
| `GET /v1/config/schema` | 200 schema | 获取服务支持的数据 schema 与语义校验编号 |

`PlanRequest.requested_target_ids` 是操作者期望的范围，服务端计算完整依赖闭包。涉及账户的策略、exec、guardian、复制接收方和所需控制进程不能被选择器排除。计划将作用域分成三部分：`mutation_scope` 是实际改变的业务字段，`dependency_scope` 是这些变化要求改变行为的依赖域，两者都必须落在调用者权限内；`delivery_scope` 是由系统服务传播完整新 revision 的副本接收集合。对未改变业务行为的其他账户进程，系统执行 `NoOpRevisionAdvance`，这不授予调用者它们的配置权限。

每次 revision 的全部副本接收者仍属于完成目标，必须更新本机文件和事实。服务端在 commit 固定完整 target set，保存 membership hash、进程稳定身份和预期代次；执行中不能静默删除离线目标来制造成功。RollingRestart 的 target 还必须含 `planned_successor`，按第 5.4 节在固定计划内授权旧实例到新实例的转换。窄范围调用者看到 `view_scope=scoped` 的计划和操作：diff 以及 target 明细只显示其有权读取的部分，其余 delivery 接收者只显示总数、完成数和集合哈希。Success 以及 effective 的 `complete` 始终根据服务端完整集合计算，不能根据可见子集计算。

### 4.1 窄账户修改

拥有完整读取和修改作用域的操作者可以提交 `format=toml/json` 的完整候选。单账户操作者先读取 `/head`，再提交 `format=scoped-patch`，由服务器加载相同 ETag 的完整基线并合并。补丁对象包含 `base_etag` 与有序 `changes`；计划的 If-Match 必须与内含 base_etag 相同。例如，只降低 spot-main 账户引用的独占风险政策：

```json
{
  "format": "scoped-patch",
  "document": {
    "base_etag": "\"cfg-41-<64位哈希>\"",
    "changes": [{
      "op": "replace",
      "selector": {"collection": "risk_policies", "id": "spot-risk"},
      "path": "/max_order_quote",
      "value": "500.00"
    }]
  }
}
```

这是占位示意；实际 ETag 必须使用完整服务端值。selector 以稳定 ID 定位现有对象：accounts/risk_policies/venues/processes 使用其 id，strategies 使用唯一 process_id，strategy_risk_budgets 使用 strategy process_id 加 account_id。path 只允许对象字段名，不允许数组下标、空根路径、路径逃逸或引用遍历；修改数组只能显式替换整字段，服务端重新计算其全部影响。add 只增加允许的对象属性，remove 不能删除 schema 必填项；新建账户/策略等集合成员走完整作用域的发布计划。

服务端先校验 selector 与全部路径的读取/写入权限，再加载和合并；未授权 selector 无论是否存在都返回相同 403。只提供 add/replace/remove，不提供可能读取其他路径的 test/copy/move。合并后的完整树仍经过全部 schema、最终 diff、风险和依赖授权校验。若某个风险政策或策略被两个账户共用，修改它产生的另一个账户属于 dependency scope，单账户权限就不能提交；不能因入口 selector 看似属于 spot 而跳过最终授权。校验错误、预览和审计响应不回显无权读取的旧值。

系统把补丁转成完整规范化候选并计算完整 hash，绑定进不可变 plan；API 不回显其他账户内容。整个配置 revision 的并发控制保持统一，其他账户先提交也会使旧 ETag 冲突，客户端重新读取 head 和计划。

调用示意，数值和 ID 由服务器返回：

```http
POST /v1/config/commits
If-Match: "cfg-41-<64位内容哈希>"
Idempotency-Key: <调用者生成的UUID>
Content-Type: application/json

{
  "plan_id": "<服务器返回的plan UUID>",
  "candidate_hash": "sha256:<计划中的64位哈希>",
  "accepted_actions": ["freeze:spot-main"],
  "reason": "降低账户单笔金额上限"
}
```

以上尖括号是说明文字，不是合法请求。真实请求使用完整 schema 验证，不接受占位符。

### 4.2 幂等、抢占与状态

幂等键绑定 `(principal, endpoint, Idempotency-Key, canonical_request_hash, base_If-Match)`，与操作映射一起持久保存。相同键、相同内容重试返回原 plan/operation；即使当前 ETag 已变，也先匹配已有记录再返回。相同键不同内容返回 409。控制提交前断连时，客户端以同键重试，服务器只允许一份准入结果；提交后断连也不会创建第二个 revision。参考键映射保留 90 天，历史操作 ID、revision 和审计身份不能复用；客户端不能在失去键记忆后盲目重新发起。

同一集群同时只允许一个正在应用的配置操作。新的 desired 必须基于当前完整 ETag；前一个操作 Stalled 时，先恢复它，或在新 `PlanRequest.supersedes_operation_id` 中明确接管其残留目标与有效版本差异。后者的候选 diff 同时比较 desired 和各 target effective，必须覆盖旧操作的未决屏障，不能仅比较两个 desired 文件后认为无需动作。提交替代计划时，旧操作转为 Failed，`superseded_by_operation_id` 指向新操作；其历史 receipts 不改写。Failed 但仍有版本差异的操作同样要求这个显式接管关系。

状态流为 `Committed → Preparing → Quiescing（按需）→ Applying → Materializing → Succeeded`。临时磁盘/网络/依赖失败或操作超时进入 `Stalled`；永久校验失败或被合法替代进入 `Failed`。202 仅表示操作已经持久准入；API 不能等待超时后返回一个虚假的成功。Stalled、Failed 的 desired 仍是已提交事实，不自动退回先前版本。

`Operation.targets` 返回每个目标的 `desired_revision/hash`、`runtime_revision/hash`、`effective_revision/hash`、三者的 projection hash、预期 projection hash/policy revision、运行代次、applied 事件序号、两个文件收据、locator generation、风险闸门与错误。这些 `*_hash` 保留全局配置身份，`*_projection_hash` 表示该进程的权限 payload。`Succeeded` 要求整个固定目标集合具有匹配全局 revision/hash、各自预期 projection hash/policy revision 以及有效实例和 locator generation 的运行时事实与持久文件收据；控制组再次持久保存完成记录后才对外报告。历史成功描述当时完成事实；之后进程崩溃或新版本变化不改写它，当前健康状况由 effective 查询另行报告。

退出进程仍需在最后一个事件边界应用退休配置、保存 effective、返回收据，再由 supervisor 保存退出证明，目标才为 `Retired`。若进程已崩溃，先恢复受控实例完成退休，或另建显式恢复/退休计划；不能用“目标已从进程列表删除”替代上述事实。

watch 的事件 ID 单调且持久化，交付至少一次；客户端按 ID 去重并携 `Last-Event-ID` 重连。事件已压缩返回 410 `WatchCompacted`，客户端重新读取 desired、effective 和 operation 后续接。SSE 的连接心跳不证明配置或交易授权有效。

### 4.3 身份、角色与错误

双向传输层安全协议（mutual Transport Layer Security，mTLS）校验客户端和服务端身份。基于角色的访问控制（Role-Based Access Control，RBAC）把已认证主体映射到角色和账户域。证书过期、撤销、签发者和 SAN 不匹配均拒绝；重试时也重新授权，不能用幂等结果查询绕过撤权。

| 角色 | 允许动作 |
|---|---|
| `viewer` | 查看本人授权作用域的 schema、配置、diff、operation 和 watch |
| `config-editor` | 创建候选计划；无提交权 |
| `config-operator` | 提交和恢复经校验计划；仅覆盖已授账户/进程范围 |
| `risk-admin` | 与 operator 联合授权风险放宽、降低持久化等级及显式风险动作 |
| `cluster-admin` | 与 operator 联合授权身份、控制成员、信任根、主机和部署变更；不隐含 risk-admin |

返回统一 `Error{code,message,request_id,retryable,operation_id?,field_errors[]}`。400 为语法/编码错误，401/403 为认证/权限，404 为不存在，409 为计划/幂等/操作冲突，410 为过期计划或压缩游标，412 为 ETag 冲突，413 为尺寸超限，422 为配置/风险/依赖校验不通过，428 为缺少 If-Match，429 为限流，503 为控制组或必要依赖不可用。

已准入操作的写盘失败保存为 target error `LocalPersistenceFailed`，operation 进入 Stalled；查询该资源可返回 HTTP 200，但状态必须保留失败事实，绝不能返回 `Succeeded`。运行时变更禁止、风险超限、发布文件/凭据无效、回滚不安全和漂移分别使用 `RuntimeChangeForbidden`、`RiskExposureExceeded`、`ArtifactUnavailable/SecretReferenceInvalid`、`UnsafeRollback`、`ConfigDrift`。校验错误只包含允许公开的字段路径和引用，不回显可能含凭据的原始值。

## 5. 本机配置文件如何同步到可恢复状态

每台机器的 supervisor 内有独立配置物化服务，按稳定进程身份写入；它不在订单热循环中执行。参考 supervisor 使用 `tickwire-config` 操作系统身份，独占配置目录写权限：

```text
/var/lib/tickwire/config/<cluster-id>/<process-id>/
  desired.toml
  effective.toml
  last-good/<revision>-<config-hash>.toml
  instances/<replacement-slot-id>/desired.toml
  instances/<replacement-slot-id>/effective.toml
  config.journal
```

完整集群配置只存于控制状态和配置物化服务的私有 authority 目录；不把全量文件或全量 FD 交给策略和其他普通进程。投影器按固定 `local_config.projection_policy_revision/projection_policy_ref` 从已提交完整配置计算每个 target 的 payload。策略只得到自己的发布文件、参数、风险预算、订阅与无凭据账户视图；exec/guardian 只得到自己授权风险域和用途的凭据引用；控制、复制和 metrics 身份也按用途过滤。字段包含规则与完整对象授权都属于投影政策，隐藏整个不需要的字段，不能靠“接收方只使用自己的部分”保护其他内容。

`desired.toml` 与 `effective.toml` 使用 [ProcessProjection](../../specs/contracts/system-config.schema.json#/$defs/ProcessProjection)：包含 `projection_schema_version`、权限内 `payload` 和 `_materialization`。它们独立校验，不要求补齐全量配置的必填域。`projection_hash=SHA-256(JCS(payload))`；每个目标的预期值和 policy revision 在计划中固定并由控制提交证明绑定，接收方不能自行扩展字段后重算哈希。不同目标的 projection hash 可以不同，相同全局 revision/hash 不证明收到的 payload 有权访问。

`_materialization` 记录 `revision/global_config_hash/projection_hash/projection_policy_revision/projection_kind/target_process_id/instance_slot_id/operation_id/control_commit_ref/process_boot_id/applied_event_seq/journal_receipt_ref/render_version`。desired 尚未应用时 applied_event_seq 为 0；初始化尚无目标 boot 时使用全零 UUID，不能伪装为 runtime receipt。effective 必须对应真实应用记录。文件由配置服务以 0600 保存；启动器验证后向目标交付其权限投影的只读 FD 或经过身份认证的对象，不把生成文件改成所有进程可写。

### 5.1 单机顺序

1. **验证控制提交。** 物化服务检查控制提交证明、目标身份、预期实例代次、schema、投影 policy revision/预期 hash、全部依赖和操作幂等映射。旧 revision 或相同 revision 不同 hash 拒绝；相同身份、全局版本和投影返回已有进展。
2. **准备恢复证据。** 对交易相关变更按计划关闭增险闸门。物化服务在私有 journal 保存 `ConfigPrepared(old_revision/hash,new_revision/hash,operation,target,commit_ref)` 与完整配置对象引用；逐目标 journal 只保存该目标的权限投影、绑定和恢复证据。相关记录与依赖先完成同步。任何文件操作前保留当前已验证 effective 为 last-good，至少保留最近两代；它不是自动回滚授权。
3. **持久保存 desired。** 在目标目录创建独占临时文件，写全权限投影 TOML，校验独立 schema、全局绑定、projection/render hash，设置属主和模式，`fsync(temp)`，同目录原子 `rename(temp,desired.toml)`，再 `fsync(directory)`。随后 journal 保存 DesiredMaterialized 与文件收据。这个步骤完成才允许运行时开始应用。
4. **在事件边界应用。** exec 将 `ConfigApply` 导入本风险域日志，保存新/旧全局版本、projection hash/policy revision、控制证明、策略/风险政策版本和精确事件边界；日志及依赖完成本地持久化后，单写 reducer 才替换不可变配置视图。同步提交模式对需要进入恢复闭包的记录遵循其镜像持久化规则。其他进程使用自己的可靠配置 journal 和输入 sequencer 固定应用位置。事件处理不可跨边界混用旧/新参数；引用旧配置的未完成意图保留原版本，并在最终发送闸门验证新约束。
5. **持久保存 effective。** 运行时返回包含本次 boot、实际事件序号、revision/hash 与已持久 ConfigApply 证明的 applied receipt；物化服务核验后，按相同 `temp → fsync → rename → directory fsync` 顺序保存 `effective.toml`。再把 EffectiveMaterialized 收据写入本地 journal 并同步。成功响应同时包含运行时 applied 和文件持久收据，不能只上报已 rename。
6. **汇总与解闸。** 控制组收齐固定目标集合的收据，校验相同全局 revision/hash、各目标预期 projection hash/policy revision、实例和 locator 代次及 target membership，并持久写入 OperationSucceeded。涉及跨账户的一致风险变化时，所有参与域在应用和文件收敛之前保持组增险屏障；放开屏障要有新的有效授权。降限额后仍超限的域按计划继续 Frozen，事实与减险处理持续运行。

`fsync(file)` 只保证该文件相关内容持久；目录条目另需同步目录。即使 rename 对同一目录的可见性是原子的，它也不构成多文件、跨进程或跨主机的事务。[Linux fsync 语义](https://man7.org/linux/man-pages/man2/fsync.2.html)

desired 和 effective 的短暂不同是正常状态，查询必须如实显示。跨进程更新以持久 operation、事件屏障和逐目标收据收敛，不能许诺集群内存与所有配置文件瞬时原子变化。对独立诊断 Hot 更新无需全域交易屏障；所有目标的文件成功条件仍相同。

内部传输使用[IPC ABI](ipc-abi.md)的 `CONFIG_PREPARE/APPLY/RUNTIME_APPLIED/FILE_DURABLE/STATUS`（操作码 80～84）。固定 ConfigControlBody 绑定 operation、稳定 process/host、预期实例、successor 证明、全局版本/哈希、投影政策/哈希、对象哈希、控制提交证明与收据。完整 payload 采用 [ProjectionObject](../../specs/contracts/system-config.schema.json#/$defs/ProjectionObject)，经认证对象服务或已封印的只读 FD 交付；不在固定控制帧中嵌入 TOML。`projection_object_hash` 校验完整交付字节，`projection_hash` 校验规范化权限 payload，二者不可混用。

参考共享投影对象额度为 128 个，每个 payload 至多 4 MiB，另有 4,096 字节 ResourceHeader；预算为 `128×(4,194,304+4,096)=537,395,200` 字节。它与 bank、cursor、快照和 256 MiB relay 合计 6,449,856,512 字节，在 8 GiB SHM 中保留 2,140,078,080 字节余量。滚动并存和旧读者 pin 也消耗这项额度；计划器按余量分批发布，额度耗尽时等待或 Stalled，不覆盖旧对象或无限扩容。

只有 runtime 身份能出具 RUNTIME_APPLIED，只有物化服务能出具 FILE_DURABLE，控制组核对双方后才能以 STATUS 宣布 Succeeded。提交证明和 receipt 的 HTTP 引用使用内容寻址对象，wire 字段取对象 SHA-256；哈希只标识内容，验证器仍须验证 issuer、boot 和 journal 来源。收据正文不能递归哈希自身 locator：外部 `receipt_ref` 不属于被引用收据的哈希正文。RollingRestart 的 APPLY 绑定已经 SuccessorBound 的新实例，同时校验它属于原计划，不能把任意新 boot 当作合法继任者。

### 5.2 写盘失败、崩溃和应答丢失

| 故障切点 | 恢复和对外状态 |
|---|---|
| 控制提交前 API 断连 | 无已知结果时按原幂等键重试；只允许一份提交记录 |
| 控制提交后 202 丢失 | 同键返回原 operation；不新建 revision |
| ConfigPrepared 前失败 | 目标未接受，旧 effective 继续有效；operation 等待或 Stalled |
| desired temp 写入/同步/rename/目录同步失败 | 不应用运行时；保留旧有效配置和 Prepared，报告 LocalPersistenceFailed；孤立 temp 在核验后清理 |
| desired 已持久、ConfigApply 尚未提交 | desired 可领先 effective；重启验证 journal 后重新检查前置条件，不能仅凭 desired 文件直接交易 |
| ConfigApply 已持久、运行时替换前崩溃 | 重放 ConfigApply 在同一事件顺序恢复新配置；不重新执行已编号经济动作 |
| RuntimeApplied 后 effective 写入失败 | 运行时可能已是新版本；保持/重新关闭受影响增险闸门，报告 Stalled；修复后补写同版本文件，不能声称成功或盲目回退内存 |
| effective 已同步，完成收据尚未持久 | 重启交叉核验文件 hash、ConfigApply 和事件边界后补记收据；mtime 不构成证据 |
| 本地收据已持久，上传 ACK 丢失 | 重发相同收据；控制组按 operation/target/revision/hash 去重 |
| 部分主机应用完成，另一台离线 | 逐目标显示差异；组屏障保持；不能把离线目标从集合抹去；超时 Stalled |
| 完成后某主机永久丢失 | 历史成功不变；按复制/恢复契约重建主机和文件；旧 ACK 不证明新主机已应用 |
| 文件内容、属主或路径漂移 | `ConfigDrift`，隔离被改文件，按策略冻结受影响增险；从控制已提交配置与本地 applied 日志重建 |
| journal 中部损坏或配置对象缺失 | `StorageQuarantined`，停止激活，使用可验证副本恢复；不能选择看起来更新的 TOML 继续 |

文件持久化失败发生在运行时应用之后时，ConfigApply 的预先持久化保证重启可以恢复新版本。若磁盘全面失效无法再记录错误，本进程和 supervisor 仍需立即禁止新风险并对外报告失败；缺少持久失败日志不会把已知失败变成成功。

### 5.3 重启选择哪份配置

重启先验证本地 journal 的完整提交前缀及其配置对象，再选择与最近可恢复 ConfigApply 对应的 effective。若 effective 落后而 journal 已证明更高版本完成应用，重放并补写新 effective；若 effective 超前且找不到对应 ConfigApply，视为漂移或存储故障，不直接采用。

desired 只表示控制组意图，用来恢复尚未完成的 operation；它不能替代本地 applied 事实。last-good 只用于解析/恢复候选和故障排查，不能在控制组已提交新政策之后自动恢复旧的、更宽松风险约束。启动成功后还要核对控制 desired、实例代次、账户事实和新授权；控制组不可达时可以恢复只读账户视图和预授权紧急行为，不能获得新的正常增险权。

### 5.4 RollingRestart 的计划内实例转换

稳定 `process_id` 表示部署位置，不等于某次进程 boot。计划给 RollingRestart target 保存旧 `expected_boot_id/expected_exec_generation`，同时预分配 `planned_successor={replacement_slot_id,host_id,generation,artifact_hash,bootstrap_nonce}`。这些值参与 membership hash。受认证启动器使用一次性 nonce 启动指定发布文件，并将实际新 boot 绑定到该 slot；控制组和本地 journal 持久记录 `SuccessorBound`。nonce 只用于一次绑定，同 slot 不接受第二个 boot，新 boot 的主机/发布文件/代次必须与计划一致。

旧实例先完成 `PredecessorQuiesced`，exec 还需持久撤销其发送资格，保存交接水位和恢复闭包。按角色要求取得真实退出或隔离证明，记录 `PredecessorRetired`；资源引用没有释放时保留旧 bank。新实例可以先无交易权预热，但不能与旧实例共享发送权。控制面普通 leader 变更不改变 target boot，不需要重做该步骤。

两个实例并存时，各自在 `instances/<slot>/` 下保存配置；唯一物化服务写这些文件，旧进程只持自己旧文件的只读 FD，新进程只读 staging 文件。稳定目录中的 desired 可先反映新期望，effective 在切换前仍表示旧实例事实。配置服务取得旧撤权/退休证据、新 `ConfigApply` 与 staging effective 持久收据后，先同步 `ConfigOwnerCutoverPrepared(old_slot,new_slot,receipts,revision,hash)`，再把经验证的新 effective 渲染到稳定目录的临时普通文件，完成 fsync、rename 和目录 fsync，最后同步 `ConfigOwnerCutoverCommitted`。

稳定 effective 不是符号链接，metadata 指向新 boot；文件收据带 `instance_slot_id`。控制组把 `successor_binding_ref/predecessor_quiesce_receipt_ref/predecessor_retirement_evidence_ref/owner_cutover_receipt_ref` 与新 runtime/file 收据一起核验，这个 target 才算完成。计划内 successor 满足原 operation 的固定目标，不需要因正常滚动代次变化重建计划；resume 可以沿同一绑定继续。计划外崩溃重启、更换 build/host 或第二个 successor boot 则需要新计划和明确接管关系。

切换准备后崩溃时，恢复器检查旧撤权/退休和新 staging 收据；稳定文件尚未换好则继续安装同一新版本，已换好则核验后补记 CutoverCommitted。它不凭目录文件新旧恢复旧发送权。退休旧实例、安装稳定文件与释放新实例风险权仍是有顺序的操作，跨主机无法合为一个文件系统事务。

### 5.5 配置根目录迁移与固定引导位置

`local_config.root_dir` 可以通过 RollingRestart 更改；启动器不能从待迁移目录中的配置推断到哪里找配置。每台主机在部署引导时固定 `/etc/tickwire/bootstrap/config-locator.toml` 及同目录 `migration.journal`，目录由平台管理员所有并按无符号链接规则保护。schema 中 `bootstrap_locator_path` 固定这个发现位置；更换这一级信任锚属于离线重新引导，不由正在运行的配置 API 自改。

[ConfigLocator](../../specs/contracts/system-config.schema.json#/$defs/ConfigLocator) 保存 `locator_version/generation/cluster_id/active_root/migration_id/manifest_hash/previous_locator_hash/checksum`。generation 严格递增，checksum 为去掉自身字段后的规范化 locator 哈希；previous_locator_hash 绑定上一份完整 locator，初代用全零哈希。manifest_hash 指向迁移文件清单，包含相对路径、大小、字节哈希、配置 journal 完整前缀水位和配置/依赖收据。locator 只是引导存储位置，不能颁发运行权限或覆盖控制配置权威。

迁移按下面顺序执行：

1. 在控制计划固定 migration ID、旧 locator generation/root、新 root 和预期目标。验证新目录所有者、设备、容量和 fsync 能力。暂停该主机配置应用入口，按计划冻结受影响交易行为；唯一物化写者排空在途文件操作，停止追加本地配置 journal，形成完整前缀 S。
2. 在固定 bootstrap `migration.journal` 同步 `MigrationPrepared(old_root,new_root,old_generation,new_generation,S,operation)`。旧写者保持 Quiesced；固定 bootstrap 目录上的内核独占锁和 supervisor 实例身份共同保证只有一个物化写者。若旧 supervisor 无法确认停止，先证明其进程已退出或隔离；不能仅等一个超时就启动第二个写者。
3. 将完整私有 authority 配置、逐进程 desired/effective/last-good、staging、配置 journal 至 S 的前缀与仍被引用的依赖复制到新 root。使用新 inode，不通过硬链接让新旧目录共享可变文件；逐文件校验并 fsync，逐级同步目录，再重新读取核对 manifest。完整后在固定 journal 同步 `MigrationCopied(manifest_hash)`。
4. 通过 `temp → fsync → rename → directory fsync` 把固定 locator 切到 `generation+1/new_root/migration_id/manifest_hash`。此点决定以后从哪个根目录恢复。随后固定 journal 同步 `MigrationActivated`，唯一写者重新打开新 root 并核验 locator generation 后，才恢复 journal 追加、运行时应用和物化。旧 root 保持只读/退休；写入收据绑定新的 locator generation。
5. 从新位置继续原 operation，把新 root 设置应用到所需投影，并完成文件收据。所有目标 runtime、文件、预期投影及 locator generation 都匹配后才能 Succeeded。旧根目录要等完整迁移证明、全部目标完成、旧实例/FD/恢复 pin 释放及 last-good 保留条件满足后才删除；删除也同步其父目录。

启动器只信固定 locator 和固定 migration journal，绝不扫描两棵目录挑选 mtime 较新者。locator 已切到新 root、effective 仍是旧配置值时，MigrationActivated 证明这是一项尚未完成的目录迁移；先按 locator 恢复，再续做 operation，不被旧 `root_dir` 字符串带回原目录。旧目录不能因仍然存在或旧 supervisor 重启而重新成为写者。

| 迁移故障切点 | 恢复结果 |
|---|---|
| Prepared 前失败 | locator 仍为旧 root；原写者可以继续，未发生迁移 |
| Prepared 后复制失败 | 旧 root 和前缀 S 保持权威；保持暂停，修复后重试复制，不能开放新写者 |
| Copied 后、locator 提交前崩溃 | 固定 locator 仍指旧 root；校验完整复制收据后继续切换，或按显式操作废弃未激活副本 |
| locator rename 可见但目录同步失败 | 持久结果不确定；保持冻结，重读 locator、固定 journal 与 manifest，补同步确认后才继续，不返回成功 |
| locator 已持久、Activated 未记录 | 恢复验证新 root 的 manifest 和 S，补记 Activated，从新位置继续；不自动回退旧 root |
| 新写者已启动、旧 root 未退役 | 固定 locator generation 和唯一写者锁只授权新 root；旧目录只作保留副本 |
| 新 root 后续故障 | Stalled/StorageQuarantined；用验证过的复制事实恢复，或通过新 migration ID 和更高 generation 迁回。不能重启旧写者来绕过新版本 |

此协议迁移的是配置存储。`journal.root_dir` 改动还必须按[持久化与接管](replication-and-failover.md)封口业务日志、搬运完整恢复闭包并验证新位置，不能复制正在写入的订单 WAL 后重开。审计目录迁移同样需要其写者屏障和完整尾部证明；配置 locator 先使启动器找到经验证的有效投影，再由该投影和对应存储迁移证明定位其他目录。

## 6. 安全文件写入与凭据管理

物化根目录由部署管理员创建并固定所有者，生成服务持目录文件描述符进行相对操作。文件描述符（File Descriptor，FD）引用已经打开的文件或目录；这里使用它避免在字符串路径验证和实际打开之间被替换路径。实现采用 `openat2` 的 `RESOLVE_BENEATH/RESOLVE_NO_SYMLINKS` 或等价的逐级无符号链接检查，不跟随 symlink，不允许 `..`、绝对子路径、非预期 mount、设备文件或目录属主漂移。临时文件使用不可预测名字、`O_CREAT|O_EXCL|O_NOFOLLOW`，生成后校验 regular file、属主、mode 与 link count；rename 的目标和临时文件必须在同一受控目录和文件系统中。平台缺少相同保护时停止物化，不能降级为任意路径写入。

secret 引用由进程身份和用途授权：策略没有正常下单凭据；exec 与 guardian 分别解析自己有权使用的凭据。凭据只在运行时签名/握手边界存在，配置、candidate、diff、operation、TOML、last-good、journal 和审计仅保存引用。API 拒绝明文 `api_key/private_key/password/token` 类字段；策略参数 schema 不得声明用于夹带凭据的自由文本入口。正文、错误和日志不得回显未通过校验的原始输入。

引用必须固定版本，禁止 `latest`。轮换先验证新身份与权限，建立新会话并完成恢复，再切换后续动作；旧未知订单继续绑定原会话和 ID。回滚时旧凭据版本已撤销，则计划拒绝或选用新的有效版本，不能为了恢复旧 TOML 重新启用失效凭据。紧急撤销发生后立即收紧执行权限，不等待常规配置 operation 收敛。

审计记录调用者、证书身份、原因、base/new revision、hash、计划/操作 ID、分类、目标、授权决策、应用水位、文件收据和错误。完整 desired/revision 配置要求调用者有其中全部账户和集群拓扑的读取权，不向只具单账户权限的用户返回其他账户引用。窄范围身份使用 head 与 scoped-patch，并查询自己可读的 effective 和 plan/operation 视图；可见视图明确标识 scoped，并保留完整 delivery 的不透明集合哈希。完整配置接口不返回缺字段的脱敏配置树，避免把不可再次提交的内容误认作候选。

## 7. 回滚与重新计划

回滚读取旧 revision 的配置值，创建新候选，再执行正常 plan/commit。它只恢复配置意图：成交、费用、持仓、预留、订单去重、client order ID、高水位、策略/执行代次以及撤销授权不回退。旧代码缺少当前状态 schema、旧标的过滤器已失效、旧凭据已撤销或当前持仓不满足旧模式时，返回 UnsafeRollback 或形成新的迁移计划。

例如将总毛敞口从 100,000 降至 50,000 后，已经产生的持仓仍为 70,000；freeze_until_within 可以使新限额生效并保持 Frozen。随后“回滚到 100,000”是一项风险放宽，需要新的 risk-admin 权限、版本和资格屏障。取消请求已经发出时，回滚限额也不会复活该订单或重发原交易。部分目标更新失败时，优先补齐同 revision；选择恢复旧参数则通过新 revision 对所有目标重新计划，不逐台手工覆盖 effective。

## 8. 实现与资格验证任务

设计产物校验包括 TOML 解析、JSON Schema 自检与实例验证、OpenAPI 3.1 校验、跨字段引用/容量/期限检查、生产模板的拒绝结果及跨文档参数一致性。它们不表示配置 API、文件持久化或热更新已经运行。

实现必须将以下案例加入[验证计划](../verification-plan.md)：

| 编号 | 测试与必须观察到的结果 |
|---|---|
| CFG-01 | 同一 TOML/JSON 候选产生相同规范化内容哈希；未知字段、小数浮点、重复键和类型混淆被拒绝 |
| CFG-02 | 同 ETag 并发提交只准入一个；失去响应后同键返回原 operation；相同键不同内容冲突 |
| CFG-03 | API 更改后所有目标 desired/runtime/effective 同全局 revision/hash，逐目标匹配预期 projection hash/policy revision；两个文件可按独立schema解析并与收据一致 |
| CFG-04 | 在每个 write/fsync/rename/directory-fsync/journal/ACK 切点注入崩溃；恢复只使用可证明的配置，不重复经济动作 |
| CFG-05 | ENOSPC、EIO、只读盘、权限变更和目录同步失败均不能得到 Succeeded；RuntimeApplied 后失败仍冻结并恢复同版本 |
| CFG-06 | 节点离线、代次切换、部分应用、控制 leader 切换与超时可查询；不删除目标、不把 Raft commit 当完成 |
| CFG-07 | Hot/Quiesce/RollingRestart/ForbiddenRuntime 与字段表一致；风险放宽、mode、endpoint、ABI 不能绕过校验 |
| CFG-08 | 降限额覆盖持仓、挂单、pending、Unknown 和条件子单；三个 exposure policy 行为可区分，撤单未终态不释放 |
| CFG-09 | live 拒绝全部 fixture/REQUIRED/未解析依赖；凭据只通过 secret refs；日志、文件和错误没有 secret bytes |
| CFG-10 | 手改文件、mtime 伪造、symlink/hardlink、路径替换与权限漂移触发隔离；生成文件不回流成第二写者 |
| CFG-11 | 旧配置回滚遇到新成交、新状态 schema、撤销凭据或变化的账户模式时重新校验；经济事实与代次不回退 |
| CFG-12 | 订阅变更和 bank 扩容计入冻结资源；三种 lease/freshness 独立；到期合约保护按固定目录与结算事实执行 |
| CFG-13 | 形式模型探索 commit/apply/file-durable/ACK/崩溃交错；不变量为 Success ⇒ 全部目标应用与文件证据匹配，旧 revision 不能复活授权 |
| CFG-14 | 窄账户 scoped-patch 只能改变获授权 mutation/dependency；数组下标、test/copy/move 和共享政策越权被拒绝；NoOp delivery 不扩权也不减少完成集合 |
| CFG-15 | 完整配置仅物化服务可读；每角色投影字段/凭据引用严格按政策过滤；全局 hash 相同但 projection/policy 不符的 APPLY/receipt 被拒绝 |
| CFG-16 | RollingRestart 只接受计划内 successor slot/build/boot 绑定；旧撤权/退休、新 staging、OwnerCutover 每个切点崩溃后保持单写者和代次正确 |
| CFG-17 | root 迁移覆盖 Prepared/Copied/locator rename/目录同步/Activated/旧根退役切点；启动器只选固定 locator，双写、按 mtime 选根和漏算 locator generation 的完成收据均被拒绝 |

机器可读契约固定了字段、端点和静态边界；实际掉电持久性、多进程事件边界、凭据存储接入、真实 Binance 会话和更新期间完整延迟必须由实现阶段提供相应证据。

## 9. 规范依据

- [OpenAPI 3.1.1](https://spec.openapis.org/oas/v3.1.1.html)：接口描述和 JSON Schema 兼容模型。
- [JSON Schema 2020-12](https://json-schema.org/draft/2020-12)：结构、类型和条件约束。
- [RFC 8785：JCS](https://www.rfc-editor.org/rfc/rfc8785.html)：规范化 JSON 内容身份。
- [Linux fsync](https://man7.org/linux/man-pages/man2/fsync.2.html)、[rename](https://man7.org/linux/man-pages/man2/rename.2.html)、[openat2](https://man7.org/linux/man-pages/man2/openat2.2.html)：本机同步、原子替换与目录约束。
