# 策略投影参数的实际消费

Status: implemented

已实现普通 registered 策略首次启动时的参数消费与交付门禁，完整全局 Applied 仍为部分实现。全局投影的 `strategy.params` 和 worker 的私有配置来自不同对象；本模块将批准的参数内容与真实 worker 的新鲜检查点相连，分别保存全局修订、投影摘要，以及私有配置修订、原始字节摘要。

## 启动输入与内容核对

App 的 `HostConfig.projection_parameters` 显式批准完整 ProjectionObject 文件及 ObjectBinding、cluster/namespace/host/process 范围、参数格式定义文件和有界查询期限。它只消费策略参数；资源限制仍通过原 `projection_launch`/`resources` 落实。两类绑定同时启用时必须指向同一个完整投影与范围，任何失败都不得回退或忽略已启用的约束。

只接受普通 registered Active/Shadow 首次启动；capture、预恢复任务及轮换拒绝。沿原有角色、完整本地通道身份、logical strategy、namespace、程序摘要和精确实例核对投影。检查点查询能力必须显式可用，使用既有 handover_enabled 路径；Shadow 保留原开启方式。旧配置省略本字段时保持原序列化与行为。

`tickwire_config::artifacts::validate_parameter_document(schema_bytes, expected_schema_hash, parameter_bytes)` 按投影的 `parameter_schema_ref` 核对格式定义原始字节的 SHA-256。格式定义最多 1 MiB、32 层，采用既有离线 JSON Schema 2020-12 和有界正则校验，拒绝外部引用。私有配置仍使用 Bootstrap 中 FactoryBinding 批准的绝对路径、原始对象摘要与修订，最大 2 MiB。严格 JSON 解析拒绝重复键、浮点、超范围数值、过深内容及尾随数据；参数对象最多 16 层，顶层包含 1–256 个属性。返回的 `Candidate` 保留规范参数值，App 将其与投影完整比较。对象键序或无关空白差异不改变规范值，工厂仍读取原始字节。

参数格式定义是批准的输入校验契约。实际工厂仍执行自身解码与语义检查；本模块不声称工厂消费了格式定义文件，也不新增静态工厂能力声明。

## 真实消费与交付门禁

SDK 的 registered 路径把精确 `VersionedBytes` 交给实际 `Registration::configure`，再检查工厂回读；create 成功后继续核对 replay_registration。运行时将该配置写入持久 `RuntimeState`。Host 的 `FreshObservation::configuration()` 从已验证完整输入封口的当前合并检查点提取 `RuntimeState.config`，返回字段私有的 `ObservedConfiguration`，保留 Source、当前 cut、检查点对象、配置修订与原始字节。读取启动 anchor 或任意同摘要对象无法替代这一提取过程。

配置事实中的 Source.clock 必须匹配本次查询实际使用的逻辑 ClockDomain 及批准 Identity.host_boot。内核 boot UUID 是另一项实际进程事实，仍由前后采样与挑战绑定；本模块不增加逻辑时钟域 ID 必须等于内核 UUID 的启动限制。

App 在实际 worker 启动后发出自己的新鲜检查点挑战。参数消费证明同时绑定原请求、实际 Child/映像/实例、当前完整闭包、私有配置的修订与原始字节，以及启动批准的投影/参数格式定义。历史 READY、旧挑战、错实例、错修订、不同配置和过期结果均拒绝。查询期限上限 30 秒，内核与持久读取完成后仍核对原期限。

证明完成前保留该策略的排队 intent，继续驱动真实行情与账户输入。参数核验失败关闭本次策略并清理受管子进程；成功后才向执行端交付原 intent。启动内部查询与外部观测互斥，外部调用不能取走或取消内部结果。`AppStrategy::projection_parameters(target)` 按精确逻辑策略、实例和代次返回字段私有的 `ParametersProof`，保存该时刻的实际消费事实；后续当前状态仍需重新观测。

启动参数查询最多每 10 ms 推进一步，未到期只比较单调时间和状态；账户基线未到达、Host 暂忙时也遵守该间隔，避免每次交易轮询读取进程信息。重试沿用原绝对截止点，期限判断先于冷轮询间隔，到期立即关闭，不延长查询。

本模块不生成全局 Applied 或 effective 双收据。风险预算、IPC、日志、超时、可观测性等其余角色字段须由各自实际消费者落实。

## 验收

定向回归已覆盖严格参数解析、当前配置事实提取及 App 内容与交付门禁。6 项实际 App 启动入口测试使用独立 counting worker 和父线程内的真实 MD JOIN server/Publisher，覆盖省略参数绑定的兼容路径、启动前值不符拒绝、真实消费证明、观察数与最低买价对实际效果的影响，以及查询等待和超时期间的 intent 门禁。效果断言读取实际消费水位与检查点计数；查询通过真实认证通道取得 `FreshObservation`，测试不向 gateway 提交订单。

普通启动兼容场景和启用参数绑定的真实进程正例分别保留证据。Linux 共享内存、认证通道与实际子进程是验收基础；本阶段全仓检查和最终固定产物验收另行记录，当前定向结果不替代这些检查。本轮不新增 TLA/TLC，生产性能与完整配置应用资格另行验证。
