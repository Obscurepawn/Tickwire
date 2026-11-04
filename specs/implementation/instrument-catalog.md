# 标的经济目录的实际消费

Status: implementation-in-progress

本模块将全局 `instrument_catalog_ref` 对应的目录正文连接到配置服务、实际 MD feed、账户基线与策略恢复依赖。当前覆盖 Binance Spot 和 USDⓈ-M 的现货、线性永续、线性交割经济定义。目录制品、规范经济内容和账户财务基线各有自己的摘要；费用、钱包、风险限额和交易授权继续由各自模块负责。

## 共享目录正文与身份

新增纯值 crate `tickwire-instrument-catalog`，仅依赖 `tickwire-venue`、serde、serde_json 和 sha2，无文件或网络 I/O。`Catalog::decode(raw, expected_artifact_sha256)` 先核对原始 SHA-256，再执行有界严格解码，返回字段私有的目录。原始与规范正文均至多 2 MiB、深度至多 16；资产至多 4096 项、标的至多 16384 项，两个集合均非空。下游账户、feed、topic 容量仍单独校验。

JSON 固定字段为 `schema=1`、`venue`（非零 u32）、`environment`（Production/Test）、`market`（Spot/Usdm）、`catalog_version`、`assets`、`instruments`。所有资产与标的的 ID、version、catalog_version、交割到期时间采用完整非零 u64 的十进制字符串，拒绝前导零、符号和溢出。未知或重复字段、尾随内容、浮点及超限集合均拒绝。

资产项包含 id、version、symbol、exponent；指数范围沿用 venue 的 -38..38。资产独立列出，允许包含交易对未引用的手续费资产。标的项包含 id、version、symbol、product、base/quote/settlement 资产 ID、price_tick、quantity_step 和 contract。价格及数量步长使用 venue Decimal 解析并要求规范正十进制字符串。合约是含 `kind` 的对象：Cash、Perpetual 仅有该字段，Delivery 另有非零 `expiry_utc_ns` 字符串；Spot 只配 Spot/Cash，Usdm 只配 LinearPerpetual/Perpetual 或 LinearFuture/Delivery。环境、市场、产品和 kind 均为字符串，拒绝其他 JSON 形态。资产引用全部解析到独立资产表，基础与报价资产 ID 必须不同；同类 ID 和 symbol 唯一。

资产与标的按数值 ID 排序，以固定 struct 字段顺序生成规范 JSON；这是此格式自己的规范表示。正文不含自引用的 catalog_hash，运行时目录身份为域前缀与规范正文串接后的 SHA-256。域前缀实际字节是 Rust `b"tickwire.instrument-catalog.v1\0"`，末尾为一个 NUL 字节。原始空白和字段顺序可以不同，原始制品 SHA 保留这种差异；规范经济内容相同则运行时身份相同。旧路径中的任意声明 hash 不自动升级为正文已核验的证明。

只读接口为 `raw_bytes/artifact_hash/canonical_bytes/catalog_hash/catalog_version/venue/environment/market/assets/instruments`，以及 `asset(id)`、`instrument(id)`、`asset_matches(AssetRef)`、`instrument_matches(Product, InstrumentBinding)`。每个只读 Instrument 提供 product 和 binding；binding 的目录版本及 hash 由当前正文派生。匹配比较完整字段和规范定点数，使用连续有序集合查找；逐消息路径无需重新解析或读取目录文件。

可序列化的 `Artifact { digest, bytes }` 保存原始制品，`Artifact::catalog()` 执行同一解码校验；`Catalog::artifact()` 在冷路径复制原文供 Bootstrap 和持久对象保存。[共享向量](../contracts/instrument-catalog-v1.json) 固定原始正文、规范字节、域前缀和双摘要；Rust 测试与[独立 Python 校验器](../../scripts/verify-instrument-catalog.py)读取同一份向量。

## 配置服务、行情与账户装配

配置服务在 `ContentValidated::check` 中读取每个全局 venue 引用的原始目录，核对正文与该 venue 的 Spot/Usdm、simulated/production 对应关系，保存全局名称、原始对象和实际数字 venue 的明确绑定。全局名称到数字 venue 的关系来自正文与部署批准，禁止按名称猜测。内容有效仍只是配置内容事实，完整 Applied 与运行时双收据另行接入。

MD 增加可省略的目录文件批准集合。文件原始 SHA 必须等于完整 artifact 引用；启用时所有实际 feed 都须对应其中一个目录，并逐项匹配完整 InstrumentBinding、venue、产品和环境。目录可以包含尚未订阅的标的。初启核验发生在创建连接、配额、memfd 和持久 Starting 记录前。Runtime 持有固定目录原文及只读索引；扩容在构造候选 topic、预留 owner 或提交 Build 前核对完整目标配置，新增成员须已存在于启动目录中。

MD 的持久快照同时保留批准与目录原文。集合最多 32 个，原始正文总量至多 2 MiB；记录继续受已有快照及总存储预算约束。恢复重算原文摘要和所有经济字段，使用原快照的固定内容。目录启用后，扩容或恢复中的删除、替换或降级为未启用均拒绝；路径替换也不会改变已运行进程持有的目录。跨目录经济规格更新留到后续独立变更协议。

App 的部署装配增加可省略的 `Deployment.instrument_catalog` 文件批准。`Deployment::assemble` 读取正文，把已验证的 `Arc<Catalog>` 保存到 `files::Configuration.instrument_catalog`，核对规范基线中的全部资产（包括手续费资产）、全部标的经济字段、scope 的数字 venue/环境，以及实际公开行情、私有账户和命令 route。`files::open_with_services` 在创建目录、日志 owner 和网络客户端前调用 `validate_instrument_catalog()` 再次核对装配对象，随后从这些已验证的输入构造 Node；Node 本身没有持续持有该 Arc 的承诺。费用、钱包与策略限额保留在原基线中。

策略启用经济目录时，App 要求同时启用 `projection_inputs`，并核对装配对象中的目录、Bootstrap 目录与完整 `venue_views.instrument_catalog_ref` 指向同一个原始对象。缺少任一边或任一字段不符均在 worker 创建前拒绝。实际消费证明沿已有共享 Fresh 查询取得 `ObservedInputs::catalog()`，要求它与当前检查点的完整账户、实际 JOIN 和批准相符。参数约束同时启用时使用同一个 Fresh 结果。新增可选字段省略时不写入旧编码，保持原启动与恢复行为；此时 `ObservedInputs::catalog()` 为 None。

## 策略回调、保存与来源重放

`Bootstrap.instrument_catalog` 内嵌原始正文与其 SHA，现有 kind9 依赖保存带该字段的 Bootstrap 正文，恢复依赖种类与固定 IPC 布局保持不变。启动时 `catalog::validate_dependency` 核对实际 Bootstrap 与命名 kind9 中的完整目录 Artifact、`market.local.identity()` 和 `clock_generation`；读取原路径后及 RecoveryWriter 复制依赖后各检查一次。这项检查不要求全部 Bootstrap 字段逐字相同，装配产生的 peer PID 等字段继续按原认证协议校验。Bootstrap 文件读取上限为显式 16 MiB，仍受完整恢复闭包预算约束。

Application 持有 `Arc<Catalog>`，Saved 仅保存原始目录的 Object（摘要与长度），避免每个检查点重复复制大目录。首个账户基线与 JOIN 经济身份核验前，市场、定时器、配置、模型和外部输入的用户回调保持关闭；市场记录的 invoked 标记在来源重放时按相同规则重算。账户基线、行情换源及输出命令均再次核对相应完整经济字段，逐条普通账户记录使用已经绑定的基线身份。目录成员在冷构造、恢复和换源核验时绑定；逐行情记录沿既有副本索引检查未知 ID 和版本，不重复查询目录。输出命令的产品与 InstrumentBinding 也须与目录相符。

`Application::restore_with_catalog` 在调用用户恢复工厂之前核对 Saved 绑定、所有账户和市场视图。原 restore 遇到带目录绑定的状态而没有正文时拒绝。目录正文通过独立依赖解析提供，恢复过程不从当前部署模板补齐历史缺失。

来源重放的 SourceJob 新增可省略的 kind9 ObjectFile。Host 从 S 和 C 两个真实封口提取相同的命名依赖，Release 验证完整对象与两端封口的关系。SDK 的 `catalog::resolve_dependency9` 再按 Source 核对实例、代次、逻辑策略、程序摘要和时钟 boot/generation，解析原目录并构造带目录的 ApplicationFactory。正文缺失、替换、与 Saved 不符或尾部输入改变经济身份时，均在用户恢复或相应回调前失败。

App 支持范围沿当前投影输入约束：普通注册策略 Active/Shadow 首启、一个账户、一个外部行情 topic，并要求 handover_enabled。SDK 支持原 Application 状态的确定性来源重放；未注册工厂的普通实时启动不支持目录字段。capture 的独立 Bootstrap 没有目录字段，向普通 Bootstrap 安装带目录的目标也会被拒绝；preflight、迁移安装、回滚探针、声明重建和轮换入口均在用户回调前拒绝带目录绑定的请求。现有 App 输入范围门禁继续阻止运行中换源及交接。在线目录重新批准、`allowed_symbols`、风险预算与完整全局 Applied 留待各自消费流程接入。

## 验收

先建立反例：原始 artifact SHA 合法且全局投影已重新绑定，仅改变 tick、资产定义、产品或到期时间，实际 feed/基线仍旧时应拒绝。正例核对全部字段、高位 u64 ID、独立手续费资产、规范身份、严格解析和容量边界；旧省略配置字节保持原样。

真实价格用例由父进程线程中的生产 MD Runtime 驱动 TLS/WSS/REST、Publisher 和 JOIN，再接入独立策略进程。两次输入均为原始字符串 `"1.00"`，批准 tick 为 `0.01/0.02` 时实际检查点盘口分别为 `100/50`，策略阈值 75 对应一个或零个意图；该用例不向 gateway 提交交易。分别保存阈值检查点与随后 Fresh 检查点，并核对同一来源、非回退序号、实际价格和效果。扩容与恢复另由 MD 进程测试覆盖未批准成员、路径替换、依赖缺失/篡改和禁止降级；来源重放覆盖原 kind9 和 Saved 的完整绑定。

完成后运行全 workspace、相关设计契约、固定 release 的实际进程验收与普通 Spot 业务兼容场景，保存源码、程序和原始证据。不新增 TLA/TLC；顶层 README 的术语及架构图仍留到功能收尾。
