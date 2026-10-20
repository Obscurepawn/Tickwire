# 完整交易命令与交易所事件

`tickwire-venue` 将完整命令封装为不可变的 `CanonicalCommand`，为资金预留、WAL、恢复和交易所适配器提供同一份内容。`new(Command)` 检查结构和参数组合，`decode(bytes)` 只接受相同 schema 的规范编码；成功后只提供 `command()`、`bytes()`、`digest()` 只读访问。`digest = SHA256(bytes)`，调用者按引用传递封闭对象，摘要和内容不能分别改写。

## 类型与归属

`CommandContext.request` 保存风险域、交易所、环境、账户、产品、意图 ID、原执行代次、策略代次、配置版本、截止时间和恢复对象 ID。`strategy` 保存 IPC 的策略身份、授权代次、策略意图序号、创建时间、经济操作 ID、风险和订单配置版本。`strategy_intent_seq` 是策略发布序号；执行器在资金预留后分配的 WAL 序号不进入此对象。

`instrument` 固定标的、标的版本、目录版本及摘要、原始 symbol、基础/报价/结算资产及版本和指数、价格步长、数量步长、合约种类与到期信息。策略提交的 `recovery_manifest_hash` 只能指已存在的依赖对象；包含当前命令的闭包根在后续提交证明中绑定，避免形成哈希环。

`CommandBody` 包含 New、Cancel、CancelReplace、AmendKeepPriority、Modify、Query、SpotList、CancelList、Algo 和 CancelAlgo。新单保存客户端 ID、方向、类型、数量方式、价格、有效期方式、持仓方向和模式、reduce-only、自成交防护、触发价、价格保护、移动止损和冰山数量。撤改单保存原订单和管理权版本、原执行代次及交易所/客户端身份。列表最多三腿，OCO 的顺序为 above/below，OTO 为 working/pending，OTOCO 为 working/pendingAbove/pendingBelow；Algo 由独立父身份和客户端命令身份表达。未知产品结构需新增有版本的类型及适配器支持，不能放入自由参数 JSON。

`CanonicalEvent` 保存来源连接代次、原始事件/订单/成交身份、原意图代次、父子关系、累计数量、未知数量的 `None`、成交价量、原始累计/本笔 quote、独立已实现盈亏，以及最多八种资产的有符号费用现金流。字段缺失时保留 None。手续费扣款为负，返佣为正。连接代次属于事实来源，不能替代原订单代次。事件规范化不会直接改变资金或证明成交真实性。

## 编码与边界

命令编码为 CBOR 数组 `["tickwire.venue-command", 1, context, body]`，事件使用 `"tickwire.venue-event"`。内部结构按 Rust 类型声明的字段顺序编码为固定长度数组；枚举使用固定数字标签。整数和长度采用最短形式，i128 超出普通 CBOR 整数时使用 tag 2/3，禁止浮点、不定长项、尾随内容、未知标签或额外字段。依据 [RFC 8949 §4.2.1](https://www.rfc-editor.org/rfc/rfc8949.html#section-4.2.1) 与 [§3.4.3](https://www.rfc-editor.org/rfc/rfc8949.html#section-3.4.3)。

数值使用 `Decimal(coefficient, exponent)`，值为 coefficient × 10^exponent，指数限定 -38…38；构造时去除可去除的十进制尾零，零统一为 `(0, 0)`。解析器拒绝非规范的等值表示。订单、成交和客户端身份使用有界可见 ASCII；标的与资产符号使用有界 UTF-8，拒绝空白和控制字符，保留原始大小写及 Unicode 字节。错误消息另用有界 UTF-8 保留原文。所有字符串绝不截断。总编码上限 16 KiB；输入先检查总长，字符串/数组先检查各自边界再分配，schema 嵌套深度固定。

本模块检查可表达性、金额符号、产品与参数组合、目录步长整除、列表身份和边界。交易所特有字符集、价格带、可用产品功能、授权、行情时效和实际账户配置由调用方核对。`AmendKeepPriority` 的“确实减量”依赖当前订单量，调用方在持有当前订单状态时核验。
