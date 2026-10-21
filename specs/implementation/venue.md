# 交易命令和事件规范化

实现目录：`crates/tickwire-venue`。接口见 venue-interface.md。本模块生成完整、不可变的命令字节和 SHA-256 内容摘要，由执行器和闭包存储绑定；它不执行网络发送或资金记账。

## 内容和身份

- 请求身份包含风险域、venue、生产/测试环境、账户、产品、意图 ID、原执行代次、策略代次、配置版本、deadline 和 recovery_object。
- 策略上下文保留 logical_strategy_id、instance_id、strategy_intent_seq、authorization_epoch、risk_policy_version、order_config_version、created_mono_ns、economic_intent_id、hedge_group_id、leg_id 和预先存在的 recovery_manifest_hash。执行器的 WAL 序号独立分配。
- 标的绑定包含 instrument/catalog 的 ID、版本、摘要、原始 symbol、基础/报价/结算资产，以及步长、合约类型、交割或期权到期时间。UTF-8 symbol 按原字节保留，禁止空白和控制字符，不作大小写或 Unicode 归一化。
- 新单完整保存 client_id、side、order_type、数量方式、price、TIF/GTD、position_side、产品/持仓/保证金模式、reduce_only、STP、触发参数、移动止损和冰山数量。现货移动止损使用基点，衍生品使用百分比，并保存工作价格源。
- Cancel、Query、Amend、Modify、CancelReplace 保存原订单身份、管理权版本和原执行代次；改量、改价、新客户端 ID 和取消替换模式独立编码。List/Algo 使用独立父身份。列表最多三腿，顺序为 OCO `[above, below]`、OTO `[working, pending]`、OTOCO `[working, pendingAbove, pendingBelow]`。

`Command` 是可编辑草稿；`CanonicalCommand` 私有持有草稿副本、编码和摘要，仅允许只读访问。两个类型都不实现 `Copy`。构造器和解码器执行相同的语义检查，摘要始终由内部完整编码派生。


`VenueEvent` 保留风险域、venue/environment/account/product、标的版本、来源连接代次、可选来源序号、原始事件 ID 和时间。订单事件保留原意图和原执行代次、内部订单/父单/list ID、客户端/交易所订单/list/algo ID。无法关联内部订单时，内部 ID 和意图字段全零，原始交易所身份仍需存在。

成交保存原量、累计量、已知剩余量、订单价、本笔量价、maker/taker 状态和原始 trade ID。交易所报告的累计 quote、本笔 quote 和已实现盈亏分别保留，缺失值使用 None，不从价格乘数量补造。已实现盈亏使用独立 AssetDelta，不归入手续费。未知量用 `None` 表示。费用最多八种资产，每项都携带资产 ID、版本、原始符号、最小单位指数和有符号现金流；同事件同资产重复行被拒绝，由适配器明确选择独立费用事件或经核对的净额。费用没有强制转成单一结算资产。连接代次变化会改变事件来源字节；经济去重按原始交易所事实身份执行，重连保留原归属和入账键。

## 编码合同

顶层是固定数组：

```text
command = ["tickwire.venue-command", 1, CommandContext, CommandBody]
event   = ["tickwire.venue-event",   1, EventIdentity, VenueEventBody]
digest  = SHA256(完整顶层编码)
```

结构体使用 [codec.rs](../../crates/tickwire-venue/src/codec.rs) 中 `structure!` 列出的字段顺序，长度固定；Option 为 `[]` 或 `[value]`，有界列表为定长 CBOR 数组。简单枚举的标签在 `enumeration!` 显式固定，复杂枚举为 `[tag, fields...]`；未知标签、缺失/多余字段和尾随内容均拒绝。schema 1 的字段或标签改变须提高版本，缺失交易参数直接拒绝。

CBOR 整数、长度和 tag 2/3 bignum 使用最短形式；仅允许 i128 范围，负整数使用 `-1-magnitude`，覆盖 `i128::MIN`。Decimal 为 `[coefficient, exponent]`，指数 -38…38，去除可去除的尾零，零只能编码为 `[0, 0]`。不接受浮点、不定长项或非规范的等值数值。

总输入先限制为 16 KiB。每个字符串、列表和固定数组先检查长度再分配；没有来自输入的递归 schema 或自由参数对象。独立尺寸计算给出的保守结构上界见 encoding-bound.log，其中连互斥的可选项也累计计入。

## 校验和扩展

本模块核对字段非零身份、创建时间早于截止、触发引用边界和重复、产品/合约种类、价格/数量步长整除、类型与价格/TIF/触发参数组合、ClosePosition、普通 Spot Market 买卖两侧的 QuoteAmount、列表腿数和客户端身份。步长取模使用无符号余数计算，消除指数对齐产生的乘法溢出。

交易所适配器还需验证该产品实际支持的订单类型、上下价关系、价格带、费率、最小名义价值、账户已应用的持仓/保证金模式、管理权，以及减量请求是否小于当前订单量。目录对象与命令内标的字段的一致性属于受信目录验证。传输层签名时间和请求 ID不改写这里的订单客户端身份或业务期限。

当前类型不包含任意厂商扩展。OPO/OPOCO、价格撮合档位、厂商附加策略标签或新的账户动作，通过新增明确字段/枚举和版本后的编码、检查与适配器扩展。适配器遇到尚未表达或支持的结构返回 Unsupported，在该类型开放前保留请求失败状态。


真实失败测试、当前命令、独立测试向量和源文件摘要见 验证目录。命令摘要的预留、闭包、WAL 和恢复绑定由执行器的 [L1_command_binding](../../tla/execution_models/business_journal/L1_command_binding/00_input.md) 模型检查；CBOR 字节解析由本模块单元与集成测试检查，不将摘要符号模型当作解析器证明。
