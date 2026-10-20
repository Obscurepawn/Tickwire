# Binance Spot 请求扩展规格

状态：已实现。接口见[列表、挂钩价格和 SOR 请求](../../docs/implementation/binance-request-extensions-interface.md)，证据见[验证记录](../../docs/validation/binance-request-extensions/README.md)。

## 历史编码和新增类型

schema 1 原命令标签 1–10、`OrderSpec` 和 `ListOrder` 的字段顺序、数组长度、字节及摘要不变。新增 `SpotAdvanced` 标签 11；其子标签 `New=1`、`CancelReplace=2`、`List=3`、`SorNew=4`。子数组分别为 `[1,SpotOrder]`、`[2,OrderTarget,SpotOrder,CancelReplaceMode]`、`[3,SpotAdvancedList]`、`[4,OrderSpec]`。新增标签仍纳入整个 canonical command 的 SHA-256。

`SpotOrder=[OrderSpec,Option<Peg>]`；`Peg=[PegPriceType,Option<u16>]`，价格类型值为 `Primary=1`、`Market=2`；`SpotAdvancedList=[list_id,client_id,ListKind,BoundedList<SpotOrder,3>]`。所有可选字段、整数和数组沿用现有严格 CBOR 编码。未知标签、错误数组长度、非最短整数、截断及多余字节拒绝。

旧解码器不认识标签 11，返回 `Enum`；四个新子型已交给固定旧提交的真实解码器验证。调用方不能把新增类型发送给尚未声明该能力的消费者。旧命令兼容性样本取自扩展前的实现，覆盖原全部 10 个标签。

## 组合校验

新命令仅允许 Spot，包含触发依据和恢复对象绑定。New、CancelReplace 必须包含 peg；List 必须至少有一条 peg 腿，列表标识非零、腿数符合类型，客户标识不得与列表或其他腿重复。这样普通订单仍只有原有表示。

Peg 允许省略原限价，保留显式限价、止损价和追踪距离的各自含义。偏移为 0–100 档，出现时同时编码偏移类型和数值。挂钩 MARKET 订单、PRIMARY 普通 LIMIT 非 GTC、MARKET peg 的 maker 订单拒绝。SOR 仅接受 LIMIT/MARKET、base quantity 和普通 Spot mode；quote quantity、maker 及条件订单拒绝。

旧普通订单校验新增一个允许组合：Spot LIMIT + PostOnly 可携带冰山数量；其编码保持不变。其他产品的冰山 TIF 约束没有放宽。

列表请求只在能够完整表达全部腿时构造：OCO 配对的方向/数量一致，OTOCO 两条 pending 腿同样一致；全部腿共享 STP 模式。working 腿、OCO 配对类型、below 不允许 maker 等条件在 Binance 编码器检查。合法腿字段先按普通 Spot 规则编码，再通过固定字段表转换为官方前缀；未列入映射的字段直接拒绝。OCO maker 冰山按照其参数表同时输出 GTC。

## 请求与完成边界

REST 签名使用实际百分号编码后的 query；WSS 签名使用排序的原始参数值，请求 JSON 保存相同参数。原账户、产品、环境、密钥算法、长度和摘要校验继续执行。输出最大 16384 字节，参数最多 64 项；恰好容量成功，少一字节失败。

本阶段输出请求，不维护 OTO 待触发腿和 OCO 互撤状态。符号是否支持 peg/SOR、实时价格过滤、列表腿占用的完整资金预算由调用方提供；当前账户实现对 `SpotAdvanced` 返回 Unsupported，协议支持不会自动赋予交易资格。

本阶段覆盖原设计的 OCO、OTO、OTOCO；官方后来增加的 OPO/OPOCO 保留为后续能力。

这是无持久状态的编解码扩展，新增独立 TLA 状态机不能证明字段映射正确。字段映射由固定参数期望、严格解码、兼容性样本及真实 TLS 协议服务器验证；发送端继续适用[JournaledSender L0](../../tla/execution_models/gateway/L0_journaled_sender/00_input.md)的持久化与首次输出边界，该模型不证明签名原文或交易所的列表处理行为。
