# Binance 协议接口

`tickwire-binance` 把 `tickwire-venue::CanonicalCommand` 编码为签名后的 REST 请求目标或 WebSocket JSON 消息，并解析业务响应。`PreparedRequest` 保存原命令摘要，字段只能读取；TCP/TLS 的发送进度由 `tickwire-transport` 提供。

## 命令与签名字节

```text
CanonicalCommand ──prepare──> PreparedRequest
      │                        │ command_digest / path / payload
      └──持久记录中的摘要─────────┘ 比较
                               ↓
                  HTTP 或 WS 编码 → PreparedSend
                               ↓
                  Connection::try_send(最终门禁)
                               ↓
                    首次 TLS 接受字节 → 等待响应
```

`prepare(command, credentials, options, max_bytes)` 核对 venue、账户、产品和环境。现货对应 `Spot`，USD-M 接受 `LinearPerpetual` 和 `LinearFuture`。凭据登记负责证明 API key 实际属于所声明的账户；编码器能核对声明之间的一致性，无法仅凭密钥字节查询其账户归属。

`RequestOptions` 包含 REST/WS 选择、明确单位的时间戳、以微秒表达的 `recv_window_micros` 和相关 ID。Spot 接受毫秒或微秒时间戳，并保留 `recvWindow` 的三位毫秒小数；USD-M 限定毫秒时间戳与整数毫秒窗口。价格、数量在 WS 中仍是字符串。

| 原语 | 输入与完成边界 |
|---|---|
| `SigningKey` | 加载 HMAC 密钥、RSA/Ed25519 PKCS#8；输出十六进制或 base64 签名。无 `Debug`、复制或密钥导出接口 |
| `Params<N>` | 固定参数容量；拒绝重复键、保留键 `signature` 和非法 JSON 数字；REST 保持输入顺序，WS 签名按键排序 |
| `rest_preimage` | 最终 query 字节直接连接最终 body 字节，中间不补 `&` |
| `fix_logon_preimage` | `A`、SenderCompID、TargetCompID、MsgSeqNum、SendingTime 以 SOH 分隔；独立于 REST/WS |
| `Decimal` | `i128` 系数和十进制位数；解析拒绝指数语法、溢出及不能整除的缩放 |

REST 的非 ASCII 参数先按 UTF-8 百分号编码，再签名。`prepare` 把全部参数放入 query，签名后的 query 原样写入 `path()`，body 为空；base64 签名只百分号编码一次。WS 用排序后的原始字符串值签名，JSON 编码只做 JSON 转义，保持值不变。

| 端点 | HMAC-SHA256 | RSA-SHA256 | Ed25519 |
|---|---|---|---|
| Spot REST / 普通 WS | 支持 | 支持 | 支持 |
| USD-M REST | 支持 | 支持 | 未确认，拒绝 |
| USD-M 普通 WS | 支持 | 未确认，拒绝 | 支持 |
| Spot / USD-M `session.logon` 签名能力 | 拒绝 | 拒绝 | 支持 |
| Spot FIX Logon 签名能力 | 拒绝 | 拒绝 | 支持 |

后两行是签名原语支持情况；会话登录消息与状态机尚未接入。容量失败保持调用者输出缓冲区原值。固定上限为 64 个命令参数、16 KiB 请求、64 KiB 签名原文、1 MiB 响应。命令准备、密钥加载和 JSON 解析允许有界分配；没有整个连接器零分配的承诺。

## 订单与响应

| 已实现命令 | Spot | USD-M |
|---|---|---|
| `New` | LIMIT、LIMIT_MAKER、MARKET、STOP_LOSS/TAKE_PROFIT 及其 LIMIT 形式 | LIMIT、MARKET |
| `Cancel` / `Query` | `/api/v3/order`；WS `order.cancel/status` | `/fapi/v1/order`；WS `order.cancel/status` |
| `CancelReplace` | `/api/v3/order/cancelReplace`，保留失败处理模式 | 拒绝 |
| `AmendKeepPriority` | `/api/v3/order/amend/keepPriority` | 拒绝 |
| `Modify` | 拒绝 | 传 side、price、quantity 和原目标；新 client ID 不受支持时拒绝 |
| `Algo` / `CancelAlgo` | 拒绝 | `/fapi/v1/algoOrder`，条件单与普通单身份分开 |
| `CancelList` | `/api/v3/orderList` | 拒绝 |
| `SpotList` | 尚未实现，返回 `UnsupportedCommand` | 拒绝 |

Spot MARKET 的 `quoteOrderQty` 支持 BUY 和 SELL。Spot 移动止损用整数 `trailingDelta`；USD-M 使用百分比 `callbackRate` 和 `activatePrice`。USD-M GTD 从纳秒到毫秒转换并截去亚秒部分，同时检查到期时间；`closePosition=true` 不携带 quantity/reduceOnly。PostOnly 分别编码为 Spot 的 `LIMIT_MAKER` 和 USD-M 的 `GTX`。

USD-M 的 Cross/Isolated 并非单笔订单参数。发送前，账户协调器须把命令中的保证金模式、持仓模式与已核对的交易所配置比较。编码器保留完整命令摘要，不会通过未文档化参数改变账户配置。订单查询、撤单同时带 orderId/clientOrderId 时，服务器的 ID 优先规则仍适用；协调器据冻结目标核对返回身份。

`decode_rest` 和 `decode_websocket` 返回状态、原始结果、错误码、错误数据及限额数据。`decode_order` 提供精确的订单 ID、十进制数量和累计成交额；ACK 中缺少的状态、成交量保持 `None`。`fills` 保留原 JSON，供后续成交/费用解码。`decode_algo` 独立解析 algo 身份。尚未认识的状态字符串原样保留。

`FailureKind` 只分类当前响应：HTTP 408、一般 5xx、未知错误码和明确的执行超时保持 `Unknown`；USD-M 503 的文档确定失败分支单独识别；参数拒绝使用显式错误码表。`NotFound`、`Partial`、限流、鉴权失败分别保留。解析失败也缺少执行失败的证据。调用者仍需关联请求、订单和已记录成交，再决定是否改变资金占用；业务拒绝不构成传输层 `NoBytes`。

## 后续接入

当前提供可调用的签名与订单协议代码，以及经真实 TLS/HTTP/WSS 的本地协议服务器测试。完整连接器还需接入行情元数据与快照增量同步、私有回报到 `VenueEvent` 的转换、账户查询补账、多维限流、时钟校准、连接代次、登录/心跳/轮换/重连恢复、Spot 列表订单、挂钩价格（peg）、SOR、完整订单响应字段、SBE 和 FIX 业务编解码。支持组合随逐项测试扩展；未支持分支显式返回错误。

协议依据和来源内容摘要见[验证记录](../validation/binance-protocol/README.md)。Spot 固定在官方提交 `f4a9c0be68235f758b46f571f7313c63d1306b25`；USD-M 保存本实现用到的字段、枚举和规则，以及所选内容的哈希。
