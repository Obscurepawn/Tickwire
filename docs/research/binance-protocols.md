# 币安现货与 USDⓈ-M 合约协议调研

本文保留调研时的源码/协议事实与推导背景；运行架构和生产要求以 [v0.3 主设计](../design/binance-ultra-low-latency.md)、[共享内存契约](../design/shared-memory-ipc.md)与[策略生命周期](../design/strategy-lifecycle.md)为准。早期关于单进程或首版取舍的建议不再作为当前部署方案。

调研日期：2026-10-20。首版同时覆盖现货与 USDⓈ-M 加密资产永续合约；本轮只查阅公开协议，没有连接账户、提交订单或测量交易延迟。

首版应保留两个产品适配器。现货可以采用 SBE 行情与 FIX 订单接入，合约采用当前公开的 JSON WebSocket 行情、订单 API 和独立账户流。两者共享定点数、订单状态、风险控制与恢复框架，但必须分别实现簿同步、账户模型、改单和限流规则。协议的推送周期、编码格式和文档中的性能描述，都不能替代部署地点上的实测延迟。

FIX（Financial Information eXchange）是带登录、序号、心跳和业务消息的交易会话协议。SBE（Simple Binary Encoding）按明确的 XML schema 编解码二进制消息。WebSocket 简写为 WS，提供持久连接；行情推送流与请求—响应式 WS API 是不同接口。

## 1. 官方依据与版本边界

现货依据官方仓库 [`binance/binance-spot-api-docs` 的固定提交][spot-commit]：`f0ee79c8ce24dbeda6645211b9504e237fddef19`，提交时间为 `2026-10-15T06:14:38Z`。该提交的生产 CHANGELOG 最后一条日期是 2026-09-18。合约依据官方开发者站的当前产品说明、接口目录及变更日志；主要说明页显示最后修改于 2026-10-19，合约变更日志已包含 2026-10-15 公告及 2026-10-16 生效项。[现货变更日志][spot-changelog]、[合约变更日志][futures-changelog]

当前网站正在使用新的 `products` 与 `catalog` 路径。部分旧的合约单接口链接会跳到产品首页或合并后的接口目录；下文引用已经核验内容的当前页面。网页的最后修改日期与某项协议的生效日期分别记录。

### 1.1 三套 SBE schema 必须分开

| 用途 | 固定文件 | `schemaId:version` | 本次核验结果 |
| --- | --- | --- | --- |
| 现货 WS SBE 行情 | `stream_1_0.xml` | `1:0` | 四类消息：trade、best bid/ask、depth snapshot、depth diff |
| 现货 REST/WS API 响应及账户事件 | `spot_3_5.xml` | `3:5` | 生命周期清单：生产 2026-07-07 发布，Testnet 2026-06-30 发布 |
| 现货 FIX SBE | `spot-fixsbe-1_1.xml` | `1:1` | 生产 2026-03-25 发布；旧 `1:0` 已于 2026-09-25 退役 |

这些 XML 的 `byteOrder` 都为 `littleEndian`，包名分别为 `spot_stream`、`spot_sbe`、`fix_sbe`。相同数字的 schema ID 不代表相同协议。编解码器必须同时绑定协议族、schema ID、版本、模板 ID 和消息长度。[行情 schema][schema-stream]、[API schema][schema-api]、[FIX schema][schema-fix]、[生产生命周期][schema-life]、[Testnet 生命周期][schema-life-test]、[FIX 生命周期][schema-life-fix]

**已发现官方材料不一致：**同一提交的 `spot_prod_latest.xml` 仍指向 `spot_3_4.xml`，但生命周期清单和 2026-07-01 公告已经宣布生产最新版本为 `3:5`。首版应固定显式版本文件，不能把 `latest` 当作版本锁。[latest 指针][schema-latest]、[现货变更日志][spot-changelog]

本次直接下载固定提交中的 XML，解析其头部，并计算了以下 SHA-256。实现阶段可据此锁定代码生成输入；本轮没有把下载内容或生成代码写入仓库。

```text
stream_1_0.xml
6ea328467e144311b1f1efff38e9fe613829997f041dd02a3b7077885d10a1f7
spot_3_5.xml
542776a038883dafe962341a041323ff5d8d3e36f1b860ab60156a88a4080bcf
spot-fixsbe-1_1.xml
6b44afe558d439ddc94323fd6ff026a949184ddd79a2cf033f610194c2537e70
spot-fix-md.xml
1da0b3d618f72c2c9a3634d6fafc92cda5866a7642d9daf228b4391f71c83bfc
spot-fix-oe.xml
f0fa25633758f0554150a81b1aa922d4b9303bed546369ff5a1df2cb4ee08df6
```

SBE 的兼容版本可能自动升至同一 schema ID 下的较高版本。解析器必须使用报文里的 `blockLength`、版本和可变长字段信息，处理新增字段、未知枚举及 `NonRepresentable`，不能仅按本地固定结构体长度强转。schema 废弃后至少支持六个月，退役后请求会失败；需要监控废弃标志与生命周期变更。[SBE FAQ][sbe-faq]

## 2. 产品能力与接入选择

| 能力 | 现货 Spot | USDⓈ-M Futures |
| --- | --- | --- |
| 公共行情 | JSON WS、SBE WS、FIX/FIX SBE Market Data | 当前公开接口为 JSON WS；分 `/public` 与 `/market` 路由 |
| 下单/撤单 | REST、WS API、FIX/FIX SBE Order Entry | REST 与 JSON WS API；普通单和条件单接口分离 |
| 账户/订单回报 | WS API User Data Stream 支持 JSON/SBE；FIX OE ExecutionReport；FIX Drop Copy | 单独的 `/private` User Data Stream，使用 listenKey；含订单、成交、资产、持仓及配置事件 |
| 独立 Drop Copy | 有，文档规定延迟 1 秒 | 本次未找到公开的 Futures FIX Drop Copy 接口 |
| 低延迟编码 | 三套不同的 SBE schema | 本次查阅合约目录、总则及变更日志，未找到可实施的公开 FIX/SBE 端点与 schema；首版按 JSON 设计 |
| 风险状态 | 可用/冻结余额、订单及成交 | 另有持仓方向、杠杆、全仓/逐仓、保证金、标记价格、资金费、强平与自动减仓 |

现货 FIX 页面明确限定为 Spot；合约 WS API 总则明确请求和响应都是 JSON 文本帧。关于合约 FIX/SBE，这里只作“当前公开材料未确认”的能力判断，不把不存在公开文档扩大成不存在任何机构专线服务。[现货 FIX][spot-fix]、[合约 WS API][futures-ws]、[合约目录][futures-public]、[合约变更日志][futures-changelog]

### 2.1 已确认的主网与测试网地址

| 通道 | 生产 | 测试环境 |
| --- | --- | --- |
| Spot REST | `https://api.binance.com/api/v3` | `https://testnet.binance.vision/api/v3` |
| Spot WS API | `wss://ws-api.binance.com/ws-api/v3` | `wss://ws-api.testnet.binance.vision/ws-api/v3` |
| Spot JSON 行情 | `wss://stream.binance.com:9443/ws/<stream>` | `wss://stream.testnet.binance.vision/ws/<stream>`，以 Testnet 流文档为准 |
| Spot SBE 行情 | `wss://stream-sbe.binance.com/ws/<stream>` | `wss://stream-sbe.testnet.binance.vision/ws/<stream>` |
| Spot FIX OE/DC/MD | `fix-oe.binance.com` / `fix-dc.binance.com` / `fix-md.binance.com` | 对应 `fix-*.testnet.binance.vision`；均使用 TLS |
| Futures REST | `https://fapi.binance.com` | 总则当前给出 `https://demo-fapi.binance.com` |
| Futures WS 订单 API | `wss://ws-fapi.binance.com/ws-fapi/v1` | WS API 总则当前给出 `wss://testnet.binancefuture.com/ws-fapi/v1` |
| Futures 行情/账户流 | `wss://fstream.binance.com/public`、`/market`、`/private` | 总则给出 `wss://demo-fstream.binance.com`；各路由在 Demo 的实际支持范围需握手核验 |

Spot FIX 的 `9000` 是文本 FIX 双向，`9001` 是文本 FIX 请求/SBE 响应，`9002` 是 SBE 双向；三种模式均列出 Testnet 地址。现货测试网只支持 `/api`，不支持 `/sapi`，大约每月清空订单等状态，API key 通常保留。合约总则只说“大部分”端点可在测试环境使用；不能由 REST 的 Demo 域名自行推导 WS 订单 API 域名，也不能把测试网流动性用于推断生产延迟。[Spot Testnet][spot-testnet]、[Testnet JSON 行情][spot-testnet-json]、[Testnet SBE 行情][spot-testnet-sbe]、[Futures 总则][futures-general]、[Futures WS API][futures-ws]

### 2.2 认证、权限和产品门槛

- Spot FIX 仅支持 Ed25519：OE 要求 `FIX_API`；MD/DC 要求 `FIX_API` 或 `FIX_API_READ_ONLY`。TLS 握手必须发送目标主机的 SNI 并验证证书。登录签名将 `MsgType、SenderCompID、TargetCompID、MsgSeqNum、SendingTime` 按顺序用 SOH 分隔，Ed25519 签名后以 Base64 发送。[现货 FIX][spot-fix]
- Spot SBE 行情握手需要 Ed25519 类型 API key，放在 `X-MBX-APIKEY`；不需要 timestamp、signature 或额外市场数据权限，IP 白名单仍生效。它不是匿名行情入口。[SBE 行情][spot-sbe]
- Spot REST/WS 支持 Ed25519、RSA 和 HMAC；现货 key 类型文档已把 HMAC 标为 deprecated。Spot 与 Futures 的 WS 会话认证 `session.logon` 都仅支持 Ed25519，认证后可省略后续请求的 key/signature；时间有效性参数仍应按各产品协议发送。逐请求签名与会话认证应是独立能力。[现货 key 类型][spot-key]、[现货 WS API][spot-ws]、[合约 WS API][futures-ws]
- Futures REST 总则列出 HMAC 与 RSA 签名方式，Futures WS 总则明确提供 Ed25519 会话认证。不能仅凭这两页推断所有合约 REST 路径都支持相同的 key/签名组合；实现应对所选组合做接口级契约核验。[Futures 总则][futures-general]、[Futures WS API][futures-ws]
- 权限查询可区分 `enableSpotAndMarginTrading`、`enableFutures`、`enableFixApiTrade`、`enableFixReadOnly` 等标志。产品开通、地区/实体可用性和该账户能否获得 FIX 权限，本轮没有登录核验；公开技术文档没有给出可用于承诺该用户必然获准接入的统一 VIP 等级。接入配置必须记录账户、产品和权限验证结果。[API 权限查询][api-permissions]

时间应分两类：用于签名的 UTC 墙钟，以及用于本地超时/延迟度量的单调时钟。Spot REST/WS `timestamp` 可用毫秒或微秒，`recvWindow` 的单位仍为毫秒，最大 60000，可有三位小数；FIX 非 Logon 请求未设置 `RecvWindow` 时不执行该项检查，因此客户端应显式发送符合策略有效期的窗口。`recvWindow` 约束请求被接收处理的时间，不是订单自动过期时间。[现货 REST][spot-rest]、[现货 FIX][spot-fix]

## 3. 现货三种二进制接口的区别

| 接口 | 请求与控制 | 二进制部分 | JSON 退路 |
| --- | --- | --- | --- |
| SBE WS Market Streams | JSON 文本帧订阅；`serverShutdown` 也是 JSON | 行情事件在 WS binary frame，使用 `stream_1_0.xml` | 另连 JSON 行情端点，重建簿并切换来源 |
| REST API SBE | 普通 HTTP 请求；`Accept: application/sbe` 与 `X-MBX-SBE: 3:5` | HTTP 响应，使用 `spot_3_5.xml` | Accept 同时接受 JSON 时，部分协商失败可回退；按实际 Content-Type 解码 |
| WS API SBE | 请求仍为 JSON 文本；URL 选择 `responseFormat=sbe` 和 schema | 方法响应及支持的账户事件，使用 `spot_3_5.xml` | 同一 URL 同时指定 SBE/JSON 会报错；切换需要新会话 |
| FIX SBE | 9001 文本请求或 9002 二进制请求 | FIX 会话/业务消息，使用 `spot-fixsbe-1_1.xml` | 独立选择 9000 文本会话，不能在原连接中猜测编码 |

WS API SBE 在 HTTP 握手阶段仍可能返回 JSON 错误；成功建立 SBE 会话后，方法响应继续使用 SBE，即使服务端随后禁用 SBE。REST 不能看到 binary 就假定成功，SBE 编码里也有错误响应。[SBE FAQ][sbe-faq]、[SBE 行情][spot-sbe]、[FIX SBE][spot-fix]

Spot SBE 行情与 API 响应时间戳以微秒表示；JSON 默认毫秒，部分 Spot 接口可以显式选择微秒；Futures 总则默认毫秒。标准化事件要带单位或转换后的统一单位，同时保留原始交易所时间，不能把三种来源的整数直接比较。SBE 的价格/数量按独立的 mantissa 和 exponent 字段恢复，不按浮点数读取。[SBE FAQ][sbe-faq]、[合约 WS API][futures-ws]

现货 SBE 行情的 trade 和 best bid/ask 为实时推送，diff depth 当前为 20ms，top-20 snapshot 为 50ms。20ms 是 2026-08-04 起的更新周期，旧资料中的 50ms/25ms 已过时。best bid/ask 含 `eventTime`，并允许服务端在拥堵时丢弃被更新报价替代的旧事件，即 auto-culling；它适合维护最新报价，不能据此重建完整逐事件历史。[SBE 行情][spot-sbe]、[2026-07-17 公告][spot-changelog]

FIX SBE 的 TCP 帧由 **6 字节 SOFH + 20 字节 message header + 消息体**组成；SOFH 两个字段也使用小端序，`encodingType=0xEB50`。要处理半包、粘包和长度上限。FIX SBE depth 当前也是 20ms；文本 FIX depth 为 100ms。FIX SBE 用 `MDEntrySize=NullVal` 表示删除，与 WS depth 的数量零规则不同；book ticker 也有 auto-culling。[FIX SBE 编码与行情][spot-fix]

## 4. 订单簿同步与切换

本地簿应带 `product、symbol、source、connection_epoch、last_update_id、state`。状态至少有 `Syncing、Live、Stale`。只有完成快照衔接、序列检查和时效检查后才能进入 `Live`；发生缺口时立即停止依赖该簿的新开仓判断。

### 4.1 Spot JSON/SBE WS diff depth

以快照 ID 为 `L`、事件首尾更新 ID 为 `U/u`：

1. 先连增量流并缓存，记录最先收到的 `U`；再请求 `/api/v3/depth` 快照，最多每边 5000 档。
2. 若快照 `L` 小于最先缓存事件的 `U`，重新获取快照；初始化缓存中丢弃 `u <= L` 的事件。
3. 依据同页“应用事件”的规则，覆盖下一次更新的事件满足 `u > L` 且 `U <= L + 1`；若 `U > L + 1`，已经缺事件，丢弃本地簿并重新同步。
4. 对 bids/asks 中的价位设置**绝对数量**，数量为零则删除；设置 `L=u`。更旧的事件忽略；相同 ID 的重复处理需保持幂等。

SBE WS 行情文档明确链接到同一套 Spot 本地簿维护规则；字段名需由对应 schema 映射。快照外未发生更新的价位仍未知，不能声称已经拥有全市场完整深度。[Spot diff depth 与同步步骤][spot-streams]

**边界文案差异：**当前 Spot 文档初始化第 5 步写“`lastUpdateId` 位于首个缓存事件的 `[U,u]`”，但随后应用规则允许 `U=L+1`。本设计用“覆盖下一更新 ID”的应用判定，必须用 `U=L`、`U=L+1`、`U=L+2`、`u=L` 四个边界录包用例固定行为；不要把初始化文字翻译成会拒绝正常相邻更新的另一套判断。

### 4.2 USDⓈ-M JSON diff depth

Futures 的初始化边界与 Spot 不同，且每条事件有 `pu`，表示上一事件的最终更新 ID：

1. 先订阅 `/public` 的 `<symbol>@depth`，缓存事件；请求 `/fapi/v1/depth?limit=1000`。
2. 丢弃 `u < snapshot.lastUpdateId` 的事件，注意这里是严格小于。
3. 第一条实际应用的事件须满足 `U <= snapshot.lastUpdateId <= u`。
4. 后续每条事件必须满足 `event.pu == previous_event.u`；不满足就重新获取快照并同步，不能仅用 Spot 的 `U/u` 判定替代。
5. 同价位后更新覆盖前更新，数量是绝对值，零表示删除；删除本地不存在的价位是正常情况。[合约本地簿同步][futures-book]

Futures 常规 diff depth 的公开更新周期为 250ms、500ms、100ms，部分档位的可用性以文档/订阅结果为准。另有包含零售价格改善订单（RPI）的 `rpiDepth@500ms`；普通深度和 RPI 深度不能混入同一本地簿。CM/UM 整合后事件增加 `st`（1=UM、2=CM）和 `ps`（pair symbol）；产品识别不能只依赖连接域名。[合约 Public 行情][futures-public]

### 4.3 FIX 行情与主备切换

Spot FIX Market Data 订阅先返回 `MarketDataSnapshot`，再返回 incremental refresh；`MarketDepth=2..5000` 决定初始快照深度，不限制后续增量。文本 FIX 用 `NEW/CHANGE/DELETE`，FIX SBE 从数量是否为 `NullVal` 恢复动作。FIX 会话序号与簿更新 ID 是两种标识，必须分别维护。[FIX Market Data][spot-fix]

主源断开后，JSON 备用源先独立完成同步，再发布新的 `connection_epoch`。即使两个接口有看似相同的更新 ID，也不在未经验证的情况下把不同连接、编码和更新周期的事件直接交错应用。best bid/ask、partial depth snapshot 与 diff depth 分成不同事件类型；“最新报价可以覆盖”不等于“任意深度增量可以丢弃”。

## 5. 连接生命周期、心跳与限流

| 通道 | 生命周期与心跳 | 客户端发送/连接限制 |
| --- | --- | --- |
| Spot JSON/SBE 行情 WS | 单连接 24 小时；服务端约每 20 秒 ping，1 分钟内收不到对应 pong 则断开；处理 `serverShutdown` | 5 消息/秒，含客户端 PING、PONG、JSON 控制帧；1024 streams/连接；每 IP 每 5 分钟 300 次连接尝试 |
| Spot WS API | 单连接 24 小时；20 秒 ping、1 分钟 pong 期限；处理 shutdown | 每 IP 每 5 分钟 300 次连接尝试；连接消耗 2 REQUEST_WEIGHT；另有各请求/订单限额 |
| Spot FIX OE | 无固定 24 小时上限；尽力保持；协商心跳 5–60 秒，TestRequest 必须回复相同 TestReqID | 10000 消息/10 秒；每账户最多 10 连接；15 次连接尝试/30 秒 |
| Spot FIX DC | 同 FIX 生命周期 | 60 消息/60 秒；每账户最多 10 连接；15 次尝试/30 秒 |
| Spot FIX MD | 同 FIX 生命周期 | 2000 消息/60 秒；每账户最多 100 连接；300 次尝试/300 秒；1000 streams/连接 |
| Futures 行情 WS | 单连接 24 小时；3 分钟 ping、10 分钟 pong 期限 | 10 incoming 消息/秒；1024 streams/连接 |
| Futures WS API | 单连接 24 小时；3 分钟 ping、10 分钟 pong 期限 | 握手消耗 5 weight；ping/pong 最大 5/秒；请求与账户订单计数另算 |

表中是本次文档值，配置加载时仍须读取交易所当前限额。FIX/FIX SBE 共用连接限额；FIX message limit 不是账户订单额度。[Spot WS 行情][spot-streams]、[Spot SBE 行情][spot-sbe]、[Spot WS API][spot-ws]、[Spot FIX][spot-fix]、[Futures 行情连接][futures-connect]、[Futures WS API][futures-ws]

Spot 的 `serverShutdown` 已取消“提前十分钟”的固定承诺，收到后应尽快迁移。FIX 维护时会发 `News<B>`，通常每十秒重复到断开；异常断线未先 Logout，原 `SenderCompID` 可能在两倍心跳周期内不能重用。重连需退避、预留连接额度和独立的新会话 ID；不能全部策略实例同时重连。[2026-06-09 公告][spot-changelog]、[FIX 生命周期][spot-fix]

Futures 必须按数据种类拆路由：depth/bookTicker 走 `/public`，aggTrade/markPrice/forceOrder 走 `/market`，账户事件走 `/private`。迁移公告给出 2026-04-23 截止，并说明旧根路径可能只保留 Public 数据；不能把“TCP/WS 建连成功”当作订阅完整性的证明。每类流需要独立的首次事件/活跃状态监控。[合约路由迁移公告][futures-routing]

### 5.1 限流器需要哪些维度

- Spot 分开跟踪 IP 请求权重、原始请求数、账户 `ORDERS`/未成交订单计数、FIX 每连接消息数和订阅控制数。账户计数共享于同账户 API keys，成交后的递减行为按官方规则处理；不能把 `ORDERS` 计数直接等同于当前 open orders 数量。[Spot WS 限流][spot-ws]、[未成交订单计数规则][spot-order-count]
- Spot 2026-04-02 起部分成功交易请求的 REQUEST_WEIGHT 可为零，但失败请求、订单计数和其他限额仍存在；amend keep priority 并非普通成功就免费，2026-04-02 澄清只对改单导致订单过期的情形为零。运行时按端点规则和实际返回计数更新，不抄旧的示例数字。[Spot 限流变更][spot-changelog]
- Futures 当前 WS IP 权重池与普通 REST IP 权重池分开；REST 单笔/批量的 place、modify、cancel 是明确的重叠项。`ORDERS` 按 UID 计，REST/WS 共享。CM/UM 还共享相应 IP/订单预算；需要账户级统一分配，而不是每连接各自计数。[Futures WS 限流][futures-ws]、[CM/UM 整合公告][cm-um]
- `429` 按响应的限流信息退避；继续轰炸可能导致 `418` IP 封禁。查询、撤单、关闭敞口与恢复流程要预留额度。Futures `-1008` 对符合条件的减仓/平仓请求有例外，不能据此认为所有限流、签名错误或网络故障都会放行撤单。[Futures 错误与限流][futures-general]

## 6. 订单歧义、重试、取消与改单

### 6.1 请求确认与订单状态分离

每个业务意图使用持久化且不复用的本地 `intent_id`，另外维护传输请求 ID、client order ID、交易所 order ID。WS `id` 只是请求/响应关联标识，不是交易所幂等键；client order ID 的唯一性也通常只约束未结束订单。Spot 已成交订单后可再次接受相同 client ID，Futures 文档写的是“open orders 中唯一”，因此重发同 ID 仍可能产生第二笔订单。[Spot 新单][spot-rest]、[Futures 普通订单][futures-ws-trade]、[Futures WS 请求格式][futures-ws]

发送前记录业务意图，收到 ACK 后标记请求已接受，再根据执行回报更新订单/成交。收到成交可能早于另一连接上的 ACK；晚到 ACK 不能把 `FILLED` 回退到 `NEW`。订单状态字段和执行事件字段分别保存：例如 Spot `x` 是执行类型、`X` 是订单状态；撤单确认也不能抹去已发生的部分成交。[Spot User Data][spot-user]、[Futures User Data][futures-user]

| 情况 | 协议含义 | 本地动作 |
| --- | --- | --- |
| 在应用层可以证明请求未发出 | 无交易所提交事实 | 可按原意图重新排队 |
| 写入/断线期间结果不明；Spot `-1007` 或 5XX | 执行状态未知，原请求可能成功 | 保留风险占用，查回报和订单；禁止盲目切通道重发 |
| Futures 503 且文本为 unknown error | 可能执行成功 | 先用用户流或 order query 确认 |
| Futures 503 `Service Unavailable`，或明确的 `-1008` | 文档明确属于失败 | 在额度和业务有效期允许时退避重试 |
| cancel 超时或返回订单不存在 | 取消结果不能单靠此响应确定 | 查询原订单及成交，保持 `CancelPending/Unknown` 直到收敛 |
| query 暂时找不到订单 | 读侧可能滞后，且历史有保留期 | 组合 client ID、order ID、open orders、成交记录和本地日志继续对账 |

Spot API 等待撮合后端超过十秒可能返回 `-1007`。Futures 503 必须按具体错误消息分类，不能把全部 503 统一重发。查询源可能是缓存/数据库，HTTP 成功与完整可见性也不是同一件事。Futures 已取消/过期且无成交的订单超过三天可能查不到，所有订单超过九十天也可能查不到，因此长期恢复不能只靠远端历史。[Spot 通用规则][spot-rest]、[Futures 503 语义][futures-general]、[Futures order.status][futures-ws-trade]

### 6.2 三类“改单”必须有不同命令

| 命令 | 行为与边界 |
| --- | --- |
| Spot cancel-replace | 先 cancel 后 new；两部分可能分别成功/失败，HTTP 409 表示部分成功。`STOP_ON_FAILURE` 与 `ALLOW_FAILURE` 不同，后一种可能在撤单失败时仍下新单；`NOT_ATTEMPTED` 也可能增加未成交订单计数。保存两个结果与两个订单身份 |
| Spot amend keep priority | 只减量，保留 order ID 和时间优先级；symbol 需 `amendAllowed=true`。成功推送执行类型 `REPLACED`；client ID 可变，拒绝时原单不变。FIX 命令为 `<XAK>` |
| Futures order.modify | 当前只改 LIMIT，改单后重新排队；同时提交 price 和 quantity。部分成交时新 quantity 小于等于已成交数量，或 GTX 改价后将立即成交，会取消原单。过滤器不通过通常拒绝改单并保留原单 |

Futures 2026-09-15 新增 modify 的 `reduceOnly` 参数只用于校验与最低名义金额例外，不会把原先非 reduce-only 的订单改成 reduce-only；属性不一致返回 `-5047`。修改序号 `modifyId` 是用户定义回显值，文档明确不检查唯一性，不能当幂等保证。[Spot cancel-replace][spot-rest]、[Spot 保优先级改单][spot-amend]、[Futures modify][futures-ws-trade]、[合约变更日志][futures-changelog]

FIX `MessageHandling=UNORDERED` 允许请求在送撮合时重排；即便 TCP 有序、MsgSeqNum 连续，也不能据此假设“先撤单再下单”在撮合侧具有同样顺序。有关联的操作应使用适合的协议命令、`SEQUENTIAL` 或等待已确认状态。`ResponseMode=ONLY_ACKS` 会关闭执行回报推送；若使用它，必须同时具备完整且健康的账户回报来源。Drop Copy 延迟一秒，适合作为独立核对来源，不能成为唯一热路径风险回报。[FIX 顺序与 ResponseMode][spot-fix]

默认 `ResponseMode=EVERYTHING` 会向并行 OE 会话推送该账户成功的 ExecutionReport/ListStatus，包括其他 FIX 会话及非 FIX API 创建的订单。会话不是策略所有权边界；归属判断与重复回报合并必须使用账户和订单身份。[FIX 回报范围][spot-fix]

FIX Logon 要求 `ResetSeqNumFlag=Y`；公开支持消息中没有提供可用于本系统假定的完整历史回放契约。断线后建立新会话并用订单/成交查询对账；不能因使用 FIX 就把通用 FIX 引擎的 resend/replay 语义视为币安已承诺的能力。

## 7. 合约账户与风险状态是首版核心

### 7.1 持仓和保证金不能套用现货余额

合约持仓键至少包含账户、产品、symbol 和 `positionSide`。单向模式为 `BOTH`；双向模式必须发送 `LONG/SHORT`，且不能发送普通订单的 `reduceOnly` 参数。双向减仓必须由 side 与 positionSide 组合表达并由风控校验，不能把“反向订单”总是解释成开另一方向仓位。[Futures New Order][futures-ws-trade]

控制面需读取并保存：持仓模式、逐仓/全仓、Multi-Assets 模式、杠杆、名义金额与维持保证金档位、可用保证金、持仓和挂单占用。`/fapi/v3/positionRisk` 只返回有持仓或挂单的 symbol；空集合的语义必须结合全量快照范围处理。`pricePrecision/quantityPrecision` 不是 tickSize/stepSize，下单对齐从 filters 读取；内部价格、数量和资金使用定点数。[合约账户][futures-account]、[positionRisk 与交易接口][futures-rest-trade]、[exchangeInfo][futures-market]

杠杆上限按 symbol 的 `/fapi/v1/leverageBracket` 读取。2026-10-09 公告已纠正文档原先固定 125 倍的上限说法，不能将其硬编码。这里的接入要求是正确识别已有账户状态；系统启动不应自动改杠杆、持仓模式或保证金模式。若首版只实现某些模式，遇到其他模式应在启动检查中明确拒绝激活交易。[合约 leverageBracket][futures-account]、[2026-10-09 公告][futures-changelog]

### 7.2 账户流与对账

Futures listenKey 创建后有效六十分钟，PUT 可延长六十分钟；对已有有效 key 的 POST 返回同一个 key 并续期；`-1125` 要重新 POST。连接寿命二十四小时是另一项期限。`listenKeyExpired` 与 WS 断连不是同一事件，收到后即使 socket 仍开着也不再收到有效账户更新。续期、心跳和连接轮换必须是三个独立定时器。[Futures User Data][futures-user]

关键事件的处理要求：

- `ORDER_TRADE_UPDATE` 驱动完整订单生命周期；`TRADE_LITE` 只推 TRADE 且字段更少，可选作更快成交提示，仍要与完整事件按成交身份去重，不能替代撤单、过期及账户更新。
- `ACCOUNT_UPDATE` 是稀疏更新，只包含变化资产/持仓；未出现的 symbol 不代表仓位归零。未成交新单或撤单不一定触发账户事件，挂单保证金还需结合订单状态与快照处理。
- `ACCOUNT_CONFIG_UPDATE` 反映杠杆/Multi-Assets 配置变化；配置变化后重新检查风险模型前提。
- 官方保证同一用户、同一连接、同一事件类型按 `T/E` 排序。跨连接、跨事件类型不能由时间戳推导完整因果总序；同时保存交易所时间、连接内接收序号和本地单调接收时间。[Futures User Data][futures-user]

账户恢复应先订阅并缓存私有事件，再取账户、持仓、open orders、open algo orders 和近期成交快照进行合并核对。与行情 `U/u/pu` 不同，这些接口没有提供一套覆盖全部账户状态的同款快照序号衔接协议；需要保留疑义并反复查询收敛，不能把一次 REST 快照称作整个账户的原子截面。订单/成交去重键要带账户与产品，不能只用时间戳。

### 7.3 资金费、强平与条件单

`markPrice` 流提供标记价格、指数价格、资金费率和下次资金费时间，更新周期是 3 秒或 1 秒。资金费周期会调整，使用 `nextFundingTime` 和 `/fapi/v1/fundingInfo`，不能把八小时写成所有 symbol 的常量。跨仓资金费 `ACCOUNT_UPDATE` 可能只有余额没有持仓字段，逐仓则包含相关持仓；账本不能依赖每次资金费都伴随完整持仓列表。[合约 Market 行情][futures-market-stream]、[资金费接口][futures-market]、[账户事件][futures-user]

`MARGIN_CALL` 到达时可能已经发生强平，因此风险限额要在本地根据标记价格、保证金和档位主动计算。`ORDER_TRADE_UPDATE` 可出现强平执行、`autoclose-*` 与 `adl_autoclose` 订单。公共 `forceOrder` 每个 symbol 每 1000ms 只推最新一笔强平快照，不能当作全部强平成交记录，也不能作为本账户强平状态的权威来源。[账户风险事件][futures-user]、[公共强平流][futures-market-stream]

当前普通 `order.place` 只列 LIMIT/MARKET；STOP、TAKE_PROFIT、STOP_MARKET、TAKE_PROFIT_MARKET、TRAILING_STOP_MARKET 使用 `algoOrder.place`，持有 `algoId/clientAlgoId`。`ALGO_UPDATE` 有 `NEW、TRIGGERING、TRIGGERED、FINISHED、REJECTED、EXPIRED、CANCELED`；`TRIGGERED` 只表示已经进入撮合，`FINISHED` 也可能是取消，均不能直接视为全部成交。条件单与触发后的普通订单需要显式关联。[合约 WS 交易][futures-ws-trade]、[Algo Order Update][futures-user]

条件平仓的 `closePosition=true` 只适用于 STOP_MARKET/TAKE_PROFIT_MARKET，不能同时发送 quantity 或 reduceOnly；触发价格可以选 MARK_PRICE 或 CONTRACT_PRICE。`priceProtect` 还会施加两类价格之间的差异阈值。若首版策略只产生普通订单，恢复和风险控制仍须识别账户里已有的条件单，或拒绝在有不受管理条件单的账户上激活交易。[条件单参数][futures-ws-trade]

Futures 提供 `/fapi/v1/countdownCancelAll`，需要按 symbol 周期续约，停止续约后取消挂单。它可以补充本地故障保护，但本身有请求权重和时间粒度，不能替代主动撤单；当前页面没有明确覆盖新 Algo 条件单的保证，不能假设它同时清掉所有条件单。[自动撤单接口][futures-rest-trade]

## 8. COIN-M、交割和其他扩展边界

首版合约交易对象应明确筛选 USDⓈ-M、加密资产、`contractType=PERPETUAL`，并核对 settlement/margin asset。交割合约需要到期/结算日和换月行为；COIN-M 需要合约张数、面值、币本位保证金和收益计算；Portfolio Margin 使用不同账户接口与风险模型。不能仅因字段相近就由同一个“期货订单”枚举默认接纳。[合约 exchangeInfo][futures-market]、[CM/UM 整合公告][cm-um]

但即使首版不交易 COIN-M，也有必须识别的账户耦合：官方公告称 CM/UM 架构整合自 2026-06-24 分阶段上线、2026-06-30 完成后，两者共享 `dualSidePosition`、订单限额和相应 IP 权重池；任一侧有持仓/挂单都会阻止切换模式。合并行情还可能同时返回 UM 与 CM，需用 `st` 和 symbol 元数据筛选。协议隔离与账户隔离不能混为一谈。[CM/UM 整合公告][cm-um]

## 9. 实现前需解决的具体核验项

1. **schema 矛盾：**固定显式的 `3:5` XML 与 hash，验证实际握手返回版本；不要直接读取过期的 production `latest` 指针。
2. **Spot 簿边界文字：**把本设计的下一更新 ID 判定写进验收用例，并核验真实流在 `U=L+1` 边界的行为。
3. **Futures decimal 类型：**WS 总则要求 DECIMAL 用 JSON 字符串，当前自动生成接口表把部分 price/quantity 标为 `number/float`。编码器按精确十进制字符串设计，接入验证发送格式；不能由生成类型直接引入 `f64`。
4. **Futures 私有路由：**User Data 页给 `/private/ws/<listenKey>`，迁移页另给 `listenKey + events` 查询参数形式；实施时核验选用形式，检查所需事件确实开始推送。
5. **测试环境差异：**分别验证 Spot JSON/SBE/FIX、Futures Demo 行情/账户、Futures WS 订单 API 的地址、权限与功能，不自动套换域名。
6. **生产能力：**确认实际账户/地区/实体的 Spot、Futures、FIX 权限，以及策略所需 symbol、保证金模式、档位与额度。本轮没有相关账户证据。
7. **恢复契约：**在测试环境或录包回放中覆盖 ACK 丢失、先成交后 ACK、撤单/成交竞争、cancel-replace 部分成功、改单导致撤单、listenKey 过期、换连接重复事件、订单簿缺口和状态查询滞后。未知状态须保持风险占用，不自动释放或重复开仓。

这些核验项均可落到协议适配器和验收用例，当前设计阶段无需访问账户。接入后再分别测量行情到达、解码、策略决策、风控、发送、ACK 和成交回报延迟；文档中没有可用于承诺本系统端到端延迟的数字。

[spot-commit]: https://github.com/binance/binance-spot-api-docs/commit/f0ee79c8ce24dbeda6645211b9504e237fddef19
[spot-changelog]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/CHANGELOG.md
[spot-fix]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/fix-api.md
[spot-sbe]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/sbe-market-data-streams.md
[spot-streams]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/web-socket-streams.md#how-to-manage-a-local-order-book-correctly
[spot-ws]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/web-socket-api.md
[spot-rest]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/rest-api.md
[spot-user]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/user-data-stream.md
[spot-key]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/faqs/api_key_types.md
[spot-amend]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/faqs/order_amend_keep_priority.md
[spot-order-count]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/faqs/order_count_decrement.md
[spot-testnet]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/testnet/general-info.md
[spot-testnet-json]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/testnet/web-socket-streams.md
[spot-testnet-sbe]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/testnet/sbe-market-data-streams.md
[sbe-faq]: https://developers.binance.com/en/docs/products/spot/faqs/sbe_faq
[schema-stream]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/sbe/schemas/stream_1_0.xml
[schema-api]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/sbe/schemas/spot_3_5.xml
[schema-fix]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/sbe/schemas/spot-fixsbe-1_1.xml
[schema-life]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/sbe/schemas/sbe_schema_lifecycle_prod.json
[schema-life-test]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/sbe/schemas/sbe_schema_lifecycle_testnet.json
[schema-life-fix]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/sbe/schemas/sbe_fix_schema_lifecycle_prod.json
[schema-latest]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/sbe/schemas/spot_prod_latest.xml
[api-permissions]: https://developers.binance.com/en/docs/catalog/core-trading-wallet/api/rest-api/account
[futures-general]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info
[futures-ws]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-api-general-info
[futures-connect]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect
[futures-routing]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice
[futures-book]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
[futures-public]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public
[futures-market-stream]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market
[futures-ws-trade]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/trade
[futures-rest-trade]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade
[futures-market]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data
[futures-account]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account
[futures-user]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams
[futures-changelog]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log
[cm-um]: https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/Important-CM-UM-Integration-Notice
