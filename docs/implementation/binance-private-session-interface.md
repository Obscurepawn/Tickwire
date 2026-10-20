# Binance 私有流会话接口

状态：生产接口已实现；行为与实际网络测试见 [验证记录](../validation/binance-private-session/README.md)。

`tickwire-binance-private`，负责真实 TLS/HTTP/WebSocket 连接上的认证、订阅、listenKey 续期、心跳、断开和重新建立连接。它复用 `tickwire-binance::auth/params/events` 与 `tickwire-transport`，不更改订单请求、行情解码或 `depth_sync` 的状态。一个实例只服务一个产品、环境和账户，并独占自己的私有流连接。

私有流中断后，上层立即限制增险，继续保存已收到的账户事实；重新收到订阅确认只恢复接收能力。账户恢复屏障仍由对账与持久化协调器负责。这对应[主设计的会话、认证和恢复约束](../design/binance-ultra-low-latency.md)。

## 1. 官方协议与实现范围

Spot 使用官方仓库固定提交 [`f4a9c0be68235f758b46f571f7313c63d1306b25`](https://github.com/binance/binance-spot-api-docs/tree/f4a9c0be68235f758b46f571f7313c63d1306b25)。USD-M 使用下表所列官方接口页；请求和本地协议服务端测试固定了下列参数与回复形状。

| 产品与操作 | 实际协议 | 本阶段编码与处理 |
| --- | --- | --- |
| Spot 登录 | `session.logon`，仅 Ed25519；提交 `apiKey/timestamp/signature`，可带 `recvWindow` | 校验成功响应的请求 ID 和 API key 后，才允许登录态订阅 |
| Spot 登录态订阅 | `userDataStream.subscribe` | 绑定本连接已确认的账户和返回的 `subscriptionId` |
| Spot 独立签名订阅 | `userDataStream.subscribe.signature`，支持 HMAC、RSA、Ed25519 | 提交完整签名参数；无需先登录 |
| Spot 状态与关闭 | `session.status`、`session.subscriptions`、指定 ID 的 `userDataStream.unsubscribe`、`session.logout` | 实际编码、关联响应；状态查询不能替代缺失的订阅成功响应 |
| USD-M 创建、续期、关闭 | `POST/PUT/DELETE /fapi/v1/listenKey`，请求头 `X-MBX-APIKEY` | 当前参数表没有 query/body 参数，也不要求订单式签名；POST/PUT 读取返回的 `listenKey` |
| USD-M 私有事件连接 | `wss://fstream.binance.com/private/ws/<listenKey>` | 使用单个 key、未筛选事件的连接，复用现有裸事件解码 |

登录及订阅参数依据 [Spot 认证接口](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-api/auth)、[订阅接口](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-api/user-data-stream) 和[固定版本 WebSocket API](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-api.md#session-authentication)。固定版本的 `userDataStream.subscribe` 表包含 `apiKey`，网页目录的示例将它省略；编码器显式提交与已登录身份一致的 key。签名订阅始终包含 `apiKey` 和 `signature`，不照抄网页目录中省略这些字段的简化示例。

Spot 旧 `POST/PUT/DELETE /api/v3/userDataStream` 和 `userDataStream.start/ping/stop` 已在 2026-02-20 下线，本实现不提供这条旧流程。[官方变更记录](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/CHANGELOG.md#2026-01-21)

USD-M REST 操作以[官方 listenKey 参数表](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/user-data-streams)为准。另有 WebSocket API 的 `userDataStream.start/ping/stop`，本阶段采用完整的 REST 管理加 WSS 接收流程，不同时实现第二套管理通道。新版 `/private` 分类及按事件筛选、多 key 聚合来自[迁移公告](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice)；聚合和筛选留作后续能力，避免漏收余额、配置、Algo 或流失效事件。

生产与测试环境使用各自固定 authority。Spot 为 `ws-api.binance.com` / `ws-api.testnet.binance.vision`，路径 `/ws-api/v3`；USD-M REST 为 `fapi.binance.com` / `demo-fapi.binance.com`，WSS 为 `fstream.binance.com` / `demo-fstream.binance.com`。USD-M 测试域来自[官方环境说明](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info#testnet-api-information)。DNS 解析结果由控制阶段提供，TLS server name 和 HTTP Host 从产品与环境推导，不接受报文改写 authority。

## 2. 状态、身份和完成点

每个 owner 固定 `risk_domain_id/venue=1/environment/account/market/credential_generation`。凭据注册表负责把 API key 与该本地账户绑定；交易所返回的订阅 ID 和 listenKey 只在这一受信绑定内解释。

`connection_epoch` 由上层持久化分配器提供起始值，进程内按 checked add 消耗，失败的建连尝试也不复用。重启后分配器给出高于旧值的新范围。每次控制请求另有不复用的 `request_serial`；USD-M HTTP 管理连接还有 `transport_attempt`。回调和回复以完整令牌关联，服务器请求 ID、subscriptionId 和 listenKey 字符串都只在对应代次内有效。新 POST 返回旧 key、新 Spot 连接再次返回 subscriptionId 0 时，旧连接的输入仍被识别为过期。

| 状态 | 已成立的事实 | 仍允许的工作 |
| --- | --- | --- |
| `Disconnected/Backoff` | 当前没有可用于接收事实的流 | 到期后建连；增险屏障保持关闭 |
| `Connecting` | 正在连接、TLS 或 HTTP Upgrade | 有界驱动网络和握手 |
| `Authenticating/Subscribing` | Spot 正在等待精确关联的认证或订阅结果 | 接收并暂存有界的提前事件，处理 Ping |
| `CreatingKey` | USD-M 正在请求 listenKey | 等待完整 HTTP 回复 |
| `Receiving` | Spot 订阅已确认；或 USD-M key 创建成功且 WSS 升级通过 | 交付事实、心跳、续期、主动轮换 |
| `Closing` | 已停止声称流连续，正在做显式关闭 | 有界完成关闭请求，不重新开启 |
| `Stopped/Exhausted` | owner 已终止或计数器耗尽 | 提供诊断；新 owner 由外层重新建立 |

`Receiving` 不包含“账户已对账”的含义。启动时和每次缺口后生成新的 `GapToken`，其中绑定账户、连接代次和缺口代次。会话模块不提供 `mark_reconciled(true)`；上层协调器以实际恢复闭包建立账户准入，并同时检查当前流仍可接收。安静账户通过认证/订阅、有效 key 与心跳建立接收状态，不等待第一笔成交。

Spot 的成功订阅 ID 可为 0。事件的 `{subscriptionId,event}` 包装精确匹配当前订阅。`session.status.userDataStream=true` 只表示连接上存在订阅，不能推导该订阅属于哪个账户；`session.subscriptions` 也只返回 ID，owner 保留自己确认过的映射。一个 owner 不在同一连接换账户或混用两种订阅方式。Spot 允许每连接 1,000 个同时订阅、累计 65,535 个订阅，本实现每次连接只建立一个订阅，关闭后换新连接。[固定版本订阅语义](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-api.md#general_info_user_data_stream_subscriptions)

先收到事件、后收到订阅回复时，owner 按接收顺序暂存原始消息及接收时间。确认成功后再按 ID 校验并交付；缓存超限、确认超时、身份冲突或格式错误均产生缺口并关闭当前连接。没有成功确认时，缓存内容不会被当作已认证账户事实发布。

USD-M POST 在账户已有有效 key 时会返回同一个 key 并续期；key 有效期为 60 分钟。续期成功才更新本地有效期，且使用该请求开始前记录的单调时间加 60 分钟作为保守边界，不从较晚的回复时间重新计算。更新要求当前请求、key 代次和旧有效期都仍有效；到期后的迟到成功回复不会复活旧流。PUT 返回 `-1125`、`listenKeyExpired` 或不同的 key 时进入缺口，重新 POST 并创建新连接代次。[USD-M 用户流规则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams#connect)

USD-M 的管理接口作用于账户当前 key，当前参数表不允许指定“只删除某个旧 key”。因此同一账户的 listenKey 管理由一个 owner 串行执行，监管进程也不能并发 POST/DELETE。正常重连和 24 小时轮换仅关闭旧 socket，不自动 DELETE；显式远端关闭是 owner 的终止操作。DELETE 结果未知时保留 `RemoteCloseUnknown`，外层不能据此立即释放管理所有权并创建一个可能被迟到 DELETE 关闭的新流。

## 3. 公共 API

下列签名对应当前实现；`request::prepare` 可独立编码，`PrivateClient` 负责实际连接及生命周期。

```rust
pub struct AccountScope {
    pub risk_domain_id: [u8; 16],
    pub venue: u32,
    pub environment: Environment,
    pub account: u64,
    pub market: Market,
    pub credential_generation: u64,
}

pub enum Authentication<'a> {
    SpotLogon { api_key: &'a str, signing_key: &'a SigningKey },
    SpotSignature { api_key: &'a str, signing_key: &'a SigningKey },
    UsdmApiKey { api_key: &'a str },
}

pub enum Operation {
    SpotLogon,
    SpotSubscribe,
    SpotSubscribeSignature,
    SpotStatus,
    SpotSubscriptions,
    SpotUnsubscribe { subscription_id: u64 },
    SpotLogout,
    UsdmCreate,
    UsdmKeepalive,
    UsdmClose,
}

pub struct RequestToken { /* 私有：connection_epoch、serial、操作 */ }
pub struct GapToken { /* 私有：scope、connection_epoch、gap_epoch */ }
pub struct PreparedControlRequest { /* 私有：令牌、路由、确切字节、摘要 */ }
pub struct SessionConfig { /* 经校验的 scope、计时、容量及重连策略 */ }
pub struct NetworkConfig { /* 已解析地址、TLS 配置；authority 固定 */ }
pub struct PrivateClient<'a> { /* 拥有连接、解析器、请求和订阅状态 */ }

pub struct ClockSample {
    pub instant: Instant,
    pub monotonic: tickwire_clock::MonoTime,
    pub signing_time: Option<tickwire_clock::SignedTime>,
}
pub trait Clock {
    fn sample(&mut self) -> Result<ClockSample>;
    // 必须检查原始签名时间、当前校准区间和剩余交付预算；无默认放行。
    fn check_signed(&mut self, scope: &AccountScope, stamp: &SignedTime,
                    now: &ClockSample, recv_window_micros: u64) -> Result<()>;
}
pub trait ControlPermit {
    // Charge 区分建连、Upgrade、API 请求、Pong/Close 帧，并绑定唯一尝试 ID。
    fn allow(&mut self, charge: &Charge, now: &ClockSample) -> bool;
}

pub struct PollBudget {
    pub io: tickwire_transport::IoBudget,
    pub max_messages: usize,
    pub max_control_requests: usize,
}
pub enum Delivery<'a> {
    State(SessionStatus),
    Gap(GapNotice),
    Event(PrivateEvent<'a>),
    Control(ControlOutcome),
}
pub enum StopMode { Disconnect, CloseRemoteStream }

impl<'a> PrivateClient<'a> {
    pub fn new(config: SessionConfig, auth: Authentication<'a>,
               network: NetworkConfig, first_connection_epoch: u64)
        -> Result<Self>;
    pub fn register(&mut self, registry: &mio::Registry,
                    tokens: RegistrationTokens) -> Result<()>;
    pub fn reregister(&mut self, registry: &mio::Registry,
                      tokens: RegistrationTokens) -> Result<()>;
    pub fn poll(&mut self, clock: &mut impl Clock, budget: PollBudget,
                permit: &mut impl ControlPermit,
                deliver: impl FnMut(Delivery<'_>) -> Result<()>)
        -> Result<PollReport>;
    pub fn request_status(&mut self) -> Result<()>;
    pub fn stop(&mut self, mode: StopMode) -> Result<()>;
    pub fn status(&self) -> SessionStatus;
    pub fn current_gap(&self) -> &GapToken;
    pub fn next_deadline(&self) -> Option<Instant>;
}
```

`new` 校验配置，不做阻塞 I/O。`poll` 执行有限工作，不睡眠，也不在内部等待下一次 socket 就绪；外部 reactor 根据 `next_deadline` 和 mio 就绪事件再次调用。重连产生新 socket 后，`PollReport.registration_changed` 提示外层注册，旧就绪通知最多导致一次无进展的非阻塞驱动。数据 socket 与 USD-M 管理 socket 使用独立的 `RegistrationTokens`。

`SessionStatus` 提供阶段、连接代次、可选订阅 ID、待确认请求令牌、key 有效期、续期时刻、远端关闭是否未知和是否需要注册。`current_gap()` 返回缺口令牌，`next_deadline()` 返回下次单调截止。`PollReport` 记录已知 I/O 计数、消息数、控制请求数、是否让出和注册变化；若底层 drive 失败且已消耗的 I/O 数量不可得，置 `io_budget_exhausted` 并停止本轮后续 I/O，不编造精确计数。`ControlOutcome` 区分 `NoBytes`、`MayHaveApplied`、`Confirmed` 和交易所明确错误；HTTP 408/5xx、连接中断和解析失败不当作确认成功。`Delivery` 回调失败会关闭接收资格并产生缺口，避免静默丢弃事实后继续显示健康。

认证编码位于新 crate 的 `request` 模块，可独立调用 `prepare(scope, auth, token, now, recv_window_micros, max_bytes)`。Spot 编码复用 `Params::websocket_preimage` 与 `SigningKey`；USD-M 输出固定方法、路径和 API key 请求头。`PreparedControlRequest` 只提供令牌、authority/path/method、payload、所需请求头和摘要的只读 getter，不暴露修改字节的方法，也不伪装成订单的 `CanonicalCommand`。摘要绑定产品、环境、账户、凭据代次、请求令牌和实际应用请求字节。

Spot 签名参数使用整数时间戳和精确的 `recvWindow` 数值，最大 60,000 毫秒、最多三位小数。缺少可靠签名时间时拒绝新的认证请求；已建立的流仍可处理事实和心跳。发出的 `id` 来自 owner 的请求序号，不接受服务器指定序号。`session.logon` 的 key 类型在编码前检查；其余签名订阅复用已有 HMAC/RSA/Ed25519 能力。

网络实现始终使用 `tickwire-transport::Connection`。TLS 证书和主机名校验保持开启，早数据关闭；随机 WebSocket key 和掩码由操作系统随机源生成。HTTP Upgrade 校验成功后，将同一次读取中剩余的帧字节交给 WS 解析器。WS 使用 `poll_message` 的 `Message/NeedInput/Yielded` 三态，预算耗尽时交回调度器，避免将已缓存的后续帧误当成缺少网络数据。

首次接受控制请求字节前，owner 先重新采样，检查原请求的单调域、Source、签名时间和截止，再同步调用 `ControlPermit`；回调结束后再次采样并执行同样的时钟检查；最后一次 check_signed 返回后再采样，核对域、顺序、原请求 fresh 和本地 deadline，随后不再执行许可或预算回调。等待可写期间不保存旧许可，排队过久的签名按 NoBytes 拒绝。`PreparedControlRequest` 保存由 `tickwire-clock` 构造的原始 `SignedTime`，不会用新的裸时间戳重绑旧请求。`Clock::check_signed` 由适配器调用 `Calibrator::check_send`，检查当前校准区间和剩余交付预算；本库没有 SystemTime 或默认放行实现。owner 首次采样固定 `ClockDomain`，之后域变化会永久停止该 owner。请求已被 TLS 接受后，超时或断连保留不确定结果。订阅确认丢失时关闭旧连接再订阅；USD-M POST/PUT 可按有界退避发起新的管理尝试，但旧成功回复无法延长新状态。`Flushed` 只表示本机输出完成，状态推进仍等待协议响应。

`Charge` 的计费边界包括 TCP 建连尝试、HTTP Upgrade 应用输出、API 请求权重和 WebSocket 控制帧；TLS 握手属于获准建连的一部分，不另算一条 WS 消息。首次写 WouldBlock 后会用相同尝试 ID 再核许可，外部限流器可保留该 ID 的额度登记，同时复核当前许可。库提供这些调用点及本地有界队列；只有接入账户/IP 共同使用的限流器，才形成共享限流。凭据、请求、客户端、状态和错误的 Debug/Display 均不输出 listenKey、签名或 API secrets；明确读取请求字节的接口只供发送和测试核验。

## 4. 心跳、续期、轮换和事实交付

Spot WS API 约每 20 秒发 Ping，服务端一分钟内未收到正确 Pong 会断开；USD-M 当前文档为每 3 分钟发 Ping、10 分钟未收到 Pong 断开。两者均有 24 小时连接期限。客户端立即回送 Ping 的原始载荷，不用业务事件频率判断空闲账户是否健康。[Spot 连接规则](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-api.md#general-api-information)，[USD-M 连接规则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)

计时配置采用有界策略：默认在 23 小时主动轮换；USD-M 每 30 分钟申请续期，并保留请求超时与重试余量；这些是本地策略，分别早于官方连接和 key 截止。配置校验拒绝续期间隔加最大请求期限已经达到 60 分钟的组合。请求、心跳、key、轮换和退避均由单调时钟驱动；Unix 时间只用于需要签名的参数。单调时间回退或计算溢出使当前代次失效。

`serverShutdown` 触发立即建立新连接的流程，不假设通知后还有固定十分钟。Spot 的 `eventStreamTerminated` 对当前 subscriptionId 生效；`session.logout` 只关闭登录态订阅，签名订阅使用显式 unsubscribe。USD-M 的 `listenKeyExpired` 即使 socket 仍连接也立即结束接收资格。[Spot 终止事件](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/user-data-stream.md#event-stream-terminated)，[关闭通知修订](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/CHANGELOG.md#2026-06-09)，[USD-M 失效事件](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams#event-user-data-stream-expired)

第一版轮换采用关闭旧连接、递增代次、重新认证/订阅的串行流程，明确产生缺口。双连接重叠切换和跨连接去重留给后续阶段；不会将“两个 socket 都连通”当作连续账本证据。已知 EOF 时立即发布缺口；已经完整收到的旧连接事实继续带原 SessionContext 排空，期间状态为 Backoff，不声称健康，也不开始新连接。排空完成后才递增连接代次。

控制消息和 Pong 共用有界发送队列，Pong 保留容量并优先于尚未开始输出的 API 请求；已部分写出的 WebSocket 帧先完成，避免把 Pong 插进帧中。API 请求和协议控制帧分别计数，同时接受外部共享 IP/账户限流器约束。429/418 进入冷却；预算不足或延迟超过心跳期限时关闭流并发出缺口，不无限积压。

正常数据经已有 `events::decode_private` 解析为 `PrivateEvent`，保留实际接收时间、原始 JSON、当前 SessionContext 和业务事件字段。每次回调的借用只在该回调内有效；持久化方在返回前复制或写入其拥有的记录。单条消息上限不超过现有解码器的 1 MiB，提前事件缓存、控制响应、帧数量和每轮交付数都有独立上限。未知事件仍保留原文；出现已知终止事件时先撤销流资格，再向调用者交付通知。

USD-M 的同连接、同事件类型顺序保证不提供跨重连的连续游标；Spot 订阅也没有可供本模块恢复遗漏区间的全局序号。会话层不根据 `E/T` 丢弃迟到或重复经济事实。订单身份关联、成交去重、Fee/Terminal 合并及 WAL 顺序继续使用[私有事件接口](binance-streams-interface.md)和账户协调器。

## 5. 实现与验证分工

源码为 `crates/tickwire-binance-private/src/{lib,request,response,session,client}.rs`，测试按请求编码、网络流程、故障、两产品生命周期和时钟绑定分组。正式规格见 [binance-private-session.md](../../specs/implementation/binance-private-session.md)，原始日志见验证目录。依赖现有 Binance、transport、venue、clock 模块及 JSON/SHA-256；随机源复用 transport，现有 feed/events/depth_sync 未修改。

| 行为及协议测试 | 实现后的可观察结果 |
| --- | --- |
| 本地 TLS/WSS 服务端校验 Ed25519 logon 原文及签名，再返回 subscriptionId 0 | 客户端按顺序登录、订阅并解码真实推送；不跳过认证 |
| HMAC/RSA/Ed25519 签名订阅分别经过实际 WSS | 服务端从收到的参数独立重建签名原文，字节与签名一致 |
| 错误 key、产品/环境不符、错请求 ID、错 subscriptionId、`id:null` 撤销错误 | 失败关闭；错误回复不能激活或续期当前流 |
| 事件先于订阅回复、回复与事件处于同一 TCP 读取、提前队列溢出 | 保序、保留接收时间；超限产生缺口且不交付半成品 |
| 实际 HTTPS POST/PUT/DELETE 与 WSS `/private/ws/<key>` | 校验 API key 头、空参数和方法；服务端随后推送真实订单事件 |
| POST 返回相同 key、PUT 延迟/超时/`-1125`、key 过期但 socket 未断 | 独立代次仍递增；只有及时匹配的成功回复延长有效期 |
| 旧请求回复、旧流事件在重新建立连接后到达 | 保持新会话状态和期限；不能冒用新 SessionContext |
| Ping/Pong、碎片帧、33 片让出、主动轮换、serverShutdown | 有界推进；Pong 内容正确；轮换发出缺口 |
| 部分写、TLS 接受后断连、确认丢失、零字节超时 | 分别保留不确定结果或 NoBytes；无旧请求自动复活 |
| 关闭签名订阅、USD-M DELETE 未知、下游回调拒绝 | 精确关闭语义；不提前释放管理所有权或声称事实连续 |

网络测试使用同一生产客户端和请求编码器连接本地协议服务器，测试 CA 正常完成证书及主机名校验。服务端检查收到的 HTTP/WS 字节和认证内容，按脚本注入延迟、碎片、错误和断开；计时边界的单元测试由可控时钟驱动。

独立 `binance_private_session` TLA 阶梯按实际单 owner 同步调用建立，验证范围与结果分别记录在模型的 00/05 文档。计数器真实 u64 边界由源码单元测试覆盖，不将小计数器域冒充实际耗尽条件。密码学、TLS 内存行为、交易所是否完整发出事实、账户对账算法和订单发送授权不作为该会话模型的已证结论。

本阶段交付范围包括实际认证/管理请求、非阻塞网络 owner、订阅与 key 生命周期、事件交付和上述本地协议测试。账户快照/历史查询分页、恢复闭包、跨连接事实去重、多账户连接聚合、SBE 私有事件以及私有流直接驱动 AccountGateway 的组合仍由后续模块承担。
