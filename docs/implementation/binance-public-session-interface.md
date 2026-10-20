# Binance 公共深度网络接口

`tickwire-binance-public` 用现有 TCP/TLS、HTTP、WebSocket 编解码器驱动真实公共深度连接，并拥有一个 `DepthSync<P, LEVELS, UPDATES, BUFFER>`。一个实例固定一个产品、环境、标的目录版本和普通 diff-depth 流，负责订阅、心跳、快照请求、重同步与连接轮换。公共类型见 [lib.rs](../../crates/tickwire-binance-public/src/lib.rs)，网络 owner 见 [client.rs](../../crates/tickwire-binance-public/src/client.rs)。

数据顺序为：WSS 握手与订阅 → 缓存增量 → HTTPS 深度快照 → `DepthSync` 完整回放 → 有效 BookView。增量持续通过同一生产解码器和 Book policy；测试替换远端协议服务及时间、额度来源，不替换这条调用链。Spot SBE 行情及 FIX 行情仍按[主设计](../design/binance-ultra-low-latency.md)分别接入；本模块先完成普通 JSON 深度网络链路，API 不提供尚未实现的 SBE 开关。

## 固定身份与 API

`P` 复用已封闭的 `BinanceSpot` / `BinanceUsdM` policy。`Source` 区分 Spot/USD-M 和 Production/Test，与 `P::MARKET`、目录合约种类共同校验。标的、价格 tick、数量 step、BookKey、目录摘要、时间单位和快照档数沿用 [DepthSync 配置](binance-depth-sync-interface.md)，构造后不从可变目录补字段。目录改变时关闭本实例并建立新的来源代次。

```rust
pub struct PublicDepthClient<P, C, const L: usize, const U: usize, const B: usize> {
    /* 独占 WSS、至多一条 HTTPS、DepthSync、时钟及有界缓冲 */
}

pub enum SubscribeMode {
    RawUrl,
    Explicit,
}

pub enum DepthSpeed {
    Default,
    Ms100,
    Ms500,
}

pub struct PollBudget {
    pub io: tickwire_transport::IoBudget,
    pub max_ws_messages: usize,
    pub max_decoder_turns: usize,
    pub max_notifications: usize,
}

pub trait PublicPermit {
    fn allow(&mut self, charge: &PublicCharge, now: MonoTime) -> bool;
    fn observe_http(&mut self, feedback: &HttpFeedback);
}

// C 使用现有 MonotonicClock；生产适配器为 SystemClock。
impl<P, C, const L: usize, const U: usize, const B: usize>
    PublicDepthClient<P, C, L, U, B>
{
    pub fn new(config: Config, network: NetworkConfig, clock: C) -> Result<Self>;
    pub fn start(&mut self, permit: &mut impl PublicPermit) -> Result<()>;
    pub fn poll(
        &mut self,
        permit: &mut impl PublicPermit,
        budget: PollBudget,
        sink: &mut impl PublicSink,
    ) -> Result<PollReport>;
    pub fn stop(&mut self, sink: &mut impl PublicSink) -> Result<()>;
    pub fn status(&self) -> Status;
    pub fn view(&mut self, maximum_age_ns: u64) -> Result<BookView<'_>>;
    pub fn register(&mut self, registry: &mio::Registry, tokens: IoTokens) -> Result<()>;
    pub fn reregister(&mut self, registry: &mio::Registry, tokens: IoTokens) -> Result<()>;
}
```

`Config` 另固定外部分配的 `owner_id`、`ClockDomain`、首次连接代次、连接/握手/订阅/首条增量/快照期限、轮换周期、退避上下限和内存上限。`NetworkConfig` 提供分别解析好的 WSS/REST `SocketAddr` 与 TLS 根证书配置；产品及环境决定 SNI、Host 和路径，测试地址覆盖只替换连接地址。禁止关闭主机名验证、TLS early data、重定向或从任意 URL 猜测产品。DNS 解析属于外部控制服务，结果变化通过明确重连生效。

`Status` 分别呈现网络阶段与同步状态：`Idle / Connecting / Upgrading / Subscribing / Receiving / Closing / Backoff / Stopped` 不替代 `DepthSync::State`。`Receiving` 仅表示当前订阅连接可接收消息，Book 是否 Live 以及是否过旧由 `view` 当场检查。`view` 自行读取固定域的单调时间，返回借用存续时阻止再次 `poll`；它不会让已返回的旧 view 随时钟自动失效。

`start` 只从 `Idle` 启动；重复调用返回明确状态错误，保留当前连接。退避后的重连由 `poll` 推进。`stop` 幂等，重复停止不增加代次或再次发出同一缺口；`Stopped` 之后重启需要新的实例与来源代次。

外部分配器保证同一 `(owner_id, ClockDomain)` 下连接代次和本地操作序号重启后不复用。WSS 代次与 Book generation 分开，HTTPS 尝试另有操作序号，并保存协调器返回的原 `SnapshotRequest`。任一计数耗尽、时钟域改变、时间回退或算术溢出均关闭可见性并进入 `Stopped`。构造器与全部容量相乘、相加使用 checked 运算；不实现 Clone，也不向调用者交出可变 Connection 或 DepthSync。

## 端点、订阅与协议参数

| 产品环境 | WSS authority / 路由 | HTTPS authority / 快照 |
| --- | --- | --- |
| Spot Production | `stream.binance.com:443`，`/ws/<stream>` | `api.binance.com`，`/api/v3/depth` |
| Spot Test | `stream.testnet.binance.vision:443`，`/ws/<stream>` | `testnet.binance.vision`，`/api/v3/depth` |
| USD-M Production | `fstream.binance.com:443`，`/public/ws/<stream>` | `fapi.binance.com`，`/fapi/v1/depth` |
| USD-M Test | `demo-fstream.binance.com:443`，`/public/ws/<stream>` | `demo-fapi.binance.com`，`/fapi/v1/depth` |

Spot 的地址与 UTF-8 说明见[固定生产流文档](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-streams.md#general-wss-information)及[Testnet 文档](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/testnet/web-socket-streams.md#general-wss-information)。USD-M 的 `/public` 路由由[连接规则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)确定，Demo authority 来自[总则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info#testnet-api-information)。Demo 使用相同路由作为明确运行配置，握手或订阅不支持时返回失败。

流名采用目录 symbol 的小写形式；原始目录 symbol 用于 REST 参数及事件身份核验。URL 编码对非 ASCII 字符的每个 UTF-8 字节及保留分隔符做百分号转义，价格保持精确十进制。Spot `Default` 为 `@depth`，`Ms100` 为 `@depth@100ms`；USD-M `Default` 为 `@depth`，另支持 `@100ms` 与 `@500ms`。`Ms500` 对 Spot 拒绝，USD-M 的 100ms 档是否存在仍由订阅结果和实际首条事件确认。[Spot diff depth](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-streams.md#diff-depth-stream)、[USD-M Public 流定义](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public#diff-book-depth-streams)。

`RawUrl` 在 URL 指定唯一流，只有完整 Upgrade 请求已被接受且 101/Accept 验证成功后才调用 `subscription_ready`。`Explicit` 使用产品对应 `/ws` 或 `/public/stream` 入口，发出唯一流的 SUBSCRIBE；保存原请求 ID、流名和连接代次，完整请求输出后仅接受匹配 ID 且 `result:null` 的成功回报。控制回报的重复身份字段、`id:null`、错类型及错误结果不能确认订阅。先到的增量仍进入当前连接的协调器缓存，匹配确认后再启动快照。

控制请求采用递增的正 `u32` ID，不复用；USD-M [订阅指南](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams)明确列 unsigned INT，而新 API 目录示例使用字符串。本接口选择指南中明确支持、也符合 Spot 整数规则的数值形式，不在接收时把字符串 ID 强转成数值。换标的通过新实例完成，不在已绑定 Book 上动态替换订阅集合。

Spot 可配置毫秒或微秒，并将 URL `timeUnit` 与深度解码上下文绑定；USD-M 固定毫秒。combined 包装层的流名也须精确匹配本实例；普通 depth、partial depth、bookTicker、RPI depth 不能混用。USD-M 继续由现有 feed 检查 `st`/合约身份；错误产品和未知必需身份字段使当前同步失效。

## 接收时间、快照和有界调度

本模块的 `received_mono_ns` 表示应用接收时刻，不是内核、网卡或交易所时间。每次从 transport 取得一批解密字节时保存原单调时间，解码器之外维护有界的“字节数、时间”队列。每次 `poll_message` 前后的 `buffered_len` 差值推进该队列；完整消息使用其最后一个完成字节所在批次的时间。碎片最终合并、预算让出或等待订阅确认均保留这个时间。采样可采用读取批次前的保守时间；不在数轮后的回调里重新赋予较新时刻。

同一消息的诊断通知、`DepthSync::on_delta` 和 Book 更新使用同一个接收时间。交易所 `E/T` 独立保留，不参与本地期限。此 owner 当前只交付深度，不从成交推送刷新 Book 新鲜度；后续成交 owner 使用同一“完整消息最后一段字节”的时间定义。

现有协调器把增量接收时间与快照动作的 `now` 放在同一单调序列内。为避免较新的 HTTP 完成先于预算滞留的旧增量，调度采用以下顺序：

1. 一个 lane 的已有解密字节先按其原批次时间搬入解码器；未搬完前不再次 `drive` 该 lane，避免将新旧批次合并成新的时间。
2. 准备调用 `request_snapshot`、`on_snapshot`、`snapshot_failed` 或 `expire_snapshot` 时，暂停 WSS 新增读取。这四个入口都会推进协调器的 `last_clock`，因此 HTTPS 传输失败也遵守同一顺序。按消息数和解码轮数预算处理已接收的完整消息；`Yielded` 表示继续调度，只有 `NeedInput` 才表示当前完整消息已耗尽。
3. 此后采样新的 `now` 并执行快照动作。已有不完整消息保留，未来完成它的最后一段字节是在该动作之后取得，因此接收时间不会倒序。恢复 WSS 读取后继续正常推进。

暂停追加输入让这个步骤只处理当前有限缓冲；耗尽这批消息后即可执行快照动作。HTTP 已完整收到的响应只保留一份有界对象，等待上述顺序完成；调用协调器时仍重新检查原快照期限。若全局硬期限、时钟故障或连接失效要求立即关闭，则先 `disconnect` 关闭 Book，旧内容随后只可作为诊断处理。

只有订阅成功且收到第一条增量后才申请快照；订阅确认后仍无增量直至首条增量期限时，关闭并退避重连。HTTPS 路径直接使用 `SnapshotRequest::path()`，一个请求一条独占连接，完整请求、单一完整 HTTP 响应、无多余响应字节才构成可交付结果。先排空已解密数据再解释 EOF，完整 body 与 EOF 同批到达仍能成功；半包、响应过大、非法 framing、错产品或部分请求输出后的抢先响应均失败。取消/期限到达时销毁该 HTTPS 连接，旧 token 在解码与时间处理前失效。

Snapshot 408/5xx、传输错误和过期只结束当前读请求并有界退避；下一次尝试使用新的请求 token。Spot 快照过旧也保留 FIFO 后重试。数据断档、错误增量或缓存溢出按 DepthSync 的 `Rebuild` 清理本轮，健康 WSS 的订阅资格可以沿用。单个协调器至多一个请求，重复请求不会产生并行快照或把较晚完成的旧响应装入新 generation。

`PublicSink::on_notice` 按顺序接收 `SyncOutcome`、缺口通知、HTTP 限流反馈和有限诊断。Sync 通知携带可选 BookView 及去掉 combined 包装后的原增量 `raw_delta`，交易所 `E/T` 因而可由发布层原样处理；所有借用仅在回调中有效。每条完整增量都先经过协调器，消息预算不会静默合并或覆盖事件。回调返回 false 时立即关闭本来源，并让 `poll` 返回 `Error::Sink`。SHM 发布者在接线层处理来源失效与新 generation；本 owner 不自行生成交易风险许可。

`stop` 先使 Book 失效并销毁连接，再向 sink 发送缺口；即使 sink 拒绝也保持停止。`view` 遇到永久时钟错误时先关闭来源，再返回错误，并在固定的单槽状态中保留缺口通知；下一次 `poll` 首先交付该通知，停止状态仍允许这一交付，但不再启动连接。这样不用无限通知队列，也不会把尚未处理的缺口越过新的有效状态。

`PollReport` 给出实际 I/O 消耗、已解码/已应用/被忽略计数、剩余可运行工作、下一期限，以及替换 socket 后需要重新注册的通知。两条连接共用一份剩余 `IoBudget`，不各自花一次完整预算；transport 返回可能已消耗 I/O 的错误后，本轮停止继续 I/O。解码器的 `Yielded` 与真正缺输入分别返回。缓冲预算包含已配置的 transport 收发缓冲、HTTP、WS 碎片、接收时间片段、控制帧和 DepthSync FIFO，构造时检查容量和与溢出；TLS 内部对象与分配器开销不按此字段逐字节统计。任一消息容量不足都关闭或重建，不丢弃旧增量腾空间，也不声称 JSON 链路零分配。

## 心跳、轮换与共享额度

Spot 官方流规则给出 24 小时连接寿命、约 20 秒 Ping 和一分钟 Pong 要求；USD-M 为 24 小时、3 分钟 Ping 和 10 分钟 Pong 要求。收到 Ping 后原载荷进入保留容量的 Pong 队列，优先于尚未开始输出的订阅控制消息；已接受前缀的帧先完成，避免插入另一帧破坏字节流。排队不等于服务端收到 Pong，未能在本地响应期限内完成输出就关闭来源。已有消息流量不代替心跳检查。[Spot 连接规则](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-streams.md#general-wss-information)、[USD-M 连接规则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)。

轮换默认在连接尝试起点后的 23 小时开始，配置上限严格小于 24 小时并预留握手、退避与关闭余量。轮换、Close、EOF、Ping 超时和 Spot `serverShutdown` 先关闭可见性，再断开旧连接、增加代次并重新订阅与快照。本模块采用串行轮换，明确产生重建间隔；后续双连接切换须让备用源独立同步，不能把两条流交错写进同一个 Book。已观察 EOF 的同一 poll 中，即使还有订阅 ACK 和增量，也不能短暂重新呈现有效来源。公共深度在失效后可丢弃旧缓冲并重建；它不同于必须排空并入账的私有成交记录。

`PublicCharge` 绑定 `(owner_id, ClockDomain, Source, lane, connection_epoch, operation_seq, kind)`，区分 TCP 尝试、Upgrade、订阅控制、Pong/Close 和快照 GET。外部额度 owner 按同一尝试幂等登记，但每次零接受重试都重查当前许可；首次输出前后重采样单调时间和期限。TLS 接受任何应用字节后只继续该不可变报文，关闭重试使用新连接、新操作号。连接尝试、IP 请求权重和每连接控制帧预算分别计费，不把公共数据推送当成客户端输出额度。

Spot 控制帧限制为 5 条/秒，USD-M 为 10 条/秒；本实例仅订阅一个流。Spot 每 IP 五分钟 300 次连接尝试需要与其他实例共享计数。快照请求权重随 limit 确定：Spot 为 5/25/50/250，对应 1–100/101–500/501–1000/1001–5000；USD-M 为 2/5/10/20，对应不大于 50 的有效档位/100/500/1000。最终额度由共享限流器授权，当前模块不提供默认放行实现。[Spot 限流](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-streams.md#websocket-limits)、[Spot 深度权重](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/rest-api.md#order-book)、[USD-M 深度权重](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#order-book)。

HTTP 状态及原权重响应头向 `PublicPermit::observe_http` 反馈，429/418 进入冷却；有效的正整数 `Retry-After` 秒数经 checked 换算后设置单调截止。缺失、非法或无法表达时，本实例停止新的快照尝试；控制服务完成对账和限流恢复后，用新来源代次重建实例。USD-M 保留其实际字段，不套用 Spot 的未声明字段保证。快照 GET 无订单副作用，超时后的处理是重试读取并重新验证序列，而不是推导交易成功或失败。

在订阅已收到首条增量、协调器为 Buffering 且没有在途快照时，下一次请求时间为本地退避与服务器冷却截止的较大值；`next_deadline_ns` 将它与连接、心跳及作业期限共同取最小值。已到期时允许返回过去的期限，让 reactor 立即继续调度。未知冷却期限不创建自动重试定时器。

## 先行验证与后续接线

| 先行 Red / 协议测试 | 要观察的行为 |
| --- | --- |
| Spot/USD-M、Production/Test 四组真实本地 WSS+HTTPS | 正确 SNI/Host/路由、流名、limit；相同生产 owner 完成快照与桥接 |
| RawUrl 握手、Explicit SUBSCRIBE、数据先于 ACK、ACK 与数据同批 | 只有准确确认允许快照；提前增量不丢失、不改时间 |
| 错 ID、重复 JSON 身份、错 stream/symbol/st、binary 或 partial-depth 混入 | 来源失效；错误消息不能确认订阅或变成有效 Book |
| 旧连接、旧快照、超时后迟到响应 | 不触发新实例的解码/时钟错误，不复活旧 generation |
| Spot 过旧快照、USD-M `pu` 缺口、FIFO/数组/容量上限 | 有界重试或重建；没有暂态 Live 或伪全量覆盖 |
| WS 首批完整消息多于预算，同时 HTTPS 完成或传输失败 | 暂停新 WS 读取，处理至 NeedInput 后才推进成功/失败快照回调的时钟；原时间不被刷新 |
| 33 片 Yielded、跨读碎片、控制帧穿插、持续服务器输入 | 下一轮继续可运行工作，快照不会因持续追加输入饥饿 |
| 完整 HTTP+EOF、半包+EOF、请求仅接受前缀就收到回包 | 仅完整关联请求/响应可安装；失败保留正确 token 生命周期 |
| ACK+增量+EOF 同批，轮换/Shutdown 与在途快照交错 | 先关闭可见性，旧内容和旧回包不能再次开放 |
| Ping/Pong 原载荷、Pong 队列满、共享额度撤销、部分写 | 正确帧序与计费；无法及时回复时关闭，不跨连接重放旧帧 |
| 429/418、权重反馈、退避满值、domain/时钟回退/计数耗尽 | 期限与身份封闭，无零等待风暴或回绕 |
| 有限快照边界外删价、旧 view 借用、下游拒绝 | Unknown 与调用时 freshness 保留；失败不发布新的有效状态 |

快照每边最大档数沿用 Spot 5000 / USD-M 1000，`complete=false`，覆盖外仍为 Unknown。网络 owner 不更改已验证的 Spot/USD-M 序列 policy；具体边界继续由[深度协调器](binance-depth-sync-interface.md)及其已有测试/模型承担。新模型应覆盖双连接调度、控制响应与 EOF、时间片段和快照调用顺序，密码学、完整交易所数据与挂起硬件行为不纳入该模型的结论。

后续接线按独立模块推进：目录 HTTP 加载与版本发布、Spot SBE 数据帧及 Ed25519 key 握手、其他行情类型、多个标的共享连接、备用源独立同步切换，以及 Book 到 MarketRecord/SHM 的发布。SBE 需要绑定官方 schema 并区分 JSON 控制帧与二进制行情；其现有公开要求见[固定 SBE 行情文档](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/sbe-market-data-streams.md)。这些事项保留为后续实现，不计入当前 JSON 深度 owner 的完成范围。
