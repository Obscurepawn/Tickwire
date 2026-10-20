# Binance 盘口启动与重连协调

`depth_sync::DepthSync<P, LEVELS, UPDATES, BUFFER>` 将 JSON 深度解码、有限增量缓存与 `tickwire-book` 接在同一 owner 内。`P` 只允许 `BinanceSpot` 或 `BinanceUsdM`，产品与序列规则不会由两个可矛盾的参数分别指定。配置固定标的目录、BookKey、时间单位、REST 深度和消息长度；网络 owner 提供已分帧的响应体与增量消息。

## 调用顺序与完成点

1. `connect(connection_epoch)` 要求连接代次严格增加，清除旧缓存、请求和盘口可见性，返回私有构造的 `ConnectionToken`。连接代次属于这条实际 WebSocket 连接，不能用于另一条连接。
2. owner 完成指定 depth 流的握手或核对 SUBSCRIBE 成功回报后调用 `subscription_ready(token)`。从 `connect` 起就可以通过 `on_delta(token, bytes, received_mono_ns)` 缓存消息，因此数据先于订阅确认到达也不会丢失。
3. 只有订阅已确认且至少收到一条增量时，才能调用 `request_snapshot(now, deadline)`。返回的 `SnapshotRequest` 包含私有令牌和已编码的请求 path；owner 使用同一请求对象关联 HTTP 完成回调。路径固定 symbol 和 limit，Spot 为 `/api/v3/depth`，USD-M 为 `/fapi/v1/depth`。
4. `on_snapshot(token, status, bytes, now)` 先检查令牌和期限，再解码、安装快照、按收到顺序应用整个缓存。完成回放后才返回 Live；只有快照而没有可桥接更新时维持 Bridging。外部仅能通过 `view(now, maximum_age)` 取得有效 BookView。
5. `snapshot_failed(token, now)` 或非 200 响应取消本次请求并保留缓存，供限流/退避后重新请求。`expire_snapshot(now)` 处理没有回调的超时。`disconnect(token)` 立即关闭可见性；下一条物理连接需要更大的连接代次。

单个协调器只有一个当前快照请求。正在请求时重复请求返回错误；取消或超时后的新请求使用新的序号，旧完成回调无效。旧连接、旧请求和重复响应在 JSON 解码及时间检查之前直接忽略，当前状态保持不变。当前回调的单调时钟回退触发重建。所有 `now` 与 `received_mono_ns` 使用同一时钟；owner 在串行交付回调时读取时间。回放沿用首次收到消息时保存的时间，不用回放时刻覆盖它。

## 缓存、序列与重建

FIFO 在构造时预留 `BUFFER` 个解码事件的空间，容量按事件数检查，不用覆盖旧元素来腾位置。单条增量双侧合计最多 `UPDATES` 项；`LEVELS` 和 `UPDATES` 均为 1–5000，每边最多 `LEVELS` 档，REST 返回每边超过所请求 limit 时拒绝。缓存逻辑容量最多 4096 条，按事件对象大小计算的预留空间最多 64 MiB；JSON 字符串解码仍沿用 feed 的分配行为，未声明整条链零分配。

Spot 在安装前检查快照 `L >=` 本轮最早缓存消息的 `U`，较旧时保留整个缓存重试。之后由独立 Book policy 丢弃 `u <= L`，以 `U <= L+1 <= u` 桥接和推进。USD-M 丢弃 `u < L`，首条应用要求 `U <= L <= u`，后续检查 `pu` 等于本地更新 ID。[Spot 固定官方步骤](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-streams.md#how-to-manage-a-local-order-book-correctly)、[USD-M 官方步骤](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)。

解码错误、缓冲溢出、Book 容量/输入错误和序列断档使本轮失效：协调器增加 Book generation，清空缓存、当前请求及首条 U，保留健康连接的订阅状态。再收到首条有效增量后可重新请求快照。失效消息不放入下一轮；新快照必须覆盖下一轮的缓存起点。重建并不自动发请求或循环重试，网络 owner 根据返回结果安排退避，继续消费连接以免堆积。

连接代次和 Book generation 分别标识物理连接与同步轮次：同一连接的重同步也会增加 Book generation。请求令牌同时绑定 BookKey、连接代次、Book generation 与单调请求序号。Book generation 或请求序号耗尽时关闭可见性并返回 Exhausted。连接代次由调用者分配，任何不递增的值都被拒绝，计数器不回绕。协调器不实现 Clone；重建进程中的 owner 应从其持久代次分配器取得新的初始代次，避免把旧回调对象交给重建的实例。

## 知识边界与接线责任

REST 深度上限为 Spot 5000、USD-M 1000；本模块要求配置容量能容纳请求的档数。有限快照始终保留双侧 `complete=false`。增量不扩展已知覆盖范围，最差已知价位以外的未存价位仍为 Unknown；BookView 的新鲜度使用最后实际应用增量的原始接收时间，回放和 REST 完成不刷新这个时间。

一次 `on_snapshot` 可能在回放到后段时失败。因为 owner 的 `&mut self` 调用串行且 Book 不对外提供可变引用，调用期间不会暴露前段暂时 Live 的状态；失败返回后已清空本轮并关闭 view。缓存按 WebSocket 接收顺序回放，不跨连接排序、拼接或合并同价位消息。

网络 owner 负责 TLS/WSS 握手、订阅确认、Ping/Pong、HTTP 状态/请求关联、连接关闭通知和调度。此模块消费真实协议字节并返回可发送的 REST path，可直接接入现有 transport。私有流登录/续期、轮换策略和多源切换发布不在该协调器内。
