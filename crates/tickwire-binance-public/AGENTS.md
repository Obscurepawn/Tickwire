# AGENTS.md — tickwire-binance-public

## 职责
公共行情网络 owner：把生产 TCP/TLS、HTTP 快照、WebSocket 订阅接到 `tickwire-binance::depth_sync::DepthSync`。`PublicDepthClient` 建立公共 WSS 订阅，收到首条增量后请求 HTTPS 快照，回放缓存并交付借用 `BookView`，同时处理 Ping/Pong、Ack、限流反馈、断档重建、指数退避重连与 24 小时寿命前主动轮换；`PublicEventClient` 负责 Trade/BBA/MarkFunding 等独立事件流（JSON 与 Spot SBE）。它是行情链路的网络驱动层，不持有授权或订单逻辑。

## 公共 API 入口
- `PublicDepthClient<P, C, L, U, B>`（`client.rs`）：`new(config, network, clock)`、`new_sbe(config, network, clock, access: SbeAccess)`、`start(permit)`、`poll(permit, budget, sink) -> PollReport`、`status() -> Status`、`view(age) -> Result<BookView>`、`stop(sink)`。
- `PublicEventClient<C>`（`event.rs`）：`new(config: EventConfig, network: EventNetworkConfig, clock)`、`new_sbe(...)`、`start(permit)`、`poll(budget, permit, sink) -> PollReport`、`status() -> EventStatus`、`stop(sink)`。
- 配置：`Config { owner_id, source, domain, first_connection_epoch, first_operation, depth: DepthConfig（转发 depth_sync::Config）, subscribe: SubscribeMode, speed: DepthSpeed, timing: Timing, limits: Limits }`；`NetworkConfig { stream_address, snapshot_address, tls }`；`SbeAccess::ed25519_api_key(key)`（仅字母数字，≤512，不落日志、不发往 REST 快照端点）。
- 事件侧：`EventConfig { owner_id, source, domain, first_connection_epoch, first_operation, context: EventContext, encoding: EventEncoding, subscribe, fast_mark, timing, limits, max_events }`、`EventNetworkConfig { stream_address, tls }`、`EventStatus`、`EventGap`、`EventNotice::{Event, Gap, Http}`、trait `EventSink::on_event`；再导出 `EventContext/EventEncoding/EventKind/StreamIdentity`（来自 `tickwire-binance::market`）。
- 额度/通知：`Lane::{Stream, Snapshot}`、`ChargeKind::{Connect, Upgrade, Subscribe, Pong, Close, Snapshot}`、`PublicCharge`、`HttpFeedback`、trait `PublicPermit::{allow, observe_http}`、`Notice::{Sync, Gap, Http}`、trait `PublicSink::on_notice`、`PollBudget`、`PollReport`、`IoTokens { stream, snapshot }`。
- 状态：`Phase::{Idle, Connecting, Upgrading, Subscribing, Receiving, Closing, Backoff, Stopped}`、`Status`、`GapNotice`、`DepthEncoding::{Json, SpotSbeV1}`；`SubscribeMode::{RawUrl, Explicit}`、`DepthSpeed::{Default, Ms100, Ms500}`。

无 binary、无 example。

## 在 workspace 中的位置
- 上游依赖（`[dependencies]` 中的 tickwire-*）：`tickwire-binance`、`tickwire-binance-clock`、`tickwire-book`、`tickwire-transport`；外部 `serde`、`serde_json`。
- 下游（已在各 `Cargo.toml` 核对）：`tickwire-app`、`tickwire-binance-quota`、`tickwire-market-publisher`、`tickwire-md`、`tickwire-runtime`、`tickwire-strategy-host`、`tickwire-strategy-sdk`。

## 对应规格
- ../../specs/implementation/binance-public-session.md

## 构建与测试
```sh
cargo test -p tickwire-binance-public
cargo build -p tickwire-binance-public
cargo clippy -p tickwire-binance-public --all-targets -- -D warnings
cargo fmt -p tickwire-binance-public
```
`tests/`（`common/` 本地 mock TLS，不访问真实网络，无 OS 条件编译）：`network.rs`、`rebuild.rs`、`control.rs`、`scheduling.rs`、`timer.rs`、`events.rs`、`sbe_config.rs`、`sbe_network.rs`、`constructor_stack.rs`。

## 关键不变量与注意事项
- **端点固定**：Spot Production `stream.binance.com` / `api.binance.com`；Spot Test `stream.testnet.binance.vision` / `testnet.binance.vision`；USDM Production `fstream.binance.com` / `fapi.binance.com/public`；USDM Test `demo-fstream` / `demo-fapi/public`（出处：`protocol.rs::endpoints`）。
- **快照权重按深度档**：Spot ≤100=5、101–500=25、501–1000=50、>1000=250；USDM ≤50=2、51–100=5、101–500=10、>500=20；其余连接/订阅/Pong/Close 权重 0（出处：`protocol.rs::weight`、`client.rs::charge`）。
- **SBE 限制**：`new_sbe` 仅允许 SpotProduction + Spot 策略 + 微秒时间单位 + Default 速度；REST 快照仍走 JSON；SBE key 不发往 REST（出处：`client.rs::new_sbe`、`SbeAccess`）。
- **配置校验**：Spot 不允许 Ms500；USDM 不允许微秒；`tls.enable_early_data` 必须关闭；rotate < 24h；pong 上限 Spot 60s / USDM 600s；各类容量有界（出处：`protocol.rs::validate`）。
- **429/418 反馈**：经 `observe_http` 把限流元数据交给共享额度，公共 429/418 冻结整个共享消费者（与 quota crate 协作）。
- **退避**：失败按 2^failures（上限 63 移位）× backoff_ns，封顶 max_backoff_ns；永久错误直接 Stopped（出处：`client.rs::invalidate`）。
- 本 crate 不实现序列规则与盘口，桥接委托 `DepthSync` + `tickwire-book`；视图只在回调期间借用。

## 改动检查清单
```sh
cargo test -p tickwire-binance-public
cargo clippy -p tickwire-binance-public --all-targets -- -D warnings
cargo fmt -p tickwire-binance-public
```
改连接/重连跑 `network.rs`/`rebuild.rs`，改调度/定时跑 `scheduling.rs`/`timer.rs`/`control.rs`，改事件流跑 `events.rs`，改 SBE 跑 `sbe_config.rs`/`sbe_network.rs`。会话契约见 `binance-public-session.md`。
