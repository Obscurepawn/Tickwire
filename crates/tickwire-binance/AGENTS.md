# AGENTS.md — tickwire-binance

## 职责
Binance 协议层基础 crate：签名与密钥、请求参数组装与预映像、精确十进制、REST/WS 响应分类、公共深度/事件与私有事件的有界 JSON/SBE 解码，以及把增量流与快照桥接到 `tickwire-book` 的 `DepthSync` 协调器。它不持有网络连接，被公共 owner、私有会话、配额、执行与账户等所有 Binance 接入 crate 复用，是整条 Binance 链路的协议事实来源。

## 公共 API 入口
`src/lib.rs` 聚合以下 pub 模块：

- `auth`：`Algorithm::{HmacSha256, RsaSha256, Ed25519}`、`Endpoint::{SpotRest, SpotWebSocket, UsdmRest, UsdmWebSocket, SpotSessionLogon, UsdmSessionLogon, SpotFixLogon}` 及 `Endpoint::permits(algorithm)`、`SigningKey::{hmac, ed25519_pkcs8, rsa_pkcs8}`（无 Debug/Clone/导出）、`sign(endpoint, preimage, out)`、`fix_logon_preimage(...)`；常量 `MAX_SIGNING_INPUT = 64 KiB`、`MAX_SIGNATURE = 1368`。
- `request`：`Market::{Spot, Usdm}`、`Transport::{Rest, WebSocket}`、`Method::{Get, Post, Put, Delete}`、`Timestamp::{Milliseconds, Microseconds}`、`Credentials<'a>`、`RequestOptions<'a>`、`prepare(command, credentials, options, max_bytes) -> PreparedRequest`、`PreparedRequest`（`authority/path/method/transport/payload/api_key_header/verify_digest`）；`MAX_REQUEST_BYTES = 16384`。
- `params`：`Params<'a, N>`（`push/rest/websocket_preimage/json`）、`Value<'a>`、`rest_preimage(query, body, out)`；`MAX_VALUE_BYTES = 4096`。
- `decimal`：`Decimal`（`parse/new/mantissa/scale`，scale ≤ 38，文本 ≤ 80 字符，checked arithmetic）。
- `response`：`FailureKind::{Unknown, Rejected, NotFound, RateLimited, Banned, Authentication, ClockSkew, Partial}`、`classify(market, status, code, message)`、`Response<'a>`、`decode_rest/decode_websocket/decode_order/decode_algo`。
- `feed`：`TimeUnit::{Milliseconds, Microseconds}`、`DepthContext::new/reserved_bytes`、`decode_snapshot::<N>`、`decode_diff::<N>`、`decode_sbe_diff::<N>`；`DepthWorkspace::new/capacity/reserved_bytes` 与 `decode_diff_into/decode_sbe_diff_into` 返回借用 `DepthUpdateRef`，JSON/SBE 增量均可复用冷预留存储；`MAX_MARKET_MESSAGE_BYTES = 16 MiB`、`MAX_DEPTH_LEVELS = 5000`。
- `market`：`EventKind::{Trade, BestBidAsk, MarkFunding}`、`EventEncoding::{Json, SpotSbeV1}`、`EventContext`（`validate/stream_name`）、`Event`/`EventValue`/`Trade`/`BestBidAsk`/`MarkFunding`、`decode_json`、`decode_sbe`。
- `events`：`SessionIdentity`、`PrivateStream::{SpotSubscription{..}, UsdmListenKey}`、`SessionContext::new`、`PrivateEvent`、`PrivateBody`、`decode_private::<N>`、`bind_order`、`bind_structure`、`bind_structure_response`。
- `depth_sync`：`DepthSync<P, LEVELS, UPDATES, BUFFER>`（`new/connect/subscription_ready/request_snapshot/on_delta/on_sbe_delta/on_snapshot/snapshot_failed/expire_snapshot/disconnect/state/buffered/generation/last_update_id/view`）、`Config`、`ConnectionToken`、`SnapshotToken`/`SnapshotRequest`、`State::{Disconnected, Buffering, Bridging, Live, Exhausted}`、`Outcome`、`SyncError`、`RetryReason`、`RebuildReason`。

无 binary、无 example。

## 在 workspace 中的位置
- 上游依赖（`[dependencies]` 中的 tickwire-*）：`tickwire-venue`、`tickwire-book`、`tickwire-binance-sbe`；外部：`ring`、`base64`、`serde`、`serde_json`。
- 下游（已在各 `Cargo.toml` 核对）：`tickwire-account-gateway`、`tickwire-app`、`tickwire-binance-account`、`tickwire-binance-execution`、`tickwire-binance-private`、`tickwire-binance-public`、`tickwire-binance-quota`、`tickwire-gateway`、`tickwire-guardian`、`tickwire-market-publisher`、`tickwire-md`、`tickwire-runtime`。

## 对应规格
- ../../specs/implementation/binance-protocol.md
- ../../specs/implementation/binance-streams.md
- ../../specs/implementation/binance-depth-sync.md
- ../../specs/implementation/binance-market-events.md

## 构建与测试
```sh
cargo test -p tickwire-binance
cargo build -p tickwire-binance
cargo clippy -p tickwire-binance --all-targets -- -D warnings
cargo fmt -p tickwire-binance
```
`tests/` 全部使用本地 mock TLS 服务（`tests/common/`、`tests/data/`），不访问真实网络：`protocol_primitives.rs`、`requests.rs`、`responses.rs`、`request_extensions.rs`、`feed.rs`、`sbe_depth.rs`、`depth_sync.rs`、`depth_sync_server.rs`、`market_events.rs`、`market_server.rs`、`private_events.rs`、`private_server.rs`、`private_structures.rs`、`protocol_server.rs`。无 OS 条件编译，全平台可跑。

## 关键不变量与注意事项
- **签名算法-端点矩阵**：Spot REST/WS 三种算法均可；USDM REST 仅 HmacSha256/RsaSha256；USDM WS 仅 HmacSha256/Ed25519；三类 SessionLogon/FixLogon 仅 Ed25519；不匹配返回 `UnsupportedAlgorithm`（出处：`auth.rs::Endpoint::permits`，官方 HMAC/FIX Ed25519 向量与独立 OpenSSL RSA 向量见 `binance-protocol.md`）。
- **密钥材料保密**：`SigningKey` 无 Debug/Clone/导出；`Authentication` 类包装 Debug 输出 `[REDACTED]`（本 crate 的 `SigningKey` 同理，出处：`auth.rs`）。
- **prepare 身份校验**：`venue == 1`、account/environment/market 与 credentials 一致；api_key 仅允许字母数字与 `-_`（≤256）；request_id 可见 ASCII 33–126（≤64）；recvWindow 1–60_000_000 µs，USDM 必须是 1000 的倍数；时间戳 Spot 可毫秒/微秒，USDM 仅毫秒（出处：`request.rs::prepare`）。
- **响应分类保守**：未识别的新服务端错误码一律 `Unknown`（不证明执行失败）；418=Banned、429/-1003/-1015=RateLimited、409=Partial、-2011/-2013=NotFound、-1021=ClockSkew（出处：`response.rs::classify`）。
- **深度桥接规则**：`DepthSync` 在 Buffering 期缓存增量（容量 `BUFFER`，溢出 Rebuild）；快照到达后按 Spot `last_update_id >= first_u`、USDM previous 链回放缓存；过期/过旧快照 RetrySnapshot，解码/盘口/时钟失败 Rebuild；连接 token 不匹配返回 `IgnoredObsolete`（出处：`depth_sync.rs`、`binance-depth-sync.md`）。
- **身份绑定**：`DepthContext::new` 校验 venue/source/外部 instrument/catalog 非零、Spot↔Cash、USDM↔Perpetual/Delivery、USDM 时间单位必须毫秒；BookKey.instrument 是可从零开始的本地槽，不要求与完整 u64 外部 ID 相等，不得截断外部 ID。装配 owner 保证完整 BookKey 唯一绑定；SBE diff 仅允许 Spot + 微秒（出处：`feed.rs`、`feed/sbe.rs`）。
- **可复用深度解码**：workspace 按双侧总档数限容，只有构造分配；返回结果借用工作区，失败不交付部分事件。JSON 新旧 diff 共用 `feed/json.rs` 的结构扫描与定点转换，合法转义键/字符串及未知字段不分配，嵌套容器最多 128 层；旧 snapshot 保留既有 serde 解码。`tests/depth_workspace.rs` 通过安全 `allocation-counter` 验证初次/连续/转义/最大 4096 档解码零分配、2 MiB 栈、错误后复用和独立 serde 参考解析器差分。错误路径无零分配承诺。
- **私有消息稀疏语义**：账户消息是稀疏增量，从不建立完整基线；变长数组先校验长度再物化下一项；重复 JSON 对象键拒绝（出处：`events.rs`、`binance-streams.md`）。
- 本 crate 不含网络 IO、时钟校准与配额；这些分别在 transport/clock/quota crate。

## 改动检查清单
```sh
cargo test -p tickwire-binance
cargo clippy -p tickwire-binance --all-targets -- -D warnings
cargo fmt -p tickwire-binance
```
改签名/参数编码后跑 `tests/requests.rs`、`protocol_primitives.rs`、`request_extensions.rs`；改解码跑 `feed.rs`/`sbe_depth.rs`/`private_events.rs`/`market_events.rs`；改桥接跑 `depth_sync.rs`/`depth_sync_server.rs`。协议契约见 `binance-protocol.md`，深度桥接契约见 `binance-depth-sync.md`。
