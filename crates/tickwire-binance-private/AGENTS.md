# AGENTS.md — tickwire-binance-private

## 职责
已认证、有界的私有流会话 owner，运行在真实非阻塞 socket 上：Spot 走 `/ws-api/v3` 的 `session.logon`（Ed25519）+ `userDataStream.subscribe(.signature)` JSON-over-WS；USDM 走 REST `POST/PUT/DELETE /fapi/v1/listenKey` 管理密钥 + `fstream` 订阅。负责连接/认证/订阅状态机、listenKey 生命周期（创建/保活/轮换/关闭）、心跳、退避重连、断档 Gap 通知，并把私有事件交给 sink。它是账户/执行/配额等上层共享的私有网络会话层。

## 公共 API 入口
- `PrivateClient`（`client.rs`）：`new(config: SessionConfig, auth: Authentication, network: NetworkConfig, first_connection_epoch)`、`poll(clock: &mut impl Clock, budget: PollBudget, permit: &mut impl ControlPermit, sink: impl FnMut(Delivery) -> Result<()>) -> PollReport`、`register/reregister(registry, RegistrationTokens)`、`status() -> SessionStatus`、`current_gap() -> &GapToken`、`next_deadline()`、`request_status()`（仅 Spot）、`stop(StopMode)`。
- 认证：`Authentication::{SpotLogon{api_key, signing_key}, SpotSignature{api_key, signing_key}, UsdmApiKey{api_key}}`（Debug 输出 `[REDACTED]`）。
- 身份/时钟：`AccountScope { risk_domain_id, venue, environment, account, market, credential_generation }`（`validate()`，`venue == 1`）、`ClockSample { instant, monotonic, signing_time }`、trait `Clock { sample(); check_signed(scope, stamp, now, recv_window_micros) }`、`SignedTime`/`ClockDomain`/`MonoTime` 再导出。
- 控制操作：`Operation::{SpotLogon, SpotSubscribe, SpotSubscribeSignature, SpotStatus, SpotSubscriptions, SpotUnsubscribe{subscription_id}, SpotLogout, UsdmCreate, UsdmKeepalive, UsdmClose}`、`RequestToken::new(connection_epoch, serial, operation)`。
- 会话配置（`session.rs`）：`SessionConfig { scope, timing: Timing, limits: SessionLimits }`（`SessionConfig::new(scope)`，USDM 默认心跳 600s）、`Timing`、`SessionLimits`、`NetworkConfig { stream_address, management_address, tls }`、`Phase`、`SessionStatus`、`GapToken`/`GapNotice`、`Charge`/`ChargeKind`、trait `ControlPermit`、`PollBudget`、`ControlState::{NoBytes, MayHaveApplied, Confirmed, Rejected}`、`ControlOutcome`、`Delivery::{State, Gap, Event, Control}`、`StopMode::{Disconnect, CloseRemoteStream}`、`RegistrationTokens`、`PollReport`。
- `request` 模块：`prepare(scope, auth, token, now, window_micros, max) -> PreparedControlRequest`、`validate_auth`、`clock_source`、`check_clock`；`MAX_REQUEST_BYTES = 16384`。
- 事件：`PrivateEvent<'a>`（`session/body/raw/event_time_ns/transaction_time_ns/received_mono_ns`，Debug 脱敏）、`bind_order`、`bind_structure`。

无 binary、无 example。

## 在 workspace 中的位置
- 上游依赖（`[dependencies]` 中的 tickwire-*）：`tickwire-clock`、`tickwire-binance`、`tickwire-transport`、`tickwire-venue`；外部 `serde`、`serde_json`、`sha2`。
- 下游（已在各 `Cargo.toml` 核对）：`tickwire-app`、`tickwire-binance-account`、`tickwire-binance-execution`、`tickwire-binance-quota`、`tickwire-binance-session`、`tickwire-runtime-policy`、`tickwire-runtime`。

## 对应规格
- ../../specs/implementation/binance-private-session.md

## 构建与测试
```sh
cargo test -p tickwire-binance-private
cargo build -p tickwire-binance-private
cargo clippy -p tickwire-binance-private --all-targets -- -D warnings
cargo fmt -p tickwire-binance-private
```
`tests/`（`common/` 提供本地 mock TLS 服务，不访问真实网络，无 OS 条件编译）：`spot_lifecycle.rs`、`usdm_lifecycle.rs`、`clock_binding.rs`、`requests.rs`、`failures.rs`、`protocol_server.rs`。

## 关键不变量与注意事项
- **认证方式与操作绑定**：`SpotLogon` 仅配 `SpotLogon` 认证（Ed25519）；`SpotSubscribeSignature` 仅配 `SpotSignature`（HMAC）；USDM 管理请求只带 `X-MBX-APIKEY`、不签名；错配返回 `Unsupported`/`Identity`（出处：`request.rs::prepare`、`validate_auth`）。
- **listenKey 生命周期**：`KEY_LIFETIME = 3600s`；默认 renew 1800s、rotate 82800s（23h）、Spot 心跳 60s / USDM 600s、退避 100ms 起、上限 30s；构造校验 `renew_ns < KEY_LIFETIME` 且 `request_ns < KEY_LIFETIME - renew_ns`（出处：`client.rs`、`session.rs::Timing::default`）。
- **时钟检查**：控制请求预映像绑定 `tickwire.private-control.v1\0` + scope/凭据代次/连接代次/serial/authority/path/method/payload/apiKey 的长度前缀 SHA-256；发送前 `check_clock` 校验 domain 一致、fresh、stamp.source == clock_source(scope)（出处：`request.rs`）。
- **重复 JSON 键拒绝**：私有响应解析器（`response.rs::Strict`）拒绝嵌套对象中的重复键。
- **事件脱敏**：`PrivateEvent` 的 Debug 不输出原文；原文只通过 `raw()` 显式取用。
- **ClockDomain 故障即停止**：poll 中遇到 `ClockDomain` 错误进入 stopping 并 fail，不自行恢复（出处：`client.rs::poll`）。
- 本 crate 不做订单记账、不持有持久化；事件绑定（`bind_order`/`bind_structure`）只产出规范事件，落账由 `tickwire-binance-account` 负责。

## 改动检查清单
```sh
cargo test -p tickwire-binance-private
cargo clippy -p tickwire-binance-private --all-targets -- -D warnings
cargo fmt -p tickwire-binance-private
```
改 Spot 生命周期跑 `spot_lifecycle.rs`，改 USDM listenKey 跑 `usdm_lifecycle.rs`，改时钟绑定跑 `clock_binding.rs`，改请求编码跑 `requests.rs`，失败/重连路径跑 `failures.rs`。会话契约见 `binance-private-session.md`。
