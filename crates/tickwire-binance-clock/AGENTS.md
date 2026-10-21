# AGENTS.md — tickwire-binance-clock

## 职责
Binance 校准时钟适配：每个采样样本使用一条独立的非阻塞 TLS HTTPS 连接请求 `/api/v3/time`（Spot）或 `/fapi/v1/time`（USDM），把交易所服务器时间喂给核心 `tickwire-clock` 校准器，并在发送前对原始 `SignedTime` 做最终窗口/域检查。它为私有会话、执行编码、公共行情等所有需要"交易所时间戳"的组件提供同一个受信 `BinanceTimeClient`，自身不持有业务授权。

## 公共 API 入口
- `BinanceTimeClient<C: MonotonicClock>`（`client.rs`）：`new(config, network, clock)`、`start(permit) -> connection_epoch`、`poll(permit, budget) -> PollReport`、`cancel()`、`sample() -> ClockReading`、`check_send(stamp, recv_window_ns, budget_ns)` / `check_send_sample(stamp, window_ns, budget_ns) -> LocalTime`、`status() -> CalibrationStatus`、`in_flight() -> Option<SampleProgress>`、`register/reregister(registry, token)`、`source()`、`domain()`。
- 配置：`Config { source, domain, calibration, unit, first_sample_serial, first_connection_epoch, connect_timeout_ns, request_timeout_ns, max_response_bytes, max_headers, io }`、`NetworkConfig { address, tls }`。
- 本地时间：`LocalTime { instant, monotonic }`、`ClockReading { local, signed: Option<SignedTime> }`、trait `MonotonicClock`（为 `SystemClock` 实现）。
- 额度：`ChargeKind::{Connect, Query}`、`TimeCharge { source, domain, connection_epoch, kind, api_weight }`、trait `TimePermit::allow(charge, now)`（闭包自动实现）。
- 结果：`Outcome { connection_epoch, http: Option<ResponseMetadata>, calibration: Result<Reading> }`、`PollReport`、`SampleProgress`；`Error::{Invalid, Busy, Denied, Exhausted, Clock, Transport, Protocol, HttpStatus(u16), Timeout, Closed, Budget, Unsupported}`。
- 再导出 `tickwire_clock::{CalibrationConfig, CalibrationStatus, ClockDomain, MonoTime, Reading, SignedTime, Source, TimeUnit}`、`tickwire_clock_sys::SystemClock`、`tickwire_transport::{IoBudget, Limits, mio, rustls}`。

无 binary、无 example。

## 在 workspace 中的位置
- 上游依赖（`[dependencies]` 中的 tickwire-*）：`tickwire-clock`、`tickwire-clock-sys`、`tickwire-transport`；外部 `serde`、`serde_json`。
- 下游（已在各 `Cargo.toml` 核对）：`tickwire-app`、`tickwire-binance-execution`、`tickwire-binance-public`、`tickwire-binance-quota`、`tickwire-binance-session`、`tickwire-guardian`、`tickwire-md`、`tickwire-runtime-policy`、`tickwire-runtime`、`tickwire-strategy-host`、`tickwire-strategy-sdk`。

## 对应规格
无专门实现文档（校准核心概念见 ../../specs/implementation/clock.md，本 crate 只做 Binance 端点适配）。

## 构建与测试
```sh
cargo test -p tickwire-binance-clock
cargo build -p tickwire-binance-clock
cargo clippy -p tickwire-binance-clock --all-targets -- -D warnings
cargo fmt -p tickwire-binance-clock
```
- `tests/https.rs`：16 个用例，全部用本地 rustls `TcpListener` mock 服务（证书名模拟 api.binance.com / testnet.binance.vision / fapi.binance.com / demo-fapi.binance.com），覆盖四产品/环境的 host 与空 `GET /time` 路径、采样前后额度计费、分片响应、重复/错类型/截断时间拒绝、非成功状态保留限流元数据、未完成应用接受不校准、取消消耗连接代次且下次用新 socket、旧 stamp 发送前复检、连接权限与代次不复用。
- `tests/system_clock.rs`：`#![cfg(any(target_os = "linux", target_os = "macos"))]`，只验证原生 `SystemClock` 单调/域契约；在 Windows 上自动跳过，其余测试全平台可跑。

## 关键不变量与注意事项
- **一采样一连接**：每次校准样本独占一条 TLS 连接，`Connection: close`；取消/失败消耗该 connection epoch，绝不复用（出处：`client.rs`、`tests/https.rs::cancellation_consumes_connection_epoch_and_next_sample_uses_a_fresh_socket`）。
- **端点固定**：Spot Production `api.binance.com/api/v3/time`、Spot Test `testnet.binance.vision/api/v3/time`、USDM Production `fapi.binance.com/fapi/v1/time`、USDM Test `demo-fapi.binance.com/fapi/v1/time`；Query 计费权重 1、Connect 0（出处：`client.rs`、`tests/https.rs::each_product_and_environment_uses_its_authenticated_host_and_empty_time_get`）。
- **校准只接受完整应用接受后的回复**：分片未完成、非 2xx、重复键、错误类型零值、截断 JSON 都不更新校准区间（出处：`response` 校验与 `a_reply_before_complete_application_acceptance_cannot_calibrate`）。
- **发送前最终检查**：`check_send_sample` 在最终门内再次采样并复检原始 stamp 的 source/domain/窗口/预算；旧时间戳超窗、域变化即拒绝（出处：`client.rs`、`clock_owner_rechecks_fresh_time_before_sending_an_old_signed_stamp`）。
- **provider 故障吊销不可被后续读取清除**：一次 provider fault 使状态 faulted，后续成功读取不能自行恢复（出处：`a_provider_clock_fault_revokes_status_and_cannot_be_cleared_by_a_later_read`）。
- 本 crate 不签名、不持有密钥；`SignedTime` 由核心校准器产出，本层只校验。

## 改动检查清单
```sh
cargo test -p tickwire-binance-clock
cargo clippy -p tickwire-binance-clock --all-targets -- -D warnings
cargo fmt -p tickwire-binance-clock
```
改端点/计费/校准门后重跑 `tests/https.rs` 全部用例；改原生 provider 再跑 `tests/system_clock.rs`（仅 Linux/macOS）。
