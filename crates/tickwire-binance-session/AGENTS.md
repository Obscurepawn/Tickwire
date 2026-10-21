# AGENTS.md — tickwire-binance-session

## 职责
私有流的校准时钟适配器（仅约 120 行，`src/lib.rs` 单文件）：借用同一个 `BinanceTimeClient`，为 `tickwire-binance-private` 的 `PrivateClient` 实现 `Clock` trait，在签名检查时比对原始 `SignedTime` 的 scope/source/domain 并把剩余交付预算交给核心校准器做最终发送检查。注意：完整私有会话生命周期（logon/subscribe/listenKey/心跳/轮换）不在本 crate，而在 `tickwire-binance-private`；本 crate 只是时钟适配层。

## 公共 API 入口
- `ClockAdapter<'a, C: MonotonicClock, B: DeliveryBudget>`：`new(time: &'a mut BinanceTimeClient<C>, scope: AccountScope, budget: &'a mut B) -> Result<Self>`；实现 `tickwire_binance_private::Clock`（`sample()` / `check_signed(scope, stamp, now, window_us)`）。
- `DeliveryBudget` trait：`remaining_ns(scope, stamp, now) -> Result<u64>`（从最终门到远端时间戳校验的上界纳秒），闭包自动实现。
- 再导出 `tickwire_binance_private::{AccountScope, Clock, ClockSample, Error, Result, SignedTime}`（`Environment`、`Market` 内部使用）。

无 binary、无 example、无子模块。

## 在 workspace 中的位置
- 上游依赖（`[dependencies]` 中的 tickwire-*）：`tickwire-binance-clock`、`tickwire-binance-private`、`tickwire-clock`（经 private/clock 传递使用）。
- 下游（已在各 `Cargo.toml` 核对）：`tickwire-app`、`tickwire-runtime`（由运行期把适配器接进私有会话 poll 循环）。

## 对应规格
- ../../specs/implementation/binance-private-clock-adapter.md

## 构建与测试
```sh
cargo test -p tickwire-binance-session
cargo build -p tickwire-binance-session
cargo clippy -p tickwire-binance-session --all-targets -- -D warnings
cargo fmt -p tickwire-binance-session
```
`tests/integration.rs` 通过 `#[path]` 复用 `tickwire-binance-private/tests/common/mod.rs` 的本地 mock TLS 服务，覆盖：真实 HTTPS 校准驱动真实签名 WSS 订阅、适配器保留精确校准样本、构造拒绝产品/环境/无效账户、原始 scope 与窗口换算检查、预算回调不能在最后样本后使校准过期、真实时钟 client 的域 fault 归类为永久 `ClockDomain`、未校准 client 不提供签名授权、新样本不为原始 stamp 重签、超大预算/回调域变化拒绝、原始 stamp 不能跨产品或时钟域、mono 过期在首次/二次预算检查时接受前拒绝。无 OS 条件编译。

## 关键不变量与注意事项
- **构造即绑定 scope→Source**：`(market, environment)` 映射 `SpotProduction/SpotTest/UsdmProduction/UsdmTest`，与 `time.source()` 不一致即 `Identity`；同时缓存 `time.domain()`（出处：`ClockAdapter::new`）。
- **sample 二次校验域与 source**：本地 mono 域、signed stamp 的 domain/source 都必须与适配器缓存一致，否则 `ClockDomain`/`Identity`（出处：`sample`）。
- **check_signed 不重签**：先比对完整 scope（账户/risk domain/凭据代次）与 stamp.source()，再转窗口（微秒×1000 纳秒，checked，零值拒绝 `Bounds`），随后重新 `sample()` 确认时间不倒退，取预算后委托 `time.check_send`；原始 stamp 保持不变（出处：`check_signed`、测试 `new_sample_does_not_resign_the_original_stamp`）。
- **错误分类**：client 的 Domain/TimeBackwards/Faulted/ClockJump/Overflow/Exhausted 映射为 `ClockDomain`（永久类），其余为 `Clock`（出处：`clock_error`）。
- **不持有校准区间或发送队列**：全部借用 `BinanceTimeClient` 与 `DeliveryBudget`，禁止在本层另存一份校准状态（出处：规格 binance-private-clock-adapter.md）。

## 改动检查清单
```sh
cargo test -p tickwire-binance-session
cargo clippy -p tickwire-binance-session --all-targets -- -D warnings
cargo fmt -p tickwire-binance-session
```
改 scope/source/domain 映射或预算顺序后，重跑 `tests/integration.rs` 全部用例；本层改动必须与 `tickwire-binance-private` 的 `Clock` trait 签名保持一致。
