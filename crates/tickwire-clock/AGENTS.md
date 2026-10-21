# tickwire-clock 接手说明

## 1. 职责（架构位置：时钟校准核心）

把交易所时间区间映射到单一挂起感知单调时钟域的校准核心。**无网络、不读 OS 时钟**；输入 `MonoTime` 由外部提供者（`tickwire-clock-sys` 或测试）给出包含挂起时间的单调纳秒。`Calibrator` 由单 owner 串行驱动：发样本令牌（begin）→ 收服务器时间（finish）→ 投影锚点（read）→ 签名时间（sign_time）→ 发送前复核（check_send）。

架构位置：时钟域抽象层，向上被网络适配器、配额、风控门禁、fencing、生命周期协调器等共享；本身不绑定任何交易所协议。

## 2. 公共 API 入口

- 常量：`PPB=1e9`、`SECOND/MILLISECOND/MICROSECOND`。
- 类型：`Error`（Invalid/Busy/Stale/RoundTrip/Uncertain/ClockJump/Expired/Uncalibrated/Future/TooOld/Domain/TimeBackwards/Overflow/Faulted 等）、`Source`（SpotProduction/SpotTest/UsdmProduction/UsdmTest）、`TimeUnit`（Milliseconds/Microseconds）。
- 域与时间：`ClockDomain`（boot_id+generation，`new` 拒绝全零/0）、`MonoTime`（domain+ns，`nanos()`/`domain()`）。
- 校准：`CalibrationConfig`（max_round_trip_ns/max_width_ns/max_age_ns/quantization_error_ns/drift_ppb/max_adjustment_ns）、`SampleToken`（serial）、`Reading`（lower_ns/upper_ns/sampled_at_ns/valid_until_ns/sample_serial）、`SignedTime`（domain/source/timestamp/unit/sample_serial）、`CalibrationStatus`（Uncalibrated/Calibrated{sample_serial,valid_until_ns}/Faulted(Error)）。
- `Calibrator`：`new`、`begin`（发令牌）、`finish`（收回复出 Reading）、`cancel`、`read`（投影当前区间）、`sign_time`（中点量化签名）、`check_send`（发送窗口复核）、`status`。

## 3. workspace 位置

- 上游依赖：**无**（纯计算，无第三方、无 workspace 依赖）。
- 下游（grep `Cargo.toml` 核对，共 20 处）：`tickwire-app`、`tickwire-binance-account`、`tickwire-binance-clock`、`tickwire-binance-execution`、`tickwire-binance-private`、`tickwire-binance-quota`、`tickwire-binance-session`、`tickwire-clock-sys`、`tickwire-fencing`、`tickwire-guardian`、`tickwire-lab`、`tickwire-lifecycle`、`tickwire-market-source`、`tickwire-md`、`tickwire-quota`、`tickwire-replica`、`tickwire-runtime`、`tickwire-runtime-policy`、`tickwire-strategy-host`、`tickwire-strategy-sdk`。

## 4. 对应规格

- [../../specs/implementation/clock.md](../../specs/implementation/clock.md) — 时钟校准核心规格（状态、样本序号、区间投影、发送复核）。

## 5. 构建与测试

```sh
cargo test -p tickwire-clock
```

- `tests/calibration.rs`：25 个测试（非对称往返保留整个不确定区间、漂移双向向外舍入、误差随龄增长并在宽度上限停止、过期排他且发送预算须先于到期、传输预算用服务器费率上界、recv_window 等值允许而少一微秒拒绝、新校准复核精确旧时间戳等）。

## 6. 关键不变量与注意事项

- **单 owner 串行**：所有变更由同一 owner 调用；`observe` 中 domain 不符或 `now.ns` 回退立即 `fail()`，进入永久 `Faulted`，后续调用只返回 `Faulted`，不得清除。
- **begin/finish 配对**：pending 令牌唯一，重复 begin 返回 `Busy`；陈旧/错配 token finish 返回 `Stale`（"An obsolete reply cannot change clock state, even with malformed time"）；`cancel` 只认当前 pending。
- **往返与宽度**：`finish` 计算 round_trip = received - sent，超 `max_round_trip_ns` 返回 `RoundTrip`；候选区间宽（upper-lower）超 `max_width_ns` 返回 `Uncertain`。
- **漂移双向外扩投影**：`project` 按龄推进，下界用 `(PPB-drift_ppb)` 慢推进、上界用 `scale_up(age, drift_ppb)` 快推进，绝不向中间收敛；新锚点与仍有效的旧锚点 gap 超 `max_adjustment_ns` 判 `ClockJump` 并 fault。
- **read 三态**：无锚点 `Uncalibrated`；`now >= valid_until_ns` `Expired`；投影后超宽 `Uncertain`。
- **sign_time**：取区间中点按 unit 量化；Usdm 源拒绝 Microseconds（精度约束）。
- **check_send**：先核对 domain/source；recv_window 必须是该源精度倍数且 ≤60s；发送预算末端必须早于 `valid_until_ns`；签名时间戳不得晚于 `lower + 1s`（`Future`）；`upper + 预算上界` 必须仍大于 `timestamp + recv_window`，否则 `TooOld`（保留 T>U 的有符号数学比较）。
- **样本序号不复用**：serial 单调递增，最后一个 u64 用完 `Overflow`，`next_serial` 置 None。
- 本 crate 不获取 wall clock，时刻真实性由 `MonoTime` 提供者保证。

## 7. 改动检查清单

1. `cargo test -p tickwire-clock` 全绿。
2. 改投影/漂移/宽度逻辑时，对照 clock.md 的状态表与 calibration.rs 既有边界值。
3. 新增 Source/TimeUnit 时同步 `check_send` 精度规则与 `sign_time` 拒绝规则。
4. 保持无网络、无 OS 读取；故障（Faulted）永久化语义不得改为可恢复。
5. 联动检查 `tickwire-clock-sys` 的单调采样与各适配器对 `Expired/TooOld/Future` 的处理。
