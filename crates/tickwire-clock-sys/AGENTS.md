# tickwire-clock-sys 接手说明

## 1. 职责（架构位置：系统单调时钟 OS 边界）

绑定到单一外部分配 `ClockDomain` 的挂起感知原生单调时间 `SystemClock`：一次 OS 读取封装为 `SystemSample { instant: Instant, monotonic: MonoTime }`。这是全工作区少数允许 `unsafe` 的审计边界（`#![deny(unsafe_op_in_unsafe_fn)]`），把 `clock_gettime` 收敛到一处，其余 crate 保持 `forbid(unsafe_code)`。

架构位置：`tickwire-clock` 校准核心的 OS 实现端，为校准网络 owner 提供单调纳秒样本；`tickwire-binance-clock`、生命周期、运行时门禁、策略宿主/SDK 经它取得本地时钟。

## 2. 公共 API 入口

- `SystemClock`：`new(domain)`（构造即先完整 sample 一次，失败不返回实例）、`domain()`、`sample() -> Result<SystemSample>`。
- `SystemSample`：`instant: std::time::Instant`、`monotonic: MonoTime`（绑定构造时的 domain）。
- `Error`：`Unsupported`、`InvalidTime`、`Overflow`、`Backwards`、`Faulted`、`System(io::Error)`；`Result<T>`。
- 内部 `decode_timespec(seconds, nanos)`（仅测试可见的解析校验）。

## 3. workspace 位置

- 上游依赖：`tickwire-clock`（`ClockDomain`/`MonoTime`）、libc 0.2。
- 下游（grep `Cargo.toml` 核对）：`tickwire-binance-clock`、`tickwire-lab`、`tickwire-lifecycle`、`tickwire-md`、`tickwire-runtime-policy`、`tickwire-strategy-host`、`tickwire-strategy-sdk`。

## 4. 对应规格

- [../../specs/implementation/system-clock.md](../../specs/implementation/system-clock.md) — 系统单调时钟实现规格（构造即采样、域不变、故障不可清除）。

## 5. 构建与测试

```sh
cargo test -p tickwire-clock-sys
```

- 模块内 5 个测试：timespec 纳秒保留与精确上限、非法字段绝不转无符号、同 tick 合法而单调回退永久 fault、Instant 回退即使原生时间前进也永久 fault、原生错误不被后续成功清除。
- `tests/native.rs`：2 个原生采样测试。

## 6. 关键不变量与注意事项

- **构造即采样**：`new(domain)` 内部先执行一次完整 `sample()`，只有成功才返回实例；域在实例存续期不变。
- **无 Clone、无换域、无故障清除**：实例不提供 clone/re-bind/reset 接口；一旦 `Guard` fault，后续 `sample` 一律 `Faulted`，原生调用成功也不能清除（测试 "native_error_cannot_be_cleared_by_a_later_success"）。
- **双轨回退检测**：`Guard::observe` 同时比较 `Instant` 与原生纳秒；任一倒退即 `Backwards` 并永久 fault；完全相同的同 tick 重复合法。
- **挂起感知时钟**：Linux 用 `CLOCK_BOOTTIME`（含挂起时长），macOS 用 `CLOCK_MONOTONIC_RAW`；其他目标编译为 `Unsupported`，不得回退到 wall clock。
- **timespec 严格解码**：负秒、负纳秒、纳秒 ≥1e9、i128/u64 溢出分别 `InvalidTime`/`Overflow`，禁止直接把有符号强转无符号。
- **unsafe 边界最小化**：唯一 unsafe 块是 `clock_gettime` 调用（timespec 已对齐初始化、指针不被保留）；新增原生调用必须维持 `deny(unsafe_op_in_unsafe_fn)` 并在注释写明 SAFETY。
- 本 crate 不做校准、不读交易所时间；校准逻辑全部在 `tickwire-clock`。

## 7. 改动检查清单

1. `cargo test -p tickwire-clock-sys` 全绿（Linux/macOS 分别验证对应 clock id）。
2. 新增原生系统调用时，保持单一边界、SAFETY 注释完整，并确认不破坏永久 fault 语义。
3. 改 clock id 时同步 system-clock.md 与 `tickwire-binance-clock` 的 `MonotonicClock` 适配。
4. 不要在此层引入 wall clock、校准或网络；那些属于上层。
5. 跨平台新增目标必须给出明确的 Unsupported 或等效挂起感知时钟，禁止静默退化。
