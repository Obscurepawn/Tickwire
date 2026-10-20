# 系统单调时钟接口

`tickwire-clock-sys` 为校准器提供真实系统纳秒读数。Linux 使用 `clock_gettime(CLOCK_BOOTTIME)`；macOS 使用 `clock_gettime(CLOCK_MONOTONIC_RAW)`。这两个选择都计入系统挂起时间，进程恢复执行后，旧校准会按已经过去的时间检查年龄。它们的频率变化特性不同：Linux BOOTTIME 继承 MONOTONIC 的渐进调整；macOS RAW 为连续原始时钟，校准配置的变化率误差仍由部署环境确定。

## 所有权与接口

```rust
pub struct SystemClock { /* 固定域、上次成功读数、永久故障 */ }
pub struct SystemSample {
    pub instant: std::time::Instant,
    pub monotonic: tickwire_clock::MonoTime,
}

impl SystemClock {
    pub fn new(domain: tickwire_clock::ClockDomain) -> Result<Self>;
    pub fn sample(&mut self) -> Result<SystemSample>;
    pub fn domain(&self) -> tickwire_clock::ClockDomain;
}
```

控制面分配 `ClockDomain`，绑定本次机器启动、进程时钟代次及 Linux 时间命名空间。构造器固定接收的域并立即采样以检查平台支持；它不从墙钟生成域标识，不提供复制或更换域的方法。系统调用失败、读数回退、时间字段无效或算术溢出后，该实例永久拒绝采样。恢复时使用控制面分配的新域重新创建实例及校准器。

跨进程共享要求控制面确认使用同一个操作系统时钟和时间命名空间；进程存续期间不能切换时间命名空间。Linux BOOTTIME 的数值属于对应命名空间，仅凭相同的机器启动 ID 不能把任意两个进程读数相减。

每次采样先调用 `Instant::now()`，再读取系统纳秒时钟。两者不是同一原子快照：`Instant` 只供传输层执行辅助超时，校准区间、租约和年龄始终使用随后取得的 `MonoTime`。先取 `Instant` 可以避免两次采样间隔使辅助期限延后，但 `Instant` 的系统实现可能不计挂起时间，因此挂起后的辅助超时仍可能晚于 `MonoTime` 期限。每次恢复处理和首次输出都以 `MonoTime` 复核期限，不能依据 `Instant` 延续校准或租约。系统调用输出的秒与纳秒先校验符号和范围，再做 checked 乘加。

`tickwire-binance-clock` 为 `SystemClock` 实现现有 `MonotonicClock` trait；系统取时失败映射为永久时钟故障。校准核心仍是独立的纯 Rust crate，不引入 libc 或网络依赖。

## 验证

行为测试先覆盖不可用实现返回错误，随后运行实际操作系统读数、跨短暂停顿的单调推进、固定域、纳秒字段范围、转换溢出、回退和永久故障。macOS 和 Linux 分别实际执行平台分支。挂起计时依据所选系统 API 的契约；本轮单元与集成测试不触发机器挂起，也不把短暂停顿当作挂起测试。

依据：[Linux clock_gettime](https://www.man7.org/linux/man-pages/man2/clock_gettime.2.html)、[Linux time namespaces](https://www.man7.org/linux/man-pages/man7/time_namespaces.7.html)、[Apple 连续时间](https://developer.apple.com/documentation/kernel/1646199-mach_continuous_time)和 [Rust Instant](https://doc.rust-lang.org/std/time/struct.Instant.html)。Apple 将 `clock_gettime_nsec_np(CLOCK_MONOTONIC_RAW)` 列为连续时间的纳秒接口；这里使用同一 clock ID 的 `clock_gettime` 获取可检查错误的 `timespec`。
