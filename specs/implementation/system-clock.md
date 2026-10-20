# 系统单调时钟实现规格

实现为 `tickwire-clock-sys::SystemClock`，接口见[系统单调时钟接口](../../docs/implementation/system-clock-interface.md)。它把一次 OS 读取绑定到控制面分配的 `ClockDomain`，并为校准网络 owner 实现 `MonotonicClock`。

## 构造与采样

`new(domain)` 先在局部对象上执行一次完整 `sample`；只有成功才返回实例。域在实例存续期间保持不变。实例没有 Clone、换域或故障清除接口。

`sample` 按以下顺序执行：

1. 已记录故障时直接返回 `Faulted`，不再次读取 OS。
2. 读取 `Instant::now()`，再调用平台单调时钟。
3. 系统调用或转换失败时记录永久故障，返回原错误。
4. 任一读数小于上次成功值时记录永久故障，返回 `Backwards`。
5. 保存本次两种读数，返回固定域的 `MonoTime`；相等读数允许成功。

Linux 选用 `CLOCK_BOOTTIME`，macOS 选用 `CLOCK_MONOTONIC_RAW`；其他平台返回 `Unsupported`。平台输出先校验秒非负、纳秒位于 `0..1_000_000_000`，再以 checked 乘加转换为 u64 纳秒。受控 unsafe 仅用于写入已初始化、对齐的 `timespec`；系统调用期间该对象可写，调用结束后无外部指针保留。

Instant 与系统纳秒读数分别读取。校准年龄和租约使用 `MonoTime`；Instant 是传输超时的辅助时钟。所选平台时钟对系统挂起的处理及部署侧域分配约束以接口中的 OS 合同为准。

## TimeClient 故障传播

原生 provider 把 `Backwards` 映射为核心 `TimeBackwards`、`Overflow` 映射为核心 `Overflow`，其余错误映射为核心 `Faulted`。

`BinanceTimeClient::local` 首先检查已有 `fatal`。调用 provider 后，遇到 `Domain / TimeBackwards / Overflow / ClockJump / Faulted` 任一核心时钟错误就保存原错误，后续调用直接拒绝，`status` 返回该故障。其它 provider 错误不设置这个永久标记。

provider 成功后，TimeClient 还检查 Instant 回退，再更新 `last_instant` 并调用校准器验证 MonoTime。校准器的永久故障与 TimeClient 自身 `fatal` 分别保存；不能因为其中一个没有设置，就忽略另一个已经拒绝读数。

## 实际验证与形式范围

首次实现的 [Red 记录](../../docs/validation/system-clock/red.json)保存编译成功后 7 项行为失败；[最终原生检查](../../docs/validation/system-clock/native-final-green.log)包含系统时钟 7 项、HTTPS TimeClient 16 项和实际原生 adapter 1 项通过，另有 [Clippy 日志](../../docs/validation/system-clock/native-final-clippy.log)。源码与测试哈希保存在 [native-results.json](../../docs/validation/system-clock/native-results.json)。

provider 错误曾只向调用方返回，未保留到 TimeClient 状态；[独立失败回归](../../docs/validation/system-clock/provider-fault-red.log)实际观察到 `Uncalibrated`，预期为 `Faulted(Domain)`。修复后，同一实例即使 provider 下一次恢复正常，也继续拒绝采样和新请求。此问题来自源码审阅与 Rust 回归，未归因于 TLC。

形式模型 [system_clock/L0_native_owner](../../tla/execution_models/system_clock/L0_native_owner/05_output.md) 的四个配置已通过 14 个不变量，分别探索 3,021、2,431、18,714 和 2 个不同状态；20 个 Next 动作均实际执行。它检查构造发布、顺序和永久故障传播，实际 u64 转换边界由 Rust 测试负责。完整 TimeClient 网络和私有流时钟 adapter 的形式组合分别保留为后续任务。
