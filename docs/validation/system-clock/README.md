# 系统单调时钟验证

`tickwire-clock-sys` 使用系统提供的、包含挂起时间的单调时钟，构造时先读取一次。后续时间回退、无效 `timespec`、系统调用失败或纳秒换算溢出都会保存故障；相等读数可以继续使用。调用方分配的 `ClockDomain` 还需绑定机器启动、进程代次及适用的 Linux 时间命名空间，模块不会用墙钟推导这些身份。

7 项系统适配测试检查真实原生读取与内部边界；HTTPS 客户端另有 16 项行为测试及 1 项原生适配测试。[native-results.json](native-results.json) 绑定该轮八个源码文件。两平台的最终全仓结果见[组合验证](../clock-session/README.md)。

提供者故障回归先观察到：客户端传播了域错误，但 `status()` 仍是 `Uncalibrated`，之后还会再次调用提供者。失败日志与修改前源码保存在 `provider-fault-red.log` 和 `pre-provider-fault/`。修复后，域不符、时间回退、溢出、时钟跳变和永久故障均保存在客户端中；后续读数与采样请求返回原故障。测试逐项检查这五种错误及提供者未再被调用。

本级 [TLA+ 模型](../../../tla/execution_models/system_clock/ROADMAP.md)转录构造探测、单所有者移动、原生故障和客户端故障保持。OS 时钟契约由系统接口提供；模型的有限时间值验证顺序，完整 `u64` 换算边界由 Rust 测试验证。
