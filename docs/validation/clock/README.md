# 校准时钟与 HTTPS 时间查询验证

`tickwire-clock` 的 25 项行为测试和 `tickwire-binance-clock` 的 15 项网络测试均通过 macOS、Linux 检查。实现分别见[时钟接口](../../implementation/clock-interface.md)与[核心规格](../../../specs/implementation/clock.md)。[network-results.json](network-results.json) 绑定本轮七个源文件和最终日志的 SHA-256；后续适配层的修改与检查单独记录。

## 从失败到通过

| 记录 | 发现与后续结果 |
| --- | --- |
| `calibration-red.log`、`pre-implementation/` | 核心接口可编译，20 项行为测试失败；实现后通过，并扩展到 25 项 |
| `quantization-bound-red.log` | 量化误差小于一毫秒仍被接受；配置校验补上毫秒响应精度下限 |
| `https-red.log`、`network-pre-implementation/` | 网络接口可编译，9 项行为测试失败 |
| `https-green-attempt.log` | 测试的 syscall 预算不足以推进连接；另一个测试未等待旧 TCP 建立就取消，旧连接是否进入服务端 accept 队列不确定。修正夹具后 9 项通过 |
| `https-review-red.log`、`network-pre-review/` | TLS 暂时接受零字节后没有再次检查请求额度；另有 `Instant` 回退和期限溢出未反映在故障状态中。修复后 12 项通过 |
| `network-expanded-green.log` | 增加错误主机名、超长响应和失败刷新保留原到期时间，15 项通过 |

本地网络测试使用真实 TCP/TLS、独立测试 CA 和固定产品主机名。服务端核验 Spot、USD-M 的生产与测试 authority、请求路径、Host、空参数和完整请求接受边界，注入响应分片、提前回复、EOF、HTTP 错误及额度撤销。

`core-final-green.log` 与 `network-expanded-green.log` 是原生最终测试；对应 Clippy 日志通过。`linux-focused.log` 在固定 Rust 容器中检查这两个 crate 的全部测试与 doctest，没有运行该轮 Linux Clippy。私有会话组合适配和系统单调时钟不在这七个文件的验证范围内。核心 TLA 模型及独立审阅保存在 `tla/execution_models/clock/`。
