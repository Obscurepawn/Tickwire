# 公共深度网络验证记录

生产实现位于 [tickwire-binance-public](../../../crates/tickwire-binance-public/)，接口见[公共深度网络接口](../../implementation/binance-public-session-interface.md)。

先建立能编译的 API 与本地 TLS 测试服务器，再运行真实行为断言：

| 记录 | 结果与归因 |
| --- | --- |
| [initial-compile-attempt.log](initial-compile-attempt.log) | 测试证书名称集合缺类型标注；属于夹具编译修正 |
| [initial-red.log](initial-red.log) | 4 项实际失败，盘口为 Disconnected 而非预期 Live |
| [pre-implementation.tar.gz](pre-implementation.tar.gz) | 初始接口与四项行为测试源码 |
| [first-green-attempt.log](first-green-attempt.log) | 实现编译通过，macOS accepted 测试 socket 继承 nonblocking，服务器 read_exact 得到 WouldBlock |
| [second-green-attempt.log](second-green-attempt.log) | 仅修正服务器 socket 为阻塞并保留期限，4 项完整网络桥接通过 |
| [close-red.log](close-red.log) | Ping/Pong 已成功，Close 未回送而断开，服务器实际读到 EOF |
| [close-green.log](close-green.log) | Close 原载荷回送及原 4 项网络测试通过 |
| [scheduling.log](scheduling.log) | 小预算下快照成功、传输失败两项时间顺序检查通过 |
| [rebuild-red.log](rebuild-red.log) | 初始等待期限已过去后发生断档，旧期限触发 Timeout，错误断开健康订阅 |
| [native-focused.log](native-focused.log) | 8 项通过，包含同连接重建修复 |
| [clippy.log](clippy.log) | 首次静态检查仅指出通知辅助函数参数过多；后续合并原事件与时间参数 |
| [native-final.log](native-final.log)、[clippy-final.log](clippy-final.log) | 定时唤醒修复前的 8 项检查及 Clippy 通过 |
| [timer-red.log](timer-red.log) | 安静 WS 与 HTTP 429：预期 2 秒唤醒，实际错误返回 30 秒心跳期限 |
| [native-timer-green.log](native-timer-green.log)、[clippy-timer-green.log](clippy-timer-green.log) | 修复后 9 项检查及 Clippy 通过；按报告的 2 秒期限唤醒后，同连接完成快照并进入 Live |

本地服务器检查真实 SNI/证书、HTTP Host/路径、WebSocket Upgrade、客户端掩码控制帧、订阅 ID 和参数；同一生产 owner 解析真实 HTTP/WS 字节并调用 DepthSync。四组基础测试包含 Spot/USD-M 与 Production/Test 地址配置，显式订阅还覆盖增量先于 ACK、USD-M combined 包装。

时间顺序测试让两条完整 WS 消息处于同一接收批次，而每轮只准交付一条消息和一条通知。HTTPS 完成或失败发生在剩余消息尚未处理时。测试确认同批消息保留相等时间，Book 使用最后一条增量的原接收时间，回放没有触发 ClockRegressed。

root 逐段独立审阅生产模块，没有发现其他阻断项；定时唤醒遗漏由作者复核 runner 调用契约时发现，并经 root 确认。修复仅在可重新请求快照时，把 `max(snapshot_retry_at, cooldown)` 纳入下一期限。源码摘要见 [source.sha256](source.sha256)，各项归档摘要见 [SHA256SUMS](SHA256SUMS)。

实际最终命令为 `cargo fmt -p tickwire-binance-public`、`cargo test -p tickwire-binance-public --tests` 和 `cargo clippy -p tickwire-binance-public --all-targets -- -D warnings`。测试使用真实 TCP/TLS 与协议字节，本机服务器持有临时测试证书。

这些记录来自 macOS 原生 focused 运行；没有把延迟、吞吐或长时间运行当作本轮结果。网络 owner 的独立模型和更多协议故障排列尚未形成本目录证据。已有 DepthSync 模型的运行状态保留在[独立目录](../../../tla/execution_models/binance_depth_sync/)，不以本轮 Rust 测试代替其扩大配置结果。
