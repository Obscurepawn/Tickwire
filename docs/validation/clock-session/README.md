# 时钟与私有会话组合验证

本轮冻结五个新增 crate：`tickwire-clock`、`tickwire-clock-sys`、`tickwire-binance-clock`、`tickwire-binance-private` 和 `tickwire-binance-session`。它们实现校准区间、系统单调时钟、HTTPS 时间查询、Spot/USD-M 私有会话及签名时钟接线。

全仓检查使用独立源码目录，241 个源码、测试与清单文件的摘要保存在 [workspace-sources.sha256](workspace-sources.sha256)。结果和日志摘要见 [workspace-results.json](workspace-results.json)。

| 检查 | 结果 |
| --- | --- |
| macOS 全仓测试与 doctest | 753 项通过，0 失败，3 个辅助入口忽略 |
| Linux 全仓测试与 doctest | 826 项通过，0 失败，5 个辅助入口忽略 |
| 两平台格式检查 | 通过 |
| macOS Clippy | 通过 |
| Linux Clippy | 在新的容器内构建目录通过 |

Linux 首次检查已完成全部测试，随后 Clippy 报 `E0463`，无法加载 `mio` 与 `rustls`。完整原始记录保留在 [workspace-linux.log](workspace-linux.log)。同一份冻结源码和固定工具链在新构建目录通过 Clippy，见 [复查日志](workspace-linux-clippy-clean.log)；未修改产品源码，首次错误的根因尚未确定。

新增模块的验证分别保存在[校准核心与时间查询](../clock/README.md)、[系统时钟](../system-clock/README.md)、[私有会话](../binance-private-session/README.md)和[组合适配器](../binance-private-clock-adapter/README.md)。网络测试使用真实本地 TCP/TLS/HTTP/WebSocket，服务端独立检查请求、签名和会话顺序。上述全仓计数不包含此后新增的公共深度网络 owner。

形式模型按模块记录范围和运行状态：[时钟校准](../../../tla/execution_models/clock/ROADMAP.md)、[系统时钟所有权](../../../tla/execution_models/system_clock/ROADMAP.md)、[私有会话生命周期](../../../tla/execution_models/binance_private_session/ROADMAP.md)。全仓测试通过不会替代尚未结束的模型检查。
