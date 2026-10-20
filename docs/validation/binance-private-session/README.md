# Binance 私有流验证记录

本地截止修复后的41项私有流及12项时钟组合原生测试、Clippy见[新增验证](../binance-private-clock-adapter/local-deadline/README.md)。下文双平台结果及本目录原 source-hashes.json 保留其验证版本；修前源码和模型完整归档在 pre-final-gate-v2。

生产客户端使用 `tickwire-transport::Connection` 建立真实非阻塞 TCP/TLS 连接。测试服务器使用独立测试 CA 和真实 HTTP/WebSocket 字节，校验 HMAC、RSA、Ed25519 签名及产品管理请求。源码/API 见[接口](../../implementation/binance-private-session-interface.md)，语义约束见[规格](../../../specs/implementation/binance-private-session.md)。

## 行为失败与修复

| 原始日志 | 实际失败或发现 | 修复与验证 |
| --- | --- | --- |
| requests-red.log | 可编译编码接口尚返回 Unsupported | requests-green.log，6 项请求行为 |
| network-red.log | 可编译 owner 尚未进行网络工作 | network-green.log，4 条实际网络流程 |
| network-green-initial.log | 完整 USD-M HTTP 回复与未发送 close_notify 的 EOF 同批到达，回复被丢弃 | 先消费完整已读回复再处理 EOF |
| boundary-red.log | 重复 JSON 身份键未拒绝；事件 Debug 输出了原始敏感消息 | 严格重复键拒绝及本 crate 的脱敏事件包装 |
| budget-red.log | 小消息预算下提前队列未先排空 | 每轮先排空已有完整事实，再继续读取 |
| control-errors-red.log | 500/-1007 和冷却分支错误 | Unknown 结果及 429/418 冷却 |
| eof-drain-red.log | EOF 前完整读取 8 条事实，只交付 3 条 | 撤销健康后继续按原 context 排空 |
| close-red.log | 收到 Close 后未回送 | 有界控制队列回送并撤销资格 |
| window-signature-red.log | 5000.120 的签名原文与实际 JSON 数值不一致 | 签名前去小数尾零，实际服务端验签 |
| partial-reply-red.log | 部分 DELETE 输出后过早回复消费 Pending，丢失 MayHaveApplied | complete 检查早于消费与状态推进 |
| config-red.log | 无效 I/O 容量配置被接纳 | 构造时校验容量与 TLS 早数据 |
| ack-eof-red.log | 订阅 ACK、事件、EOF 同批读取后仍显示 Receiving | 在消息处理前后均撤销 ended 流资格 |
| clock-binding-red.log | 新类型接口尚未接入 Source/domain/区间与首接受复查 | clock-binding-green.log，5 个真实失败转为通过 |
| counter-red.log | gap/backoff u64 溢出后状态仍为 Receiving | counter-green.log，进入 Exhausted 并清理连接 |
| clock-domain-error-red.log | 首接受闭包吞掉显式 ClockDomain 错误，转为 Backoff | clock-domain-error-green.log，sample/check_signed 错误保留分类并永久停止 |

`partial-reply-compile.log` 是测试数组比较的编译错误，不算行为 Red。`spot-lifecycle-initial.log` 的碎片让出断言失败来自测试服务端逐片执行 TLS write，输入未保证同批；改为一次写入完整碎片组后在 spot-lifecycle.log 通过，生产解析未修改。`native-clock-quantization-fixture.log` 来自 clock 模块收紧毫秒采样的量化误差下限，测试原设 1 ns 被拒绝；测试改为明确的 1 ms 后重跑。

## 源码版本与独立审阅

`pre-clock/` 保留 typed clock 迁移前的 11 个 crate 文件、源包和哈希；该版 27 项测试见 review-final-green.log。它使用裸签名时间，不包含后续 SignedTime/区间检查，不沿用为迁移后的测试结论。

market_core 对初版网络 owner 的只读审阅发现并核对了 recvWindow 尾零、部分回复 Pending 消费顺序和 ACK+EOF 健康撤销三个缺陷。后续 typed clock 独审另指出显式 ClockDomain 错误在首接受闭包被吞掉，真实 Red 与修复分别记录。market_core 复读修复及完整 typed clock 增量后，无开放阻断项；审阅没有独立重跑网络测试。

最终原生与 Linux 全量日志、Clippy 结果及最终源码哈希在本目录归档；各测试输出保留实际命令的运行结果，统计以最终日志为准。纯状态模型和 TLC 结果单独放在 `tla/execution_models/binance_private_session/`。

## 最终检查

原生与 Linux 各 41 项测试通过（2 个内部计数边界、6 个时钟绑定、13 个故障、6 个协议流程、6 个请求编码、4 个 Spot 生命周期、4 个 USD-M 生命周期）；两平台 Clippy 均通过。源码与依赖哈希见 `source-hashes.json`。Linux 使用镜像 `rust@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29`。

```sh
cargo test -p tickwire-binance-private
cargo clippy -p tickwire-binance-private --all-targets -- -D warnings
docker run --rm --mount type=bind,src="$PWD",dst=/work -w /work \
  -e CARGO_TARGET_DIR=/work/target/linux-private \
  rust@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29 \
  cargo test -p tickwire-binance-private
```

Linux Clippy 使用同一 Docker 参数，将末行换为原生 Clippy 命令。网络 EOF、签名原文、部分接受及 clock 错误分类修复经过独立只读审阅；审阅未代替上述实际执行。
