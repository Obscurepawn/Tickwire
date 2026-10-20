# Binance 盘口同步协调验证

新增 `depth_sync` 把现有精确 JSON 解码、真实 Book 和有限增量缓存接起来。构造器与状态操作均可由实际网络 owner 调用；协议测试中的服务器替代交易所，客户端使用仓库中的真实 transport。

## 行为证据

[red.log](red.log) 中 12 项测试已经编译通过，随后因接口占位实现拒绝连接而真实断言失败；[red-source.rs](red-source.rs) 保存该实现。完成协调逻辑后 12 项通过，随后补齐 6 项边界测试和 2 项协议集成测试。

新增行为测试 18 项，实际 HTTPS/WSS 集成测试 2 项。覆盖订阅前缓存、首条 U、有限 REST 深度、Spot/USD-M 不同桥接、FIFO 溢出、回放后段失败、请求超时和过期响应、同连接重建、实际断连后新连接、Unicode path、双侧总容量及 Unknown。完整列表见 [Specs](../../../specs/implementation/binance-depth-sync.md)。

两产品的服务器都先完成 WSS 握手并接收 SUBSCRIBE，再接受 REST 请求；在 HTTP 响应前通过分片 WSS 继续推送增量。随后制造序列断档，客户端在同一连接重新缓存和请求；最后 Close 后建立第二条真实 WSS/HTTPS 连接并验证新的 epoch。HTTP 使用 chunked，测试限制每次 TLS 接受与读取的字节数，生产协调器处理实际解码后的消息。

## 复跑与来源

```sh
cargo test -p tickwire-binance --all-targets
cargo test -p tickwire-binance --doc
cargo fmt -p tickwire-binance -- --check
cargo clippy -p tickwire-binance --all-targets -- -D warnings
```

Linux 使用固定镜像 `rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29`；macOS 与 Linux 全 crate 均为 108 项通过，doctest 为 0，格式检查和 Clippy 通过；实际结果保存在 [native.log](native.log)、[linux.log](linux.log)、[fmt.log](fmt.log)、[clippy.log](clippy.log) 和 [results.json](results.json)。测试不计时性能，不运行持续压力负载。

Spot 官方原文固定到提交 `f4a9c0be68235f758b46f571f7313c63d1306b25`。USD-M 官方同步页提供 `.md` 内容入口，可实际取得 1112 字节原文，SHA-256 为 `3cb2268d6a07f841627b164ec4b32eb9e4faa2a76dc2b5357c2ed8bd025e03db`；该站未提供不可变提交，本次按实际下载的内容哈希固定。普通 HTML 入口在命令行返回 202 空响应，因此不把空响应哈希当作协议证据。[official-sources.json](official-sources.json) 保存来源和规则摘要，[capture-sources.py](capture-sources.py) 可重新下载、检查状态并计算摘要；源站变化将产生不同摘要。

Spot 初始化先拒绝落后于首条缓存 U 的快照；通过这一步后 Book 允许 U=L+1 的增量。USD-M 初次覆盖 L，后续核对 pu。有限快照以外的未观察数量保留 Unknown。此处仅归纳本协调器使用的同步规则，网络 owner 仍负责订阅确认、Ping/Pong、请求关联、退避和断连通知。

[源码摘要清单](SOURCE.sha256) 固定协调器、测试、模块导出和所消费的 feed/params/Book 源码。该清单的 SHA-256 为 `7c6ff2c7185ef1b047e7e94ce33ce9bf96be1fed677d92f8de6ae64f53a3c84e`。
