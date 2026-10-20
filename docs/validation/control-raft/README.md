# 控制 Raft 文件测试

首轮[行为失败](red.log)实际运行 4 项，均失败；最初可编译占位保存在[源码记录](pre-implementation.rs.txt)。实现 `Storage`、journal 分片及 RawNode 驱动后，最初 4 项通过；继续加入持久切点和消息边界测试后，[最终测试](final-tests.log)累计 8 项通过。

包含 app、lab、md 和控制 Raft 的[统一 Clippy](clippy-aggregate.log)以 `--all-targets -- -D warnings` 通过。命令、范围及源码摘要记录在[结果文件](results.json)中。

- 三个 voter 使用独立真实目录，提交短命令和 100,000 字节命令后全部重启，再换 leader 提交。
- 隔离旧 leader 时，其新命令和安全读均未完成；多数派选主后提交，旧节点重新接入并追平。
- Ready 和 applied 的 Begin/Chunk/End 共六个同步失败位置逐一注入。当前实例均返回错误并停止；重新打开后保留有效前缀，重试得到一条准确的命令历史。
- 已返回肯定响应的投票经重启保留，同任期向其他节点投票被拒绝。
- [Append 索引失败](append-index-red.log)和[Heartbeat 提交水位失败](heartbeat-red.log)都在实际 raft-rs 调用中触发 panic。入口补齐结构与本地日志边界检查后，两项返回明确协议错误。

构建时曾因固定 `protobuf-build 0.14.1` 未识别 Apple Silicon 而失败，记录于[构建日志](build-protoc-missing.log)。[工具选择脚本](../../../scripts/protoc-raft)复用固定依赖自带的 protobuf 3.9.0 编译器；随后才运行行为测试。

本阶段验证固定三个 voter 与本地持久化适配。snapshot、成员变更、TLS 网络 owner、配置业务 reducer 和运行时接线继续按[接口](../../implementation/control-raft-interface.md)实现。

复跑：

```sh
cargo test --locked -p tickwire-control-raft --all-targets
cargo clippy --locked -p tickwire-control-raft --all-targets -- -D warnings
```
