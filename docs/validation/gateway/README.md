# 持久命令发送验证

验证对象为[持久发送协调器](../../../specs/implementation/gateway.md)及其 Binance REST 适配器。生命周期测试使用真实文件 WAL、正常证书与主机名校验的本地 TLS 服务器；授权服务和恢复对象存储使用显式 Mock，完整依赖清单仍由 `tickwire-recovery` 实际校验。

## TDD 记录

- [red.log](red.log)：编译成功，7 项生命周期行为断言失败；[源码摘要](red-source.sha256)对应尚未实现状态机的接口。
- [green-initial.log](green-initial.log)：初次实现发现测试装置的风险域输入错误；[tls-fixture-first.log](tls-fixture-first.log)保留后续服务端非阻塞模式问题。
- [green.log](green.log)：修正测试输入和服务端模式后的 8 项核心回归通过。
- [Binance Red](binance-red.log)：4 项实际行为失败；[最终专项记录](binance-green.log)包含后来新增的可信时间失败用例。

最终生命周期覆盖 12 项测试，包括 11 种独立门禁拒绝、实际 TLS 部分接纳、同意图冲突、日志同步失败、零接受中止同步失败、原始命令缺失和新 writer 恢复。Binance REST 另有 5 项测试，包含 Spot、USD-M 实际签名 HTTP 输出、门禁拒绝、身份与容量失败、可信时钟不可用。两产品服务端均从真实 TLS 字节重建签名原文并验签。

## 工作区与模型

[macOS 工作区输出](macos-workspace.log)和[Linux 工作区输出](linux-workspace.log)来自隔离的提交候选目录，包含全部 targets、doctest、格式与 Clippy 检查。[源码摘要](source.sha256)固定实际检查的源码、依赖锁、样本和脚本。网络专项输出、源码摘要及复核范围另见 [binance-results.json](binance-results.json)。

[发送协调 L0](../../../tla/execution_models/gateway/L0_journaled_sender/05_output.md)对应真实的单所有者调用顺序，检查同步意图先于应用输出、首次门禁、零接受中止发布、错误尾部与冻结恢复。该模型使用授权提供者和底层存储的接口保证，完整账户风险和业务回报关联继续由相应模块验证。

## 复现

```sh
cargo test -p tickwire-gateway --all-targets
cargo clippy -p tickwire-gateway --all-targets -- -D warnings
cargo fmt -p tickwire-gateway -- --check
```

工作区检查使用 `scripts/check-rust.sh` 与固定镜像的 `scripts/check-linux.sh`。归档日志保留原测试与构建输出；末尾空白行作统一清理。
