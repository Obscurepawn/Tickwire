# 账户发送组合验证

测试执行实际账户计算、账户 WAL、非阻塞 TLS 发送和恢复重放。`tests/binance_rest.rs` 另外使用真实 `FileBlobStore` 保存恢复资料，并由本地 TLS 对端收到的请求独立校验 Spot、USD-M 的参数和 HMAC 签名。授权与控制状态由明确的 Mock 提供。

## 接口与失败测试

`red.log` 记录首批四项测试在可编译接口桩上的行为失败，`red-source.sha256` 固定该版本的接口与测试摘要。实现后，同一组测试通过，见 `green-initial.log`。

`lifecycle.log` 保留扩展测试首轮结果，其中撤单 fixture 复用了原意图的 `recovery_object`，被不可变绑定正确拒绝；修正为新意图独立的恢复对象 ID 后通过。开发期间测试服务端还显式恢复了已接受 socket 的阻塞模式，避免 macOS 继承监听 socket 的非阻塞设置后提前结束 TLS 读取。

独立审查发现：若交易所事实已经到达，本地取消不能先记录 Aborted 再检查账户是否允许。真实失败复现、修复和两平台独立验证见 [持久化复核](review-failures/README.md)。当前实现先在候选账户验证中止，再同步日志并发布候选账户。

## 行为范围

当前新增 25 项测试：

| 测试文件 | 项数 | 检查内容 |
| --- | ---: | --- |
| submission.rs | 4 | 准入只预留一次、最终授权拒绝、部分接受后取消保留责任、USD-M 真实资金预留 |
| lifecycle.rs | 12 | 恢复无重发、持久中止、缺失恢复资料拒绝、当前门禁各条件、零 I/O 预算、重复与冲突、事实入口限制、重复成交的新观察、冻结后的撤单查询、拒绝输入保留 |
| persistence_failures.rs | 7 | Admit/Aborted/VenueEvent 的实际半写与同步回复丢失、已观察订单拒绝本地中止 |
| binance_rest.rs | 2 | 两产品实际签名 TLS 请求、本地 WAL 和恢复资料文件、重开资源一致及无活跃发送凭据 |

故障测试使用实际文件后端包装，区分写入半批与同步成功后丢失返回值；它们没有模拟设备断电。恢复始终核对每次准入之前的历史，任何已拒绝的外部输入都不能在重放中被跳过。

## 重现

```sh
cargo test -p tickwire-account-gateway --all-targets
cargo clippy -p tickwire-account-gateway --all-targets -- -D warnings
cargo fmt -p tickwire-account-gateway -- --check
```

单模块原生结果为 `native.log` 和 `clippy.log`。阶段全仓检查使用从暂存树导出的独立源码副本，分别执行 `scripts/check-rust.sh` 与 `scripts/check-linux.sh`；最终测试结果和源码摘要保存在 `results.json` 与 `workspace-sources.sha256`。

这一组合仍依赖调用者提供当前权限、owner 隔离和可信时钟。网络会话重建、业务响应关联、恢复后的重新授权及共享恢复资料的持久化优化不属于这 25 项测试的覆盖范围。
