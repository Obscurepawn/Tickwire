# 执行内核与 MockService 规格

Status: active（核心 reducer 与服务回归已运行；系统实现继续进行）

## 接口与状态

接口设计见 [execution-interface.md](../../docs/implementation/execution-interface.md)。实现位于 `tickwire-execution`，外部服务测试实现位于 `tickwire-mock`。`OrderRequest` 固定账户域、唯一意图、数量、单位支出上界、费用预算、恢复对象、授权版本、期限和提交模式；收据不能改变原意图的提交模式。

一次准入原子地预留 `quantity × unit_budget + fee_budget`。订单持久化后才可尝试发送；调用任何交易所适配器前先变为 `MayHaveSent`。本地成功提交、提交收据丢失、交易所 ACK 丢失是不同事件。默认提交模式不等副本；显式同步提交模式等同一记录的副本持久收据。

`spent` 只记录实际成交支出和费用。`held` 由尚可能成交的数量、终态中尚缺明细的累计成交量与费用尾款组成。拒绝未发送意图通过 `fee_released` 释放尾款，不增加 `fees_paid`。成交与费用身份分别去重，不用最大成交 ID 跳过乱序事件。真实支出超过资本时保留事实、暴露负可用额并冻结新增风险。

去重容量耗尽、冲突、无效或无法表示的外部经济事实使内核冻结；经济字段保持失败前状态，增险资格发生变化。Mock journal 先保存收到的事实，随后应用，无法解释的事实会使恢复失败并保留现场。存储失败也冻结增险，调用者保留输入并重试存储。

## 测试驱动记录

1. 先写 `tests/lifecycle.rs`，`cargo test -p tickwire-execution` 因缺少 `Gate`、`Engine` 等接口失败，再实现接口与 reducer；首批 11 项通过。
2. 添加 `no_bytes_release_does_not_invent_fee_payment`，观察失败：`fees_paid` 为 2，期望为 0；改为独立 `fee_released`，回归通过。
3. 独立源码审查发现去重容量耗尽未冻结。添加 `unaccountable_external_fact_freezes_new_risk`，观察 `assert!(e.frozen())` 失败；为外部事实错误统一冻结，回归通过。
4. 增加提交模式不可降级回归，固定提交模式存于请求及持久记录；首轮扩展到 14 项通过。
5. 继续独立审查：费用事实后迟到的 `NoBytesAccepted` 原本会释放本金，正费用和零费用回归均先失败；现费用确认发送，矛盾回调冻结。明确 Unknown 原本只保留占额，未落实共享池冻结；先失败回归后修复，生命周期共 16 项通过。
6. MockService 先写 9 项测试，因尚无 `MockRuntime` 等类型失败；实现 SQL CAS、不可变 KV、journal 提交/收据故障、交易所 ACK 丢失与恢复后全部通过。随后增加空 journal 重启回归，修复重启时沿用旧授权的问题，服务回归扩展到 10 项通过。
7. 非阻塞适配器新增正常等待回报的 `PendingResponse`，先验证缺失分支编译失败；正常等待时允许预算内并发，明确超时后冻结，服务回归共 11 项通过。
8. `spot-roundtrip` 示例测试串联真实盘口/风险/IPC 库与服务 Mock，运行结果为 `submit_calls=1 spent=101 held=1 recovered_frozen=true`。

后续测试计数以实际 `cargo test --workspace` 输出和[实现状态](../20261020-implementation.md)为准。

## 服务替换契约

`ConfigStore` 的 compare-and-exchange 对 revision 原子生效；`ObjectStore` 对同 key 同内容幂等，冲突内容拒绝覆盖。`Journal` 提交将准确请求、序号和恢复对象绑定在收据里。Mock 使用内存对象模拟稳定外部服务，`restart()` 只重建 exec，不清空 journal/KV/交易所。

`Exchange::submit` 返回明确未接收可输出字节、正常等待回报、明确不确定、已 ACK 四种结果。业务代码不因超时自动重发。测试使用 venue 7 和 42 验证身份无币安常量依赖；未来适配器各自实现认证、协议编码和回报转换。

## 仍待实现

- 当前执行原语使用单个结算资产的有界支出，尚未把完整 Spot 多资产、Futures 保证金、策略归属及授权表集成为生产 OMS。
- `Journal` 的真实 WAL、哈希/CRC、RecoveryManifest 闭包、复制和截断尚待实现；Mock 中的依赖字节只用于测试。
- 恢复默认冻结，没有用虚构“对账成功”解除冻结；真实对账、fencing、检查点和订单槽回收需独立实现。
- 当前服务测试在一个进程内运行，尚未覆盖 SHM 传输、实际 TLS、掉电与进程故障。
- 有界 TLA+ 结果单独登记在 `tla/`，不由 Rust 测试推断。
