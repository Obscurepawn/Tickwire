# 恢复后的观察去重与账本序号

Gateway 组合模型首先发现：恢复时添加的 Unknown 会遮住随后首次写入的交易所 Unknown，导致在线处理和完整重放使用不同账本序号。独立审阅进一步指出，主动冻结也会遮住重复成交附带的新终态异常。

真实回归位于 `crates/tickwire-account-gateway/tests/recovery_metadata.rs`。使用真实 FileBackend、规范基线、完整依赖闭包、已同步 Admit 和本地 TLS 连接，提交后未调用发送 poll；服务端确认没有应用字节。之后重开、保存部分成交、保存同一成交的新 Unknown 观察并再次重开。

| 输入 | 修复前实际结果 | 修复后实际结果 |
|---|---|---|
| 基线序号 0，恢复后部分成交及同 trade + Unknown | 在线序号 2，重开序号 3 | 两次序号均为 3 |
| 基线序号 MAX−3，同上 | 在线 MAX−1，重开 MAX | 两次均为 MAX |
| 基线序号 MAX−2，同上 | 第三条返回 Duplicate，再次恢复 ArithmeticOverflow | 第三条在线明确返回 ArithmeticOverflow；已保存的被拒记录也阻止恢复 |
| 已有终态累计量，再主动冻结，再收到同成交但缺累计量 | 在线序号 2，重放序号 3 | 两次均为 3，精确重复该异常不再增加 |

前三种输入分别比较资产余额、预留、策略归属、订单剩余责任、公开 Unknown、原 AdmissionImage、WAL 位置和发送资格。普通及 MAX−3 输入保持完整一致。MAX−2 输入保持原资金状态和冻结状态，没有把不可应用记录返回为成功。

执行命令：

```sh
PATH="$HOME/.cargo/bin:$PATH" cargo test -p tickwire-account-gateway --test recovery_metadata -- --nocapture
PATH="$HOME/.cargo/bin:$PATH" cargo test -p tickwire-account -p tickwire-account-codec -p tickwire-account-journal -p tickwire-account-gateway --all-targets
```

首轮 4 项行为测试全部失败，修复后 4 项全部通过；相关四个 crate 共 118 项测试通过，账户与协调器的格式检查和 Clippy 通过。最后补充的断言确认，账户序号已到 MAX 时，相同观察仍可返回 Duplicate；被拒的溢出观察保持资产、订单和原 AdmissionImage。最终目标运行记录为 `recovery-metadata-final-atomic-green.log`。原始普通序号 Red、边界 Red、MAX 重开失败、Green 和相关模块回归输出保存在 [Gateway 模型证据目录](../../../tla/execution_models/account_gateway/L0_guarded_account_output/04_evidence)。这些是新增检查；先前提交中的全仓结果保持原样。

root 从隔离源码副本完成 macOS 653 项、Linux 726 项测试及格式、Clippy 检查。该完整检查使用补强前的测试文本，四个实现文件与最终版本相同；补强后的四项测试在两端单独重新通过。两版源码清单及各自日志由 [workspace-results.json](workspace-results.json) 绑定，不包含同期其他模块的开发中变更。

修复区分持久交易所 Unknown 与恢复附加 Unknown；终态异常在订单中保留单调标记。重复成交不再依赖全局 frozen 的变化判断是否增加序号。IPC 独立核对源码差异和实际日志，确认所有观察冻结分支均有订单字段记录，公开风险拒绝条件保持。
