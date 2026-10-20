# 恢复序号反例与修复

## 恢复 Unknown 与重复观察的账本序号

状态：retired(fixed, validated by L0_AccountGateway_v2)。Rust 修复与四配置强性质检查均通过。root 与 IPC 已独立核对真实路径及修复；没有发现资金损失或新增交易权限。

初模包含“账户账本序号与从原始 WAL 重放一致”的性质。TLC 在 11 个状态的轨迹中发现差异，原始模型、配置和轨迹保存在 `pre-metadata-review/`。

| 步骤 | 操作 | 账户序号 | 相关状态 |
|---:|---|---:|---|
| 1 | Init | 0 | 无意图 |
| 2 | Prepare | 0 | 候选 Admit 序号 1 |
| 3 | ClosureOk | 0 | 恢复依赖已绑定 |
| 4 | Crash 保留完整 Admit | 0 | WAL 有 Admit，旧内存丢失 |
| 5 | RecoverOk | 1 | 恢复注入 unknown=true、frozen=true |
| 6 | PrepareFact | 1 | 新成交，正常订单观察 |
| 7 | FactSyncOk | 1 | 成交已同步 |
| 8 | ApplyFact | 2 | 成交计入，confirmed=true |
| 9 | PrepareFact | 2 | 同一 trade、相同成交内容，观察改为 Unknown |
| 10 | FactSyncOk | 2 | 重复成交观察也保存 |
| 11 | ApplyFact | 2 | 在线是 Duplicate；从原始 WAL 重放则变为 3 |

`Account::recover` 最后给未闭合订单添加 Unknown；它不追加恢复标记。重复成交分支比较应用观察前后的订单与 frozen 状态，只有发生变化才增加账户序号。因而恢复进程早已有 Unknown，后续同 trade + Unknown 可不再增加序号；从原始基线重放时，这条记录首次设置 Unknown，会增加一次。

真实测试 `crates/tickwire-account-gateway/tests/recovery_metadata.rs` 使用已同步 Admit，再重开、写 partial Fill、写同 trade + Unknown、再次重开。完整编译成功，最后的序号相等断言失败：2 与 3。此前所有断言通过：base/quote AssetView、剩余订单责任、OrderView、原 AdmissionImage、传输状态和日志位置均相等；quote 总额 980、预留 36、可用 944，剩余数量 0.0006；两次均冻结、无活跃 CommittedAdmission。原 AdmissionImage 的历史序号仍为 1。

命令与原始输出见 `04_evidence/recovery-metadata-red.log`。该测试是新增证据，不属于之前已提交的全仓检查结果。

独立审查提出上限验证：把基线设为 `u64::MAX-2`，同一流程的第三条记录返回 `Ok(Duplicate)`，WAL 位置为 3，但再次恢复返回 `Journal(Account(Risk(ArithmeticOverflow)))`。真实输出见 `recovery-metadata-max-red.log`。因此差异会阻止成功接纳过的日志重新打开，不能仅当作显示值变化忽略。

另一个真实 Red 先保存部分成交及匹配的终态累计，调用本地 `freeze()`，再收到同成交但终态缺累计量。旧逻辑只比较订单和全局冻结状态，在线为 2，重放为 3。四项扩展回归全部编译成功后实际失败，日志为 `recovery-metadata-boundary-red.log`。

修复将交易所 Unknown 与恢复附加 Unknown 分开，公开查询与风险检查仍使用两者的或值；终态累计缺失、越界或冲突保存单调的 `observation_unreconciled`。重复成交依赖订单状态变化计数，不依赖全局冻结状态。普通输入两次均为 3，MAX−3 输入两次均为 MAX；MAX−2 的第三条现在在线明确报溢出，资金保持，已保存但未应用的错误记录仍阻止恢复。冻结后的新终态异常两次均为 3，精确重复不再计数。

初始资金投影诊断模型保存在 `pre-metadata-review/financial-diagnostic.tla.txt`；它没有用于结案。当前 `L0_AccountGateway_v2` 检查完整持久订单状态及序号相等，并显式转录序号溢出失败。原 Red 与首轮 Green 保留；新增精确重复上限检查的输出为 `recovery-metadata-final-green.log`，进一步比较被拒观察前后资产、订单和原 AdmissionImage 的最终输出为 `recovery-metadata-final-atomic-green.log`。
