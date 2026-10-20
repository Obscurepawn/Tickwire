# L1 推导

新增关注点是 exec 易失状态与外部记录的完成边界，模式为 faithful fault injection。MockRuntime 的 restart 注释和实现明确让 journal、objects、exchange 跨模拟进程重启存活；模型允许在每个调用边界发生崩溃。`CommitAndLoseReceipt` 先保存记录再返回失败，`CommitFailure` 没有保存记录，二者保持独立。

实际 `&mut MockRuntime` 同步串行提交，没有两个同时执行的 submit 调用。`pc` 表示这条调用链的进度，Idle 时可以开始新提交、重试已有 intent 或调用其他 Engine API；一次 method 尚未返回时，只允许模拟进程崩溃，不插入另一个所有者修改 gate。MockExchange::submit 在进程外接受请求的作用被单独计数，崩溃时保留，因此“无重复提交”有真实行为见证。

| 具体行为 | 抽象及依据 |
| --- | --- |
| reserve、恢复预算、订单槽、固定 profile、sequence、send state | 沿用 L0；每笔预算相同，但共享总额仍限制竞争 |
| 不可变恢复对象 | 每个 intent 使用独立 recovery_object；request 除 profile 外按 intent 固定，对象内容相等简化为 profile 相等，真实 API 逐字节比较；跨 intent 共用对象 key 的冲突不在本级 |
| journal 内容与回执 | journal 是成功保存的记录集合；receiptReplica 是某次回执的副本完成位；不会把丢回执解释为没有记录 |
| journal identity/sequence 去重 | 显式保存 journalProfile/journalSeq，冲突不允许写入 |
| caller 传入 commit profile | StartSubmit/Resume 分别枚举匹配和不匹配的 profile，RejectReceipt 覆盖画像降级拒绝 |
| dispatch/accepted 次数 | 独立历史计数跨崩溃存活，NoBytes 不增加 accepted，但仍增加 dispatch |
| 进程死亡 | 清除 Engine 和待处理调用，保留 journal/objects/exchange；恢复按真实代码整体替换 Engine |
| 不可用服务、配置冲突 | 错误返回简化成 DependencyFailure/CommitFailure 或调用前 stutter；失败不产生虚构 durable 记录 |
| 本地/副本持久化 | mock 的布尔契约，不声明真实 fsync、远端磁盘或机器掉电结果 |

模型允许提交成功保存但本地尚未接受回执、发送授权后尚未调用交易所、交易所接受后尚未处理 ACK 这些位置崩溃。恢复时即使记录从未发送，也保守保留全部预算并冻结，避免同一 intent 被重放为新单。当前没有解冻/新 epoch 激活 API，所以 RecoveryStaysFrozen 是当前实现的安全性质；它不表达系统最终恢复交易的活性。

primary 使用一个 intent/一次崩溃；scaling 使用两个 intent/两次崩溃，资金限制为三单位、每单两单位。后者包含“已提交但未发送的订单被本地撤销后释放资金，再接纳另一单；崩溃时两笔 durable intent 都恢复预算”——恢复占用可以超过 capital，但 frozen 阻止新风险，所以只在未冻结状态检查 NoOvercommitWhileActive。

未建模 fill、fee、terminal、私有回报与 ACK 的交错、事实日志重放、deadline 的真实时钟刷新、多 exec fencing 和恢复后激活。近期 Rust 回归修复的 fee→NoBytes 冲突属于 L2。MaxCrashes 是显式故障次数边界，没有添加状态约束裁掉队列或失败分支。

`Pending` 对应真实 PendingResponse：Dispatch 记录传输已接受，Respond 保持 MayHaveSent 与原 frozen 值；`Unknown` 对应显式不确定结果并冻结。额外 pending 配置提供四单位资本，使两笔各两单位的正常等待订单可以共存。异步 ACK 后续调用在本级没有加入，L0 已覆盖独立 ack 状态迁移。
