# 反例与审阅记录

最终四配置的性质检查结果见 [运行记录](03_run_log.md)。本层没有由 TLC 确认的 Rust 缺陷。

独立源码/模型审阅发现两项模型问题，已在本层关闭前修正：

| 问题 | 影响 | 修正与复核 |
| --- | --- | --- |
| 时间域原为 0..1 | 成功请求期限只能为 1，成功回放只能在 0，缺少到达与完成时间不同的见证 | 四配置扩大到 0..2；单列 SnapshotDelayed，性质覆盖两类成功结果；ipc 复核通过 |
| 回放失败后计数耗尽仍使用安装前 Book | 对关闭可见性的结论没有影响，但 diagnostic last_update_id 与源码不符 | 先采用 Replay 的已安装/部分回放内容，再 Reset；耗尽只失效，正常换代清空；ipc 复核通过 |

这两项来自源码对应审查；没有将其标成 TLC 发现的实现错误。延迟动作最初使用纯别名，TLC 将两个分支的覆盖输出合到 SnapshotResponse。随后在 wrapper 添加与既有 outcome 同义的检查，保留状态关系，并重新运行以取得独立动作计数。调整前的模型、配置和两产品主运行保留在 [pre-distinct-coverage](runs/pre-distinct-coverage/)。

早期建模的五份日志保留了语法/求值问题：`authoring-first.log` 为 `=<<` 分词，`authoring-second.log` 为量词/IF 缩进，`authoring-third.log` 为同一量词中依赖前一变量的集合，`authoring-none-type.log` 为字符串 None 与 record 比较，`authoring-none-access.log` 为在 None 上选择 token 字段。最终分别改为明确分词、完整 IF、嵌套量词、模型常量 None、显式 IF。这些日志是模型编写记录，没有构成生产代码的性质违反。


扩大配置首次运行因满队列重复枚举事件而求值缓慢，手动停止，进程退出码为 143；没有取得 PASS。旧文本、配置、两主配置已完成日志，以及扩大配置和原可达性探测的未完成日志保留在 [pre-overflow-enumeration](runs/pre-overflow-enumeration/)。随后仅移动与后继无关的存在量词，等价理由见 01，最终安全结果全部来自优化后的同一文本。

另用 [正向可达性探测](runs/late-replay-witness/L0_LateReplayProbe.tla) 确认前段已应用、后段回放失败可达。它扩展同一冻结模型并加入 `WitnessNotReached`，预期 TLC 反驳这个观察断言；探测用约束选择一条具体轨迹，不用于安全性 PASS。实际 12,801 generated、146 distinct、深度 7、退出码 12，完整状态见 [reachability.log](runs/late-replay-witness/reachability.log)：

| 状态 | 调用 | 关键变化 |
| --- | --- | --- |
| 1 | Init | Disconnected，generation 0 |
| 2 | Connect | connection 1，generation 1，Buffering |
| 3 | Subscribe | 本连接已订阅 |
| 4 | DeltaBuffer | 缓存 U=0/u=1、qty=1、stamp=0、payload 有效 |
| 5 | Request | 当前 token=(1,1,1)，deadline=1 |
| 6 | DeltaBuffer | 再缓存 U=u=2、qty=1、stamp=0、payload 无效 |
| 7 | SnapshotValid | 第一个事件成功应用，第二个失败；结果 LateReplayFailure，generation 2，Buffering，缓存和 Book 清空 |

这个轨迹说明关闭性质具有实际失败场景；第 7 步发生在完整 snapshot 调用内部，外部没有读取中间 Live 的机会。
