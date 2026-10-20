# L0 建模依据

模式为忠实故障注入。`MockRuntime` 的 restart 注释声明日志和交易所跨模拟 exec 重启存活；本级在持久步骤后允许重启，同时保留 mock 的 commit 失败、回执丢失与事实写失败。

`pc` 表示同步调用栈位置，不表示并行任务。只有 Idle 开始 submit/resume/terminal；崩溃可发生在任意步骤。`live` 是当前 Engine 的订单分类，`rebuilt` 是 restart 中尚未发布的局部 Engine。恢复开始冻结旧 Engine，完成才替换。`pos` 是已读取日志前缀的下一条位置；`journal` 是真实内容序列，不是提交水位。

每个订单固定数量 1、零费用、正整数预算。`Held` 由 Reserved/Pending/Confirmed 的预算求和；Abort/Terminal 的占用为 0。`AmountMax` 是小整数算术上界，边界配置保留“先释放小单、再预留最大单”这个实际内容见证。Rust 测试另以 `i128::MAX` 检查真实整数边界。模型不声称小整数穷举覆盖全部 i128 算术。

`PersistRelease` 和 `ApplyRelease` 分开，验证日志已保存但应用尚未发生的中断。两个动作同时服务 Abort 和零成交 Rejected 终态；后者用于见证真实顺序问题。调用成功才进入 Terminal 状态，模型未把终态与无字节拒绝合并。`ReplayOne` 逐条解析日志并检查缺失 intent、重复 intent、矛盾终态和溢出；`ReplayCannotFail` 检查合法产生的日志不会在这里出错。

固定请求映射省略 full-record 逐字段比较、配置、期限、策略代次及对象编码；相关变化由 Rust 测试覆盖。LocalDurable 的存储成功代表 Mock 持久域，不代表 fsync/复制。没有 fills、fees、取消请求、重新授权或业务 runtime。每单至多一次终态或内部中止；重复事实由 Rust 测试覆盖。订单标识在有限运行中不复用；restart 后冻结，因此模型不会自行重新授权发送。

`used` 和 `sent` 是检查观察量；前者避免有限身份复用，后者计数 Exchange.submit 次数。`ReleaseVisible` 检查 Idle 时持久释放已反映到实际占用；`UnresolvedHeld` 检查缺少释放事实的已提交 intent 仍持有预算。两者不在恢复循环的中间状态强制局部 Engine 等于完整日志。
