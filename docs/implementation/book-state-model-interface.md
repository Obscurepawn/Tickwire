# Book 生命周期模型接口

此文件记录 `tickwire-book` 有限模型的调用边界，独立于已冻结的[订单簿接口](book-interface.md)与性能证据。

模型中的写动作对应一个完整的 `&mut Book` 调用：安装快照、应用增量、开始重建、外部失效。读动作对应一次 `view(now, maximum_age)` 及即时读取；返回的借用在下一次修改前结束。`BookView` 不会自动追踪后续墙上时间，其年龄是调用时的检查结果。

| 接口 | 成功效果 | 拒绝效果 |
| --- | --- | --- |
| `new` | AwaitingSnapshot，内容与水位为空 | 无 |
| `install_snapshot` | 整体替换双侧，设置水位，进入 Bridging | 错身份/状态保持原状；当前合法状态中的非法快照仅使状态 Invalid |
| `apply` | 双侧与水位/时间整体提交，进入 Live | 旧事件保持全部原值；序号/时间/批次错误使状态 Invalid，内容不半写 |
| `begin_rebuild` | 仅更大 generation，清空内容、水位、覆盖与时间 | 非递增 generation 保持原状 |
| `invalidate` | 当前 generation 进入 Invalid | 旧 generation 回调不改变当前簿 |
| `view` | 仅 Live 且调用时不过期、不倒退 | 无状态修改 |

L0 具体转录 Spot 与 USD-M 的 `SequencePolicy`。批次内容用有限双侧见证表示，校验器结果作为合法或非法输入类别；它检查失败提交边界，不验证排序过程。L1 以有限价格和隐藏完整内容见证细化 coverage、quantity 与 best；两层均已完成主配置和扩大配置。

有效 view 可以返回未知最优价。模型检查这个知识边界，但不把仓库中尚未实现的策略适配器或执行端 `market_valid` 更新规则当成既有接口。

L1 的完整内容见证表示已接受水位，不是实时交易所真值。模型以快照及 complete 标识准确、事件包含完整版本变化为输入契约；网络适配器的契约实现另行验证。模型输入、范围与结果见 [Book 状态验证契约](../../specs/implementation/book-state-model.md)。
