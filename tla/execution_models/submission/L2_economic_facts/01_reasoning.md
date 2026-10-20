# L2 建模依据

新增关注点是同一笔订单的经济事实如何改变已花费金额、未结预留，以及重复、冲突和迟到事实的处理。模式为 faithful transcription；Mock 自带的 append 失败和同步 restart 按已有方法转录，不引入并发写 Engine 的线程。

`Engine` 和 `MockRuntime` 的变更方法均持有 `&mut self`。一个调用结束后才开始下一个调用；各笔订单的回报可以任意交错。`Record` 中 journal 追加成功后，即使 apply 返回错误，原始事实也留在日志。`Restart` 先冻结原 Engine，再建局部 Engine 并逐条重放；失败保留已冻结的原 Engine 和金额，成功发布冻结的局部 Engine。此入口冻结由 L2-001 的 Rust 回归与 v2 模型验证。

| 源码量 | 模型量与含义 |
| --- | --- |
| orders、spent、held、frozen | `engine`：当前 Engine；spent/held 是所有订单的合计；send 字符串 Unknown 对应 `SendState::MayHaveSent` |
| filled | 已成功写入成交明细的数量 |
| observed_filled | max(此前观察到的累计量、本次终态累计量、已记明细量)，不是事件序号 |
| fees_paid | 已成功记入的独立手续费金额，零金额也保存事件和确认发送 |
| events[T] | 完整内容的有限集合；去重键是 kind+id，Fill 和 Fee 的同数字 ID 互不冲突 |
| facts Vec | `journal`：有序原始事实，包含重复及 apply 失败的记录 |
| committed intents | `present`：本级成功完成合法发送入口的订单集合 |
| restart 临时 Engine | 纯函数 `Replay` 的局部结果，只在全部成功时发布 |
| 检查见证 | `checks`：记录失败原子性、重复幂等、恢复冻结、控制回报不改变经济账本等跨状态性质 |

模型按源码保留 terminal 后的迟到 fill/fee。terminal 的累计量大于已记明细时保留差额数量预算；费用预留为 max(fee_budget-fees_paid,0)。取消请求与取消拒绝只改变 cancel。成本超过原预算的有效事实仍记账，`spent+held>capital` 时冻结；available 允许为负，所以本级不以余额永远非负作为不变量。

模型的事件容量是每订单共享 Fill/Fee 槽数。重复先于容量判断，冲突先于容量判断。新事实先在订单副本中计算；事件无效、槽满或 checked arithmetic 失败时仅冻结，原订单、金额和已保存事件不变。NoBytes 只有 MayHaveSent 且尚无终态、观察成交或实际成交时才释放全部预算；有效 fee 将 send 改为 Confirmed，包含零费用。

有限域保留数量 2、单位预算 1、费用预算 1。主配置一笔订单，扩展配置两笔订单；日志分别限制为 3/4 条。上限是环境停止追加事实的边界，不是源码日志容量。记录集合包含同 ID 不同内容、零费、负费、完整/缺失成交明细、取消/完成终态和超大金额。顺序全部由 TLC 枚举，未施加网络先后顺序约束。

算术采用边界代表值 `ArithMax=1000`：小金额保持原值，1000-k 对应 i128::MAX-k。普通金额累计不超过 20，远低于边界；合法状态至多含一个近上界金额。小额相加、上界附近相加/减去旧值、大小比较和 checked overflow 在该有限域保持一致。这个嵌入覆盖 selected i128 正上界分支；没有枚举所有 128 位数值或 u64 数量加法溢出。Rust 定点运算的 MIN/scale 边界不属于此级。

事件槽的物理顺序被删除，因为代码只按键匹配并找首个空位，不依赖此前槽序。合法 reserve/commit/begin_send 合并成 `Admit`，其 gate/profile 来源由 L0/L1 检查；本级仍重新执行资本准入，冻结后禁止 Admit。ACK 和取消标记在重启时丢失，NoBytes 的本地终态也未写入事实日志；因此恢复可以增加保守预留，已失败的 unsent 事实也可能在恢复为 MayHaveSent 后被成功记入。模型不假设恢复前后每个控制字段相同，也不把“所有历史 apply 错误必然导致重放失败”设为结论。

`Canceled/Rejected/Expired` 在 terminal 计算中都只表示“已有非 Filled 终态”，本级输入合并为 Canceled；完整 Filled 单独保留。ACK、uncertain 和取消请求的无效调用若只返回错误而不改状态，以 stutter 表示。事实输入限定为已有 intent；不存在 intent、非法 domain/sequence、u64 上界、恢复 intent 时的预算总和溢出，以及损坏提交记录的恢复分支未纳入本级有限域；本级反例发生在已成功恢复全部 intent 之后的事实重放。
