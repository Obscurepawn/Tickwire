# L2 输出：订单经济事实与恢复

修复后的 `L2_EconomicFacts_v2` 四配置全部 PASS。TLC 穷尽检查了有限域内成交、手续费、终态、重复与冲突、事件槽耗尽、金额上界以及一次 Mock 同步重放的交错。原模型捕获的 L2-001“恢复失败保留旧交易权限”已由真实 Rust 回归确认；恢复入口先冻结旧 Engine 后，原反例消失。

| 配置 | 不同状态 | 完整深度 | 域 |
| --- | ---: | ---: | --- |
| primary | 24,036 | 8 | 1 订单、2 事件槽、3 条日志、capital=3 |
| long | 280,458 | 9 | 1 订单、3 事件槽、4 条日志、capital=3 |
| boundary | 1,495,032 | 11 | 2 订单、2 事件槽、3 条日志、capital=ArithMax |
| scaling | 1,485,288 | 11 | 2 订单、2 事件槽、3 条日志、capital=6 |

每笔订单数量为 2、单位预算为 1、费用预算为 1；每个配置最多重启一次。ArithMax=1000 是 `i128::MAX` 的上界代表值，保留所选小金额与上界附近运算的分支，映射依据见 01_reasoning.md。

模型中的订单花费和成交量等于成功保存的唯一经济事件合计，池总账等于各订单合计。完全重复的事实不改变 Engine；冲突、容量和算术失败保持原订单与金额，并冻结准入。有效的超预算支出仍记账，允许出现可表示的 deficit，同时冻结。

终态先于成交明细时，缺失数量预算继续保留；迟到 fill/fee 仍可处理。取消请求与拒绝不释放经济预算，零费用也确认发送，随后矛盾的 NoBytes 只能冻结而不能释放预留。Mock 先追加事实再 apply，失败记录保留；恢复成功或本级覆盖的重放失败都保持冻结。

## Finding

L2-001：retired(fixed, validated by L2_EconomicFacts_v2)。root 提出独立源码候选并完成 Rust Red/Green；risk_core 复现 TLC 反例、转录 v2 并运行全部配置；ipc_codec 独立确认 API 可达性及修复方向，root 最终审阅 v2 的源码忠实性、数值嵌入和失败见证通过。完整反例与审查记录见 04_counterexamples.md 和 06_notes.md。

## Fidelity & coverage

- Fidelity: 18 transitions modeled, all cited file:line; 0 UNCLEAR/TODO within scope; abstractions listed in 01_reasoning.md。每个变量和动作均有源码或观察见证含义。
- Code coverage: modeled 14 of 14 selected function paths in `tickwire-execution`/`tickwire-mock`：held、record_event、terminal/apply_terminal、fill/apply_fill、fee/apply_fee、replace、NoBytes/apply_no_bytes、uncertain/not_found、ack、request_cancel、cancel_rejected、append_fact、record、restart。这是所声明输入域的设计面映射，不是运行二进制的行覆盖；合法 admission 与 recover_intent 使用 L0/L1 的入口摘要。
- Model coverage: TLC --coverage — all 18 Next actions fired in every cfg；每类事实和每类错误都有单独动作，generated 计数见 03_run_log.md。源码 anchors 为 444 行，检查 0 changed。

## 导出与后续边界

L3 可以继承本级有限域的唯一事件记账、失败原子性、未结成交预算、费用发送证据以及恢复失败关闭准入契约。L2 采用完整同步 record/restart 调用；append 与 apply 之间的进程崩溃、真实 WAL/复制、全范围整数运算、完整 Spot/Futures 保证金、费用尾账释放和恢复后重新授权留在后续实现与模型中。当前验证对象是单结算资产支出 primitive 与 Mock 编排。
