# Risk Core 实现契约

实现目录为 [`crates/tickwire-risk`](../../crates/tickwire-risk)，接口决策见 [`risk-interface.md`](../../docs/implementation/risk-interface.md)。该库是无运行时依赖的安全 Rust 计算库，禁止 `unsafe`，不调用外部存储或交易所。`serde_json = 1.0.145` 只用于测试读取已有合成向量。

## 计算与提交边界

| ID | 实现保证 | 验证位置 |
|---|---|---|
| RISK-1 | 十进制直接解析 i128；scale 0..38；额外非零小数拒绝；负数 floor/ceil 对齐数学定义 | `boundaries::decimals_preserve_precision_scale_and_minimum`、`signed_division_*` |
| RISK-2 | 加减拒绝隐式 scale 转换；跨 AssetId 资金操作拒绝；乘法先约去十进制因子，剩余溢出返回错误 | `scale_conversion_*`、`products_cancel_*` |
| RISK-3 | 预留失败保留原余额和已预留量；算术溢出另关闭增险闸门；计时器释放零 | `reservation_failures_*` |
| RISK-4 | Spot 基线的锁定覆盖必须完整；原挂单锁定不重复从 free 扣减；收到币手续费超额转为外部预留 | `baseline_requires_*`、`fees_exceeding_*` 与数值向量 |
| RISK-5 | 未确认订单全计资本；单向取两端包络；双向同时核对 BUY/SELL 数量与各侧潜在变化 | `hedge_capital_rejects_*`、`funding_counts_*` 与数值向量 |
| RISK-6 | 档位从零连续，累计维护金额连续，边界选高档；未经资格确认的系数拒绝；失效正抵押计零、负债仍扣除 | `bracket_continuity_*`、`invalid_collateral_*` |
| RISK-7 | 减仓量扣除既有分配并向下取 step；真实仓位不足拒绝；单向与双向的方向/reduceOnly 不混用 | `reduce_allocation_*` |
| RISK-8 | 交割计时器保留仓位与费用预留；收入 ID 去重；未决订单、钱包差额或参考差额未闭合时不释放；释放一次 | `settlement_timer_*`、`settlement_overflow_*` |

`Fixed::mul_to` 和 `product_to` 在指定目标 scale 舍入；多因子抵押、退出费和资金费尽量只舍入一次。`Fixed::div_to` 同样使用 checked i128 中间值，若无法在当前中间表示下计算则拒绝，即使理论上更宽整数能计算该输入，也不会降级为浮点。金额相加保留负余量，只有设计规定的可用额、亏损正部等位置截取零下界。

`ReservationAccount` 是一项资产的串行资源原语，订单 ID、事件去重、版本、日志提交及最终发送由执行器负责。`reserve_computed` 在候选计算和两项金额检查都完成后提交。失败时仅算术错误会关闭 `increase_open`，原资金状态保持不变。

`Settlement<N>` 使用固定容量数组保存一个已规范化的账户/合约/交割实例内的收入。容量不足、重复 ID 内容冲突或整数溢出均不部分入账。实际收入与参考计算分开；参考值不产生另一笔钱包入账。`reconcile` 只在完整账户事实、全部订单终止、实际持仓归零且两项差额为零时关闭该实例。已闭合后收到新的真实收入会重新进入等待对账，重复原收入保持幂等。容量不足、冲突或溢出等记录失败会锁存 `unresolved_evidence`，旧对账事实无法再次关闭实例；调用方须在恢复层取得完整事实并重建该实例。

## TDD 与实际执行记录

1. 先落接口文件及 signed division 两个测试，库入口保留明确未实现的除法。workspace 当时有尚未生成 Cargo.toml 的成员，首次 Cargo 命令因 workspace 加载失败；这次失败不算测试 red。
2. 使用 `rustc --edition=2024 --crate-name tickwire_risk --crate-type rlib crates/tickwire-risk/src/lib.rs -o /tmp/libtickwire_risk_red.rlib`，再以 `--test crates/tickwire-risk/tests/boundaries.rs --extern tickwire_risk=/tmp/libtickwire_risk_red.rlib` 编译测试并执行。实际结果 **0 passed / 2 failed**，失败点是未实现的数学除法。
3. 实现商余数除法后，`cargo test -p tickwire-risk --test boundaries` 实际结果 **2 passed**。
4. 扩展公式和向量适配器后，新增 `hedge_capital_rejects_wrong_order_direction_for_the_envelope`。单独执行该测试实际得到 **1 failed**：将开多数量对应到 SELL 订单仍返回 `Ok(Capital)`。补上 BUY 与 `open_long + close_short`、SELL 与 `open_short + close_long` 的分别相等检查后通过。
5. 独立审查发现已关闭结算实例在新收入记录失败时仍保持关闭。先加 `late_unrecordable_settlement_evidence_reopens_reconciliation`，实际结果 **1 failed**，得到 `SettledReconciled` 而预期 `AwaitingSettlement`。修复为失败只保留金额、同时重新进入等待并锁存未处理证据；容量、冲突和溢出均覆盖。
6. `cargo test -p tickwire-risk -- --nocapture` 实际结果为 **17 个边界测试通过**，以及 **21 cases / 131 expected fields** 的数值契约测试通过。向量适配器只把 formula/input 送入公开计算 API，expected 只用于最终比较。
7. `cargo test -p tickwire-risk --release` 同样通过全部 17 个边界测试及 21/131 数值契约。`cargo clippy -p tickwire-risk --all-targets -- -D warnings` 通过；`cargo fmt -p tickwire-risk` 已执行。

上述命令在仓库根目录执行，工具链通过 `PATH="$HOME/.cargo/bin:$PATH"` 发现。其余公式的现有向量在实现后接入，不能把这一轮全部 21 个向量描述为逐项完成了实现前的 red。

## 集成要求与当前边界

风险公式接受已规范化数据。调用方负责将产品模式、币种、合约到期实例、配置版本、档位/费率/抵押资格有效期和行情时间绑定到同一判断。参数缺失或失效不允许调用方填零后放行。真实费用、成交、资金费和外部活动通过执行账本去重；该库的 Spot 覆盖公式不替代覆盖票据核销状态机。

跨标的/跨池资本汇总、策略预算、逐仓划转、收费池补充与终止、未知订单身份恢复、账户/交易所影子票据核销仍需执行器组合这些计算器。`Settlement<N>` 的收入 ID 命名空间、收入归一化及事实完整性由接入/对账层证明。该库没有连接 Binance、进行实盘交易或测量端到端延迟。TLA+ 必须以最终执行状态机和本库提交边界为依据，数值测试不能替代状态机模型检查。
