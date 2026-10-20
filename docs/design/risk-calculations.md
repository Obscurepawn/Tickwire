# 风控计算与账户预留契约

本文把[主设计第 7 节](binance-ultra-low-latency.md#7-订单预留与账户状态)落实为可实现的计算规则，适用 Spot 与 USDⓈ-M 线性加密资产永续、交割合约。`tickwire-exec` 单写实际账户、订单预留和策略归属；策略进程只能提出意图。本文与[策略生命周期](strategy-lifecycle.md)使用同一订单身份、管理代次和恢复屏障。

官方材料核验日期为 2026-10-20。下文区分三类量：交易所返回的账户事实；公开公式可以重建的单项量；本系统选择的资本占用和压力上界。最后一类用于准入，**不声称复制交易所完整初始保证金或清算引擎**。真实账户资本、杠杆上限、压力参数和时效阈值必须来自部署配置；缺少参数时拒绝激活。文末和 JSON 的数字均为 `synthetic` 合成输入。

## 1. 计算输入、单位与定点规则

### 1.1 有版本的输入

每次判断绑定 `(account_epoch, ledger_seq, instrument_revision, risk_revision, fee_revision, bracket_revision, collateral_revision, valuation_revision)`。配置变更、余额/订单事实和最终发送门禁都在同一 exec 序列中执行。先计算候选状态，再原子提交预留；多个策略不得分别读同一余额后各自承诺资金。

| 输入 | 取得方式与含义 | 无效时处理 |
|---|---|---|
| Spot `free/locked`、订单、成交及佣金资产 | 账户快照、订单查询、成交记录与私有流建立可解释基线 | 不能解释覆盖关系，冻结共享资产风险域 |
| Futures 钱包、可用余额、保证金、持仓 | `/fapi/v3/account`、余额、持仓信息和账户/标的配置；必要字段缺失时补充对应接口 | 不用零值代替缺失；模式与币种不一致则拒绝激活 |
| 杠杆档位 | 账户签名 `/fapi/v1/leverageBracket`，保留 `notionalCoef`、全部档位和原始响应 | 缺失、过期、区间不连续或个性化语义未核准则禁增险 |
| 抵押资格与估值 | `exchangeInfo.assets.marginAvailable`、账户适用资格、`/fapi/v1/assetIndex` 和正式 Multi-Assets 抵押表 | 正资产不计入，负债仍全额计入；不能估值负债则整域禁增险 |
| 成交费率 | Spot `account/commission` 或逐单计算费率；Futures 用户标的佣金率、BNB 抵扣配置 | 不能建立当前费率上界则禁增险；撤单继续 |
| 标记价格、资金费时间/参数 | mark price 流/查询；永续的资金费参数及历史入账 | 超过配置最大年龄、发生跳变或日程无法界定则禁增险 |
| 交割时间、状态与结算 | 合约元数据 `contractType/deliveryDate/status`、合约条款、实际结算价格、结算费及账户入账 | 时间/条款不明则禁增险；到时或预估价格均不产生账本事实 |

Futures 账户总量在单资产模式和多资产模式下单位不同：不能将单资产模式顶层 USDT 数值当作全部 USDC 资金；多资产模式顶层聚合量按 USD 口径处理。每个结算币、全仓池和逐仓钱包均带单位和所属风险域。官方字段与接口依据见[账户目录][f-account]、[行情目录][f-market]。

### 1.2 整数表示

`QuantityAtoms(q_scale)` 的单位是基础资产，`PriceAtoms(p_scale)` 是报价资产/基础资产，`MoneyAtoms(asset, a_scale)` 是该资产，`RateAtoms(r_scale)` 是无量纲比率。scale 表示十进制小数位，不表示交易所最小步长；`stepSize/tickSize` 另行精确校验。JSON 向量统一使用 8 位小数，生产 scale 从版本化元数据选择并验证，不能把 BTC、USDT、BNB 的 atoms 相加。

所有十进制字符串直接解析成有符号 `i128`，拒绝非有限值、超长输入、非零多余小数和未知单位。计算只使用 `checked_add/sub/mul/div`。负数的 `floor_div/ceil_div` 必须分别向负无穷/正无穷取整，不能直接依赖 Rust 整数除法的向零截断；`abs(i128::MIN)` 也必须检查。`ceil_div(n,d)` 用商余数实现，禁止先算可能溢出的 `n+d-1`。

例如 `N_up(q,p,a_scale) = ceil(q_atoms × p_atoms × 10^a_scale / 10^(q_scale+p_scale))`；实现先消去十进制公共因子，剩余乘积仍须 checked。数量和价格均为 8 位、金额为 8 位时直接算 `ceil(q_atoms × p_atoms / 10^8)`。费用再以金额 atoms 乘费率 atoms 并除费率 scale。乘积溢出即 `ARITHMETIC_OVERFLOW`，拒绝候选意图并关闭受影响计算域的增险闸门，保留原预留；不截断、不饱和、不降级为浮点。

费用、债务、损失、保证金、名义金额上界向上取整；资产价值、可用额度、收到数量下界向下取整。相乘后尽量只在目标单位舍入一次，但分别收费的不同费用项目逐项向上取整后相加。下单数量向下取到 step；买价上限向下取到 tick、卖价下限向上取到 tick，使合法化不突破调用者的价格界。不能在订单拒绝后悄悄放宽价格或数量。

## 2. Spot：逐资产预留与费用

### 2.1 原子资源向量

令 `q_rem = max(0, q_orig - q_cum_fill)`，买单最坏报价支出 `D_quote = N_up(q_rem, p_buy_cap)`，卖单基础资产支出 `D_base = q_rem`。每个订单持有 `R[o,asset] = principal_debit + fee_external_debit + fee_tail`；没有预期成交收入抵扣。多个订单的同资产需求求和；不同资产分别检查。

主动吃单使用显式 `BoundedTaker` 意图，必须带最大支出/数量和滑点界。基于完整、有效深度计算可成交量和遍历总价，只作为当前流动性检查；**深度快照不构成未来成交价格上界**。`BoundedTaker` 编码成有数量和价格界的立即成交否则取消限价单（Immediate-or-Cancel，IOC），未成交量允许取消；它与原生 `MARKET` 是不同意图，不能静默替换用户要求的全部市价执行。Spot 原生 `MARKET quoteOrderQty` 的买方报价预算可限制支出，却不同时给出本系统所需的基础资产持有数量上界；原生按数量市价买单也没有固定报价支出上界。因此生产准入不把这些原生语义替换成推测的 `last_price × (1+slippage)`。确需原生市价行为时返回 `UNBOUNDED_EXECUTION`；受限风险处置另按第 6 节处理。相关订单参数见[Spot REST][s-rest]。

### 2.2 费用情景与币种

Spot 一般费、税费、特殊费分别计算。账户标的费率按 `max(maker,taker) + buyer_or_seller` 建立每类上界；逐单 `*CommissionForOrder` 已含买卖方向，不能再加 buyer/seller。折扣只适用于一般费。实际入账一律使用成交的 `commission/commissionAsset`，不反推实际收费。[官方佣金说明][s-fee]

设接收资产数量 `x`：买入为基础资产成交数量，卖出为报价资产成交额。无抵扣时每类费 `f_k = ceil_asset(x × r_k)`；这部分从收到资产扣除，未来净收入下界为 `max(0, x - Σf_k)`，不再扣一份既有库存。若费率可能大于收到额，按超出额建立外部扣款预留，无法界定时拒绝。当前收到资产在成交前不计入可用额。

BNB 等第三币种收费采用独立情景：先用压力转换价上界将费用基数换成报价金额，再除以第三币种/报价币的正下界，逐项向上取整。容量估值不依赖折扣，`fee_BNB_estimate = Σceil_BNB(fee_quote_k / BNB_quote_low)`；折扣失效已包含在该压力估值中。卖出限价只有下界，更好的成交价仍可能增加 BNB 费，因此这个估值不能单独充当第三资产硬预留。启用 BNB 抵扣时，exec 建立账户级独占收费池，预留该币种全部尚未被已知锁定覆盖的余额；所有相关订单共享这一个池，不逐单重复冻结整池，不再把池内 BNB 分配给买卖、划转或保证金。后到 BNB 收入也先加入池。要使用这些 BNB 做其他交易，须先确认交易所已禁用抵扣并闭合存活订单的收费责任。余额不足回退接收币收费的官方语义提供剩余边界；不存在这种有界回退的外部收费资产必须有交易所可执行扣款上限，否则拒绝启用。JSON 的 BNB 数字是逐单容量估值，严格可用额还须扣独占收费池。第 2.3 节资源公式中，收费池作为一条合成订单占用：其 `D` 汇总真实 BNB 扣费，`R` 随池内剩余余额更新，基线尚未冻结的池资金 `c=0`；逐单 BNB 估值不再叠加进 `R`。Futures 抵扣采用同一规则，池内币不得再计入可共享抵押价值。税费和特殊费不享受一般费折扣。若抵扣不足，交易所可能回退到接收币收费，故同时保留无抵扣的净收入下界；各币种预留取所有允许情景的逐分量最大值，不把互斥币种情景相加成实际费用。BNB 价格失效时禁用抵扣能力须有交易所配置确认，否则停止新增收费订单，不能假定自动回退。[官方佣金说明][s-fee]

除标准接收币/BNB 规则外，任何部署允许的外部收费币种都必须给出 `fee_debit_asset`、费率上界和转换价格界；引用价格未知时该情景无界，拒绝。费用存在逐笔舍入时，仅按总成交额算一次费用可能不足：为剩余 `q_rem` 设 `K_fill_max = ceil(q_rem / min_trade_quantity_quantum)`，对每个收费项目额外预留 `K_fill_max × fee_asset_quantum`。`min_trade_quantity_quantum` 必须是能界定逐笔成交最小数量的协议元数据/资格证据，不能未经验证直接等同下单 LOT_SIZE；缺少该界时要求持久的最大可收费用包络，否则禁增险。确认的每笔费从尾部缓冲核销；终态后已成交但费用未覆盖的 `fee_tail` 独立保留到对账闭合。

### 2.3 free/locked 不重复扣减

建立基线 `B0=(free0, locked0, covered_ledger_seq, covered_fills, covered_orders)`，不能用本机接收时间推测 REST 快照包含哪些成交。令 `T0=free0+locked0`，`K` 为已解释的非交易锁定，`T = T0 + Σ基线后真实现金流`，本地可分配量为：

```text
SPOT_LOCAL_FREE: local_free[a] = T[a] - K[a] - Σ_o R[o,a]
```

这里已经包含交易所锁定，**不再从 free0 中扣全部 R**。基线中每个订单的已证明冻结覆盖量为 `c[o,a]`，满足 `Σc + K = locked0`；无法分配的 locked 进入 `K_unattributed` 且冻结增险，不用它抵扣任意订单预留。

额外的交易所 free 上限按同一基线推进。`D[o,a]` 是该订单基线后的实际支出，`C[a]` 是已证明没有被基线包含的净接收/释放，`D_other[a]` 是其他实际支出：

```text
SPOT_VENUE_CAP: venue_cap[a] = free0[a] + C[a] - D_other[a]
                               - Σ_o max(0, D[o,a] + R[o,a] - c[o,a])
SPOT_AVAILABLE: available[a] = max(0, min(local_free[a], venue_cap[a]))
```

订单的原冻结资金只能覆盖它自己的已花费和剩余预留。新增订单 `c=0`，立即减少可用量；旧单锁定已含在 `locked0`，不再重复扣 free。若终态和完整累计成交证明释放原锁定，`C` 才增加 `max(0,c-D-R)`；这项释放只记一次并关闭对应覆盖票据；归档时同时处理该票据的 `D/R/c` 覆盖汇总，不能仅清零 c 后仍把 D 当成未覆盖扣款。成交收入按去重的真实净现金流加入 `T` 和 `C`，不是按计划买卖额加入。尚未知道费用时保留 fee_tail。对上述两条式子取最小值，即使部分覆盖票据处理滞后也只会少给资金。

部分成交原子完成：现金流入账、累计成交增加、`q_rem` 减少、费用核销、剩余预留重算。因实际买价低于限价而减少的原本金上界可释放；实际支出不会“释放成”可用资金。CancelPending、超时、策略换代、父条件单消失均不触发释放。新的快照只有在订单/成交/现金流覆盖关系闭合后才能替换基线，且同步重置覆盖票据、账本偏移和未覆盖预留。

## 3. Futures：估值、档位与资本占用

### 3.1 仓位、模式和压力区间

单向模式实际仓位 `q` 有符号，正为多，负为空；双向模式保存非负 `q_LONG/q_SHORT` 两个独立仓位。全仓按共享保证金池核算；逐仓按交易所实际独立钱包键核算，不预设 LONG/SHORT 必然拥有互相隔离的钱包。单资产 USDT、USDC 池互不借用；多资产模式只按已验证的全仓能力激活。Portfolio Margin 与 COIN-M 不在此计算器内，发现耦合的未覆盖持仓/订单即拒绝域激活。

对每个标的定义 `P_low=floor_tick(mark × (1-down_shock)) > 0`，`P_high=ceil_tick(mark × (1+up_shock))`。冲击比例来自风险配置；标记价与指数价偏差超出配置阈值则禁增险。这是本系统压力区间，不是行情不会越界的承诺。市场越界、抵押贬值、费率上调或已有订单价格界不能覆盖当前估值时，更新真实缺口、关闭增险并执行受限处置，不能裁剪行情来保持额度充足。

### 3.2 最坏未决敞口

在每个真实订单上仅计算未成交量。普通单、尚未发送但已接纳意图、MayHaveSent/Unknown、CancelPending，以及条件结构可能产生的子单都进入集合。对单向模式，令 `B/S` 是所有可能买/卖的剩余数量，包括方向相反且尚无确认的腿：

```text
ONE_WAY_ENVELOPE: q_min = q - S; q_max = q + B
                 q_abs_max = max(abs(q_min), abs(q_max))
                 q_capital = abs(q) + B + S
```

端点已覆盖 `[0,B]×[0,S]` 的任意成交组合；不能仅检查“全部成交后的净值”。`q_abs_max` 用于账户净仓限额；`q_capital` 故意不抵扣任何未来成交，用于本系统总额资本占用。已获严格减仓分配的订单也保留在 `q_capital`、执行费和成交损失缓冲中，直至确认成交后以新实际仓位重算。

双向模式按 LONG/SHORT 分别计算潜在增量，`q_LONG_max=q_LONG+ΣBUY_LONG`、`q_SHORT_max=q_SHORT+ΣSELL_SHORT`；资本数量采用 `q_LONG+q_SHORT+Σ全部未决数量`。平仓单不提前降低实际多/空量。最坏净敞口还要比较 `q_LONG_max - max(0,q_SHORT-close_SHORT)` 与 `max(0,q_LONG-close_LONG)-q_SHORT_max`；账户净额和多空各自总额均检查。

### 3.3 档位、维持保证金与 notionalCoef

初始保证金（Initial Margin，IM）是开仓占用资本；维持保证金（Maintenance Margin，MM）是持仓持续所需的最低保证金。公开的线性合约单项公式为 `MM = notional × maintMarginRatio - maintenanceAmount`；本地用档位 `cum` 作为经过核准的 maintenanceAmount。它不提供整个账户清算价。[官方保证金说明][f-margin]

对已核准的有效档位 `(floor_j,cap_j,rate_j,cum_j,max_leverage_j)`，按当前压力名义额重新选档；区间使用 `[floor,cap)`，最后一档包含上限，恰在边界选较高档。`MM_TIER(N)=max(0,ceil(N×rate_j-cum_j))`。验证首档从零起、区间连续、费率非降、`cum` 连续性与响应精度；使用账户特定数据，不拷贝网页示例。

`notionalCoef` 官方仅说明为用户标的档位乘数，未在该接口完整说明返回区间与 `cum` 是否已经变换。适配器必须保存 `bracket_interpretation=effective_as_returned` 或有资格证据的具体变换规则；不得对返回 cap/floor 再盲乘一次。存在非 1 系数且无法证明语义时，`BRACKET_SEMANTICS_UNVERIFIED` 禁增险。控制面可用官方针对该账户的说明与账户保证金/档位边界样本确认后发布有版本解释；运行时无需猜测。任何账户实测与重建量不匹配均失效该解释。[账户档位接口][f-account]

本系统对 `N_capital=ceil(q_capital×P_high)` 使用更严格的 `MM_upper=ceil(N_capital×max_{k≤j}(rate_k))`，即不抵扣 `cum`；这是对已验证非负 cum 公式的上界。双向数量按两侧相加后选档，即使交易所存在对冲减免，本系统也不提前使用。`MM_TIER` 留作核对、报警和数值向量，准入使用 `MM_upper`。

### 3.4 可直接执行的资本公式

`L_eff=min(configured_capital_leverage, venue_symbol_leverage, tier.max_initial_leverage)`，三者都必须已确认且不小于 1。`IM_charge=ceil(N_capital/L_eff)` 是本系统总额资本规则，**不等于交易所的组合 IM**；风险配置可以取更低杠杆。

当前仓位压力损失逐仓/方向计算并求和：单向模式 `q≥0` 时，`loss_pos=ceil(max(0,q×(entry-P_low)))`；`q<0` 时，`loss_pos=ceil(max(0,abs(q)×(P_high-entry)))`。双向模式的 `q_LONG/q_SHORT` 均为非负数量，分别代入多仓/空仓公式后求和；不能把有符号负仓位直接乘入空仓损失。无成本基线时不能用零 entry。已实现盈亏使用账本事实；当前仓位只使用仍未实现部分，避免已实现亏损扣两次。

每个未决订单独立计最大成交后损失：买单 `loss_order=ceil(q_rem×max(0,p_buy_cap-P_low))`，卖单 `ceil(q_rem×max(0,P_high-p_sell_floor))`；同一个买卖循环两边都计，故不能靠净仓为零漏掉低卖高买损失。所有可能成交订单的执行费按最大适用费率计入；买单金额用买价上限，卖单在当前压力模型内用 `max(P_high,p_sell_floor)` 估值，不能误用卖价下限当费用上界。卖单优价超出区间时按真实费用追加并触发重估；该费用模型属于本系统压力约束。手续费币种另作资源预留，USD 等值只计一次。退出缓冲覆盖当前仓位以及全部潜在成交后的仓位：单向使用 `q_exit=q_abs_max`，双向使用 `q_exit=q_LONG_max+q_SHORT_max`，`exit_fee_reserve=ceil(q_exit×P_high×exit_fee_rate_upper)`。已有平仓单不提前释放退出缓冲；新开仓在成交前就承担其未来退出费用。这样，开仓成交后执行费转入钱包扣款、未决数量转成实际持仓，不会凭空新增一份未预留的退出费用。

```text
FUTURES_CAPITAL:
  capital = Σ_pool_symbol max(IM_charge, MM_upper + mm_addon)
            + Σloss_pos + Σloss_order + execution_fee_reserve
            + exit_fee_reserve + funding_reserve + pending_outbound + capital_buffer
  local_headroom = collateral_value - capital
```

`mm_addon/capital_buffer` 是非负配置金额或版本化公式，不能取未声明的默认值。`collateral_value` 使用钱包抵押本体，**不加正未实现收益**；负未实现部分已由 `loss_pos` 压力覆盖。公式所有项都按相同计价单位转换。本公式选择在压力区间内分别最大化各项再相加，可能对应不同市场情景，仍构成保守组合；不需要在热路径枚举指数级的成交排列。

当前本地余量和交易所可用余额必须同时覆盖新请求。设已闭合的交易所快照可用量为 `A0`；对其尚未覆盖的每个已接纳请求建立影子扣款 `H_i=max(0,capital_after-capital_before)`，同时记录现金费用/资金费等不在该 H 内的真实扣款 `D_uncovered`。市场变化造成的额外资本需求累计为 `H_market`，不因一次反弹自动归还。`A_shadow=max(0,A0-ΣH_i-D_uncovered-H_market)`；新请求须满足 `delta_capital≤A_shadow` 且 `local_headroom_after≥0`，并检查全部净额/总额/策略限额。

`H_i` 覆盖了预估费用时，后续实际费用先在该 H 的费用分项核销，不能在 `D_uncovered` 再扣同一笔；超出部分才追加。订单成交、逐仓划拨和减少本地资本不自动释放影子额度；仅证明请求完全未产生经济效果，或新的交易所基线明确覆盖该请求和全部现金流后才能核销。基线包含的既有挂单保证金不再全量从 A0 扣一次。快照刷新由控制面异步批次完成，逐笔发送不发 REST 请求；影子额度耗尽时等待下一闭合基线，不能自行把快照 availableBalance 加回。官方没有完整组合 IM 公式，因此交易所拒单仍是合法业务结果，不通过重试或换 ID 突破本地限额。

### 3.5 多资产估值和折扣

抵押价值折扣（haircut）在本文表示扣减比例；`h=1-haircut` 是可计入比例。不可将 `assetIndex.bidBuffer` 或 `autoAssetExchange` 当成 h。控制面从官方 Multi-Assets 抵押表取得资产、适用 VIP/额度档、比例、有效时间和内容摘要，结合账户资格生成 `CollateralSchedule`；没有可靠机器接口时允许运维签名导入官方表，记录出处、抓取时间、适用账户和过期时间，不抓未知网页值自动放行。该配置是可交付输入契约，过期即失效。公开资产表入口见[Multi-Assets 官方说明][f-multi]。

对资产 a，`v_low=min(index,bidRate,autoExchangeBidRate)×(1-collateral_down_shock)`，`v_high=max(index,askRate,autoExchangeAskRate)×(1+liability_up_shock)`，所有使用字段都须正且新鲜；未启用自动兑换也可保留更低估值。本系统保守转换不声称这些 rate 就是保证金折扣。[资产指数接口][f-market]

若抵押表有额度档，按表规定的资产数量或价值单位分段，`collateral_positive=Σfloor(slice_quantity×v_low×min(h_venue_tier,h_policy))`；不得先用全部余额乘最高一档。负余额为 `-ceil(abs(balance)×v_high)`，不享受 haircut。`COLLATERAL_VALUE` 为所有资产正抵押价值减负债，再扣实际锁定于其他不可共享域的金额。缺失比例/资格的正资产计零；缺失负债转换价直接禁增险。价格失效时正资产计零并立即重估，过期抵押表不能仅靠账户 availableBalance 新鲜就继续使用。

全仓在池内汇总以上资本与抵押价值；逐仓只用对应 `isolatedWallet` 和已确认逐仓现金流，不能重复计入 cross 钱包。增拨逐仓保证金先在共享来源预留，来源扣款确认后仍不增加逐仓可用量，目标钱包确认后才入账；提回保证金反向执行。自动补保证金若开启，必须把最大可吸取来源预算作为持续预留；没有可执行上限则该组合不得激活。模式切换、杠杆调整和自动补保证金配置均是 exec 管理的风险动作，收到确认并重新建基线前不按新配置交易。

### 3.6 线性交割合约：到期与结算

交割合约使用本节相同的线性数量、保证金、仓位模式、压力损失和策略归属公式；启用前必须确认 `contractType`、合约到期实例、`deliveryDate`、状态、结算资产与单位。`pair` 相同但到期实例不同的仓位不能合并，不能仅依赖连续合约 K 线或同名 ticker 建立身份。永续的资金费周期不应用于交割合约：经合约资格确认的交割模式使用 `funding_mode=NONE`、`funding_reserve=0`；这不豁免到期结算费。USDⓈ-M 的结算资产和永续/季度区别见[官方合约规格][f-spec]。

部署策略指定 `expiry_no_increase_window_ms`；参考技术配置为 1800000 毫秒，即 30 分钟。这是本系统提前停增险的窗口，不是交易所接受订单的截止承诺。exec 在 `now + clock_uncertainty >= deliveryDate - expiry_no_increase_window` 时单调进入 `ExpiryReduceOnly`，拒绝新增开仓、增险改单和可能在窗口后增加仓位的条件结构，执行已授权的普通/条件挂单取消，并保留撤单结果未明的全部预留。交易所更早收紧状态时立即跟随；本地较早的闸门不因交易所暂仍可交易而自动放开。

交割前把退出费用替换为 `ceil(q_exit × P_high × max(exit_fee_rate_upper, settlement_fee_rate_upper)) + settlement_rounding_buffer`，覆盖主动退出或持有至结算两种可能。其余未决订单执行费、可能亏损和本金占用继续保留。USDT 季度合约规则规定到期结算费等同 taker 费，适配器据此绑定该账户、该合约当前有效的 taker 费率；其他结算资产或条款变体仍须单独核准，不从产品名称推定。[USDT 季度规则][f-usdt-quarter] 结算费率及收费币种必须来自该账户、该线性合约的有效条款/费率资料；不能直接把其他产品示例费率或币本位倒数价格公式套到 USDⓈ-M。费率未知或过期时禁增险，已有预留不因时间到期被清零。官方交割说明确认结算涉及平仓、盈亏入账和费用，也说明特殊市场状况下可延期；其中币本位算例不作为本文线性核算公式。[官方交割说明][f-delivery]

`estimatedSettlePrice` 仅用于监测和压力重估，不能记为最终结算价格、利润或可用资金。到期后从 `/futures/data/delivery-price?pair=...` 取得与合约到期实例匹配的 `deliveryTime/deliveryPrice`，该公开行情是核对输入；实际持仓变化、结算盈亏、费用及钱包变动仍以账户事实为准。不能仅因公开价格已发布而释放账户保证金。[结算价格与预估字段][f-market]

exec 进入 `AwaitingSettlement` 后，按以下顺序闭合事实：收到并去重实际结算/收入/费用记录；核对结算前数量、成本、实际价格和收费币种；确认普通订单、algo 及其子单均终止且成交覆盖完整；确认实际持仓变化与钱包新基线。线性核对量为 `reference_pnl = q_signed × (actual_settlement_price - entry)`，`reference_fee = ceil(abs(q_signed) × actual_settlement_price × qualified_settlement_fee_rate)`。这些量只检查单位、符号和差额，**不作为第二份入账来源**；实际费用或盈亏与参考不符时按真实记录入账、差额进入 `unattributed_settlement`，相关范围继续冻结直至解释完成。实际记录可能合并盈亏与费用，适配器必须先归一化且证明净额覆盖，不能再减一次已经包含的费用。

结算超时、到期时间跨过、公开价格更新、合约停止交易以及余额单独变化，都不能自行把持仓设为零或释放未决订单/保证金。延期时保持 `AwaitingSettlement` 和预留，持续更新标记价、费用与账户事实；有效新公告和元数据可修改预期时间，但不自动重开增险闸门。换月必须是独立授权的跨合约操作，按两个真实订单、独立费用和未对冲风险检查；旧合约到期不自动创建下一期仓位，也不移植旧单身份或策略成本。

## 4. 撤改、条件单与未知状态

| 状态/动作 | 计入算法 |
|---|---|
| 部分成交 | 已成交进入真实现金流/仓位；仅剩余量进入订单集合；未知费用保留尾部缓冲 |
| CancelPending、请求超时 | 原剩余量全留；不设超时释放 |
| Spot cancelReplace | 旧单剩余与新单全部求和；只有两部分权威结果分别闭合后才减量 |
| amend keep priority 减量 | 结果未知保留旧量；成功后按权威新累计/剩余量调整 |
| Futures 同 orderId 修改 | 已确认累计成交去重；未知修订对每个金额/数量维度取旧、新两种可能状态的最大值；若协议或身份不能排除两个独立订单，则按求和 |
| 已知固定数量条件单 | 触发前即按全部潜在子单占额；STOP_LIMIT 使用限价界，触发市价增险单无执行价界则拒绝 |
| 条件父单与子单 | 已证明同一经济订单且绑定累计成交时，只计一次剩余；关联未闭合则分别占额 |
| Spot OCO/OTO/OTOCO 等 | 所有可达子单的潜在资源相加，不依赖未来腿的收益；需要借已买资产才能挂出的结构须由适配器证明交易所资源依赖，否则拒绝 |
| `closePosition=true` | 动态数量上界取相关真实仓位加所有已接纳潜在开仓量；新开仓会同步扩大其费用/执行缓冲。无法建立有界闭仓数量、模式或子单关联时禁用该结构 |
| 可枚举 Unknown | 保留发送前最大量、所有未解决修订和费用包络；冻结其可影响资产/保证金池增险 |
| 缺失身份或无法枚举 Unknown | 整个共享风险域冻结；不得只冻结一个策略后借用其余额 |

同一条件结构可以选择比交易所实际互斥关系更宽的集合；这种保守求和有明确上界。不得以“条件未触发”“OCO 会撤另一边”或“自成交保护会阻止成交”提前减掉资源。结构分支数量、子单数量、深度和未决修订必须有部署容量上限；超过上限拒绝新结构，已有未知结构占额并冻结，不丢弃分支。

Futures 普通订单和 algo 查询/取消是独立接口域，`algoId` 与触发后的订单 ID 分别保留；主网接口能力依据[交易目录][f-trade]。准入时全部潜在分支纳入，触发只转换身份和已覆盖资源，不重新领取一份预算。

## 5. 策略总额、账户净额与资金归属

对单向合约，`q_account=Σ_s q_strategy[s]+q_unattributed`；双向按 positionSide 分别守恒。Spot 每个资产数量及费用也满足同样归属守恒。`TransferAttribution` 原子改归属，不改变交易所仓位或账户资金；generation 切换不释放额度。

账户净敞口使用第 3.2 节最坏区间，策略限额使用各自虚拟仓位与自己全部可能订单。另算 `strategy_gross=Σ_s abs(q_strategy[s])×P_high`，不因跨策略抵消缩小；策略资本配额基于同样的不抵扣未来成交公式。账户准入须同时满足实际资本、账户净/总额和每个策略预算。策略预算是限制，不是额外的钱包余额。

例如策略 A 为 +5、B 为 -3，账户实际 +2。A 卖 5 是减少自己的虚拟多仓，却会让账户变为 -3，因此属于账户增险判断；不能只写 `reduce=true`。成交归 A 后 A=0、B=-3，账户=-3，归属仍守恒。未归属费用、资金费和外部持仓不能随意塞给任一策略以通过限额。

## 6. 减仓分配与停机处置

`REDUCE_ALLOCATION` 按 exec 接纳序列分配真实可减数量：单向多仓只能 SELL，空仓只能 BUY，`reducible=max(0,abs(q_actual)-Σ未决减仓分配)`；逐笔分配 `min(requested,reducible)` 并向下取 step，少于请求时按意图声明的 `allow_partial_reduce` 返回部分量，否则拒绝。单向发送 `positionSide=BOTH, reduceOnly=true`；双向平多 `SELL+LONG`、平空 `BUY+SHORT`，不发送 reduceOnly。双向各侧独立分配，交易所可能因其他挂单拒绝，即使本地数量已足够也不扩大或重试为开仓。[新单参数][f-trade]

分配票据绑定订单、positionSide、仓位版本；CancelPending/Unknown 持续占用。成交同时减少实际仓位和该票据剩余，外部平仓/强平使分配超过实际量时立刻禁止进一步发出、撤销能撤的剩余单并对账。不能把预期新增仓位先作为普通减仓票据来源。

停机允许的 ReduceOnly 动作必须对当前最坏敞口集合重新证明不扩大账户净额/总额；有其他可能反转仓位的非减仓订单时先保留其包络并验证，不能仅凭当前 q 判断。净额减少仍可能消耗手续费、实现亏损或抽走对冲；若无法证明资本不恶化，使用显式 `ExecutorRiskAction` 处置预算和权限，不能伪装成无需资本的普通减仓。数据失效时取消仍可执行；价格无界的原生市价处置须来自明确的应急授权并记录无法保证的成本界，不适用正常增险公式，也不能据此恢复风险闸门。

## 7. 资金费、外部活动与判定结果

永续合约的资金费预留使用未来覆盖时段内可能发生的结算次数 `n`，按当前公布的最短适用间隔和日程计算；`rate_abs_upper=max(abs(published_cap),abs(published_floor),policy_rate_bound)`。`FUNDING_RESERVE=ceil(N_capital×rate_abs_upper×n)`，多空各自收费上界求和，不预借应收资金费。参数/结算日程变化立即重算；未知是否跨过结算点时将该次计入。覆盖时段必须覆盖订单最长存活与恢复处置时间，长期订单到期前刷新时段；不能在等待 Unknown 时让时间预算自然归零。官方资金费参数与标记价格入口见[行情目录][f-market]。

实际资金费按 income/账户事实去重入账并消耗对应缓冲。可确定结算时点的持仓、标记价和费率时，先按各策略当时的有符号仓位计算理论份额；向零舍入各份额后，实际账户金额与理论总和的差只允许在可证明的舍入界 `settlement_atom × (strategy_component_count + venue_income_component_count)` 内。界内残差按固定策略 ID 顺序逐 atom 分配，最终策略和严格等于交易所实际金额；界外差额不伪装成舍入。若差额超界或持仓覆盖不完整，整笔进入 `unattributed_funding`，总账仍即时扣款且冻结相关归属增险，待事实闭合后重新归属。

手工订单、其他程序、充值/提现、划转、自动兑换、强平和自动减仓都可能改变账户。入金/正调整在来源与归属闭合前不增加可用额度；出金/负调整立即扣账；发现绕过 exec 的主动交易关闭共享域增险并查询全部普通/条件订单。只观察后到回报无法提供并发资金保证。资金费、强平等交易所自主动作不因“外部”而丢弃，应触发仓位/票据/归属重估。

判定返回 `Accept(reservation_id, state_digest, charges)` 或稳定原因码，包括 `INSUFFICIENT_ASSET`、`INSUFFICIENT_COLLATERAL`、`VENUE_AVAILABLE_CAP`、`STRATEGY_LIMIT`、`UNKNOWN_EXPOSURE`、`STALE_VALUATION`、`BRACKET_SEMANTICS_UNVERIFIED`、`UNBOUNDED_EXECUTION`、`ARITHMETIC_OVERFLOW`。计算的输入版本、候选前后占用、币种、舍入结果和拒绝原因进入审计记录；不记录凭证。

## 8. 数值契约与验收

机器可读向量位于 [`risk-vectors-v1.json`](../../specs/contracts/risk-vectors-v1.json)。它定义公式名、合成输入和精确预期；所有金融数值用十进制字符串，不经 JSON 浮点。下面列出主要检查点，完整字段以 JSON 为准。

| 向量 | 输入摘要 | 预期 |
|---|---|---|
| `spot_partial_fill` | 原买 10×100；成交 4×99 | 支出 396，剩余预留 600，上界减少 4 |
| `spot_received_fee` | 买入 2；三类费率 0.001/0.0002/0.0003 | 接收币费用 0.003，净接收 1.997 |
| `spot_third_asset_fee` | 报价基数 20000；费率 0.001/0.0001；BNB 下界 200 | BNB 预留 0.11；退出抵扣情景收到币净额 19978 |
| `spot_baseline_dedup` | free=600、locked=400，旧单 R=400，新单 R=200 | 总资产 1000，可用 400，不重复扣旧冻结 |
| `one_way_opposing_orders` | q=2、买3、卖6、压力高价110 | 净仓区间 [-4,5]，资本数量11，名义额1210 |
| `hedge_mode` | 多4、空3、开多2、开空5、平多1、平空2 | 多上界6、空上界8、资本数量17 |
| `tier_crossing` | 50000 档位界；N 从49000增至51000 | MM 从196到208；准入 MM_upper=408 |
| `collateral_haircut` | BTC 0.1×20000×0.9；USDT 1000；USDC 负100×1.01 | 抵押价值2699；BTC 比例失效后899 |
| `unknown_cancel_replace_algo` | 旧单剩4×100、新单5×101、潜在子单2×110 | 报价预留1125，未知状态不释放 |
| `shared_funds` | 共享可用1000，两个策略各申请600 | 第一笔接纳，第二笔拒绝，余400 |
| `reduce_allocation` | 真多仓5，先分配3，再请求4且允许部分 | 第二笔仅2，再无可减量 |
| `fixed_rounding_and_overflow` | 微量乘积、负数除法、超 i128 乘积 | 向上/向下不同，溢出稳定拒绝 |
| `capital_with_funding` | 合成资本、损失、费率、两次资金费 | 潜在最大仓位5，退出费0.55，资本458.767，余541.233 |
| `negative_short_loss` | q=-1、entry=100、压力高价110 | 空仓压力损失10 |
| `post_fill_exit_fee` | 空仓买1×90、压力高价110、杠杆10、执行/退出费率0.001 | 成交前需11.20，钱包11.09拒绝；假设成交后钱包11，仍需11.11 |
| `strategy_netting` | 策略+5/-3，实际+2，第一策略卖5 | 实际变-3，必须账户增险判断 |
| `venue_shadow_cap` | A0=500，未覆盖H=200，其他扣款20，市场追加50 | 上限230，新增231拒绝 |
| `linear_delivery_settlement` | 多2 BTC、成本100 USDT/BTC，实际结算110，费率0.001 | 参考盈亏20 USDT、费用0.22、实际净入账19.78；到期计时器释放0 |

验收必须覆盖：所有向量逐字段相等；同资产不同 scale 的显式转换；重复成交不改变账本；终态迟到费保留尾部；档位/折扣过期立即失效；基线覆盖票据只能核销一次；所有 checked 失败不改变原账户状态。这里只交付设计和数值契约，不表示已经实现或测试交易引擎。

## 9. 官方依据

- [Spot REST：订单、账户与撤改][s-rest]，固定官方提交 `f0ee79c8ce24dbeda6645211b9504e237fddef19`。
- [Spot Commission FAQ：三类费用、接收币与 BNB 情景][s-fee]，同一固定提交。
- [USDⓈ-M Account：账户、佣金、配置与账户档位][f-account]。
- [USDⓈ-M Market Data：exchangeInfo、mark price、fundingInfo、assetIndex、delivery-price][f-market]。
- [USDⓈ-M 合约规格：结算资产、永续与季度类型][f-spec]。
- [USDT 季度合约规则：线性结算与结算费等同 taker 费][f-usdt-quarter]。
- [季度交割说明：结算费用、账户入账与延期][f-delivery]；其中币本位公式不能作为 USDⓈ-M 线性结算公式。
- [USDⓈ-M Trade：仓位模式、普通订单、algo 与逐仓接口][f-trade]。
- [Leverage and Margin：公开维持保证金公式][f-margin]。
- [Multi-Assets Mode：抵押资格、官方抵押表入口][f-multi]。本轮未取得该动态表的真实账户适用比例，因此没有把示例 h 当成生产配置。

[s-rest]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/rest-api.md
[s-fee]: https://github.com/binance/binance-spot-api-docs/blob/f0ee79c8ce24dbeda6645211b9504e237fddef19/faqs/commission_faq.md
[f-account]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account
[f-market]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data
[f-trade]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade
[f-margin]: https://www.binance.com/en/support/faq/detail/360033162192
[f-multi]: https://www.binance.com/en/support/faq/detail/e18fac92defe4a118b4ed8804ae7d6fd

[f-spec]: https://www.binance.com/en-AE/support/faq/detail/360033161972
[f-delivery]: https://www.binance.com/en-AU/support/faq/detail/a3401595e1734084959c61491bc0dbe3
[f-usdt-quarter]: https://www.binance.com/en-AE/support/faq/detail/3ae441db4ae740e19af3fe9228eb6619
