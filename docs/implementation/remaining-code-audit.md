# 核心功能剩余代码清单

以下按[主设计实施阶段](../design/binance-ultra-low-latency.md#14-实施依赖与生产退出条件)核对实际入口。独立库、已接入进程的功能和组合验证分别记录；接口中拟定的类型不算已实现代码。

## 行情、交易与账户

| 范围 | 已实现 | 剩余代码 |
|---|---|---|
| P0—P1 Spot SBE | 固定官方 XML、生成常量、四类模板 codec；Binary diff 经真实 WSS、REST 快照、DepthSync、MarketPublisher、arena 和 MD 配置接入；Linux mapped 发布测试已通过 | Trade、BestBidAsk 和 depth20 已有 codec，但各自的独立业务 owner、规范发布及订阅接线尚缺；depth20 不能替代完整初始化快照 |
| P1 其他行情 | Spot/USD-M JSON 深度、独立多标的 MD、认证 JOIN、完整盘口与 coverage、SHM 消费和 bank 轮换已有代码 | JSON Trade、独立 bookTicker，以及 USD-M markPrice/funding 行情 owner；标记价格/资金费率行情与账户实际资金费入账是不同输入 |
| P2 订单传输 | 真实 REST encoder/owner、可信时钟、共享额度、WAL 和首次输出门禁已接入；已有 WS 请求编码、FIX 签名原文及文本分帧 | WS 交易会话、持久请求关联与响应恢复；FIX 登录、会话序号恢复和业务消息 codec。已有编码不能替代完整连接器 |
| P2 条件单与撤改 | SpotList/peg/SOR、Amend/Modify、Algo 请求编码；账户对部分有价格上界的列表/条件单参数有准入计算 | List/Algo 父子结构的持久生命周期、撤销与恢复对账；Amend/Modify/peg/SOR 的账户风险及结果处理。当前普通 Cancel/Query 可走非增险准入，CancelList/CancelAlgo 在账户执行分支仍拒绝 |
| P2 USD-M 账户输入 | 独立 `CanonicalAccountInput`；稀疏 ACCOUNT_UPDATE、收入查询与精确去重、资金费入账、PnL/手续费与本地 Fill 关联；输入 WAL、账户标签 12/13、恢复和复制接线 | 新资金费及未解释余额/仓位观察的对账闭合；观察与明确收入键集合的关联审批；外部成交、强平、ADL、外部调整、交割结算和资金费策略归属。`ExternalTrade` 已有规范编码，账户 reducer 仍拒绝 |
| P2 收入基线 | `IncomeReader/IncomeScan` 的有界查询及饱和区间拆分；真实查询和三快照证据；专用 risk-admin 审批、输入/账户 WAL 安装与重放；app 初始导入扫描和审批文件调度 | 已有基线后的持续收入扫描与闭合调度；拟定标签 14 `AccountInputsReconciled` 的验证和持久转换；关联批准服务。当前导入不自动清除后续未知收入或观察责任 |
| P2 风险模式 | OneWay/Hedge、单结算币、Cross 非零仓位核对，以及固定压力价格/杠杆/档位的风险计算 | 多资产抵押资格与估值的生产接线；逐仓钱包核对和划转；动态风险参数、标记价格资本重估、交割与换月处理 |

依据与具体边界：

- [SBE 规格](../../specs/implementation/binance-sbe.md)及[Linux 修复与实测](../validation/binance-sbe/linux-fixes/README.md)已覆盖真实 Binary WSS→规范盘口→pending JOIN gate→SHM arena 借用读取。不能继续把 mapped 发布集成列为未实现。[MD owner](../../crates/tickwire-md/src/engine.rs)通过 `sbe_api_key_file` 选择 SBE，JSON 分支保留。
- [账户准入](../../crates/tickwire-account/src/admission.rs)当前只批准 Base 数量且有硬价格界的 Limit/StopLimit/TakeProfitLimit 等明确组合。原生 Market/Quote 数量的拒绝是风险边界；接入时需建立实际最坏执行责任，不能用最新价乘任意滑点放行。
- [私有输入 owner](../../crates/tickwire-binance-account/src/runtime.rs)已将 USD-M 账户消息绑定为规范账户观察，其他未接入的非 Order 消息仍隔离。[规范账户输入](../../crates/tickwire-venue/src/account.rs)独立于订单 `CanonicalEvent`，因此旧订单枚举没有资金费分支不代表资金费输入不存在。
- [USD-M 账户规格](../../specs/implementation/usdm-account-facts.md)与[收入基线导入](income-baseline-import-interface.md)对应已实现的前两阶段。[收入闭合接口](income-reconciliation-interface.md)仍是后续方案：`ACCOUNT_UPDATE.bc` 不直接生成现金流，同金额/同时间也不自动证明观察与收入唯一关联。未知正额继续隔离，未解释负额继续占用并冻结。
- [远端快照核对](../../crates/tickwire-binance-account/src/reconciliation/snapshot.rs)仍要求条件订单结构为空、Cross 且非多资产模式；[部署产品类型](../../crates/tickwire-app/src/config.rs)已含 Spot、USD-M 永续和交割，产品枚举本身不代表上述全部账户行为已经完成。

## 配置、发布与接管

| 范围 | 已实现 | 剩余代码 |
|---|---|---|
| 统一配置候选 | `tickwire-config` 的严格 TOML/JSON、规范摘要、结构/静态资源校验、权限投影、完整路径变更分类和账户作用域授权 | 真实部署/参数 schema/设备/退休资源校验；固定目标和当前 effective 绑定的计划器；配置业务 reducer、ETag/幂等提交、HTTPS 控制 API；desired/effective/last-good 物化 agent 与各运行时配置应用收据 |
| 控制共识 | `tickwire-control-raft` 的三 voter 持久日志、Ready/HardState/LightReady 同步顺序、本任期 barrier 和 Safe ReadIndex；真实文件节点测试 | 认证网络服务、业务 reducer 接线、snapshot 转移、日志压缩与成员替换。当前独立库未接成配置、发布和授权的统一控制服务 |
| P3 策略加载与交接 | 动态 ShadowLoader、真实 child 认证、完整账户报告、检查点 challenge、Prepare/Commit/Release、账户管理权 WAL、真实游标退订和旧进程退出；连续操作及部分 Released 恢复逻辑 | 多执行域实际进程组合及跨阶段恢复覆盖仍需完成；协调器与统一共识控制服务接线。Released 与全域资源清理完成的 Finalized 仍需分别实现/确认，不能以单次交接成功代替 |
| P3 发布预检与恢复 | schema2 发布三根、实际 binary/Bootstrap/恢复模板及依赖核验；ObserveOnly/FixedDemo 实际恢复或迁移；独立 rollback probe、同步回执、HandoverSeal v2、本地与镜像共同验证 | 泛型策略从固定来源起点到当前切点的完整合并输入尾部重放；实际 DeclaredRebuild 工厂和预热；其他业务 schema/订单能力适配。当前内置工厂要求 Prepare 静默后的业务状态与固定来源完全相同，变化便拒绝 |
| P4 数据复制与恢复 | 实际 mTLS 镜像进程、有界后台复制、两种固定提交模式、连续账户/输入前缀与完整恢复闭包；app 的 `seal`/`restore`、写者锁、恢复标记和冻结恢复入口 | 真实故障隔离后端与控制编排、备机持续预热及自动接管授权；与统一控制多数派和配置发布的连接。离线封口及恢复工具不等于完整自动主备服务 |
| P4 guardian | 独立可执行程序、可信时间、独立凭据、查询/撤单白名单、原请求和响应审计 WAL、隔离/应急签名核验 | 实际隔离设备动作与回读；分布式配额控制服务及审计复制。当前 CLI 的外部额度分配存储仍为显式 `MockQuotaStore`，审计复制能力未接入 |

[配置接口](configuration-interface.md)描述的计划、物化和完整成功收据尚未由现有候选库完成；[配置规格](../../specs/implementation/config-candidate.md)列出了实际已实现校验。[Raft 规格](../../specs/implementation/control-raft.md)只保证其持久共识边界，不替上层签发业务成功。

[发布预检接口](release-preflight-interface.md)明确固定迁移来源和不断刷新的 READY 各自绑定什么。当前代码不复用旧授权或旧 FD，也没有用只追账户报告替代行情、定时器、配置等完整输入重放。[复制接口](replica-interface.md)和[app 接管工具](../../crates/tickwire-app/src/failover.rs)已经包含实际数据传输与恢复流程，不能再笼统记为“只有复制协议”。隔离设备和新执行权仍由独立控制能力提供。

## 组合验证状态

[Spot schema2 实际多进程链](../validation/core-chain/spot-handover-v2/README.md)已在 F2 固定源码和独立 Linux volume 上通过：目标进程 2485 完成实际迁移，独立回滚探测进程 2487 完成恢复，已消费动作保持已消费；签名 Prepared、Committed、Released 回执齐全，旧策略进程 2476 正常退出。新管理者从来源账户位置 3 消费到最终位置 9，接收迟到成交并完成费用闭合；订单 origin generation 5、manager generation 6，最终 BTC 1.001、USDT 940，`fee_reconciled=true`、`unknown=false`、未决订单为空。源端与镜像 9 条规范账户记录同序同字节。[较早的 schema1 结果](../validation/core-chain/spot-handover/README.md)独立保留。本次通过限于内置 FixedDemo 和同一执行域；泛型策略从固定来源到当前切点的完整合并输入尾部重放仍是代码缺口。

SBE mapped 发布已有独立 Linux 通过证据；USD-M 收入导入、两种复制提交模式、完整故障接管和多执行域发布仍需各自的真实组合验证。单元测试与文件级测试保留各自范围。新增代码的模型同步及模块、全局核心链路形式验证，在功能代码完成后统一执行。
