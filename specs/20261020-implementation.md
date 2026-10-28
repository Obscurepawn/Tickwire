# Tickwire 实现与验证

Status: active

实现以[详细设计](20261020-binance-ultra-low-latency.md)为依据。行情、策略、下单、私有回报、入账、费用核对，以及自定义 V1→V2 策略的 schema3 完整输入回放、持久安装、独立回滚探测和交接，已在 Linux 独立进程中运行通过。当前继续完成声明重建运行时接线、配置发布、完整账户行为和恢复接管，同时对固定源码快照执行模块及全仓检查。接口决策随实现记录，开发期间继续必要的单元测试、进程集成、故障注入和源码复审。全部代码开发完成后，再按最终源码逐模块执行 TLA+，然后验证全局组合；开发中不启动新的形式化检查。既有验证结果保留其原始源码范围，不自动覆盖后续改动。

实现按可编译阶段提交并推送远端，规格索引与完整验证证据集中收尾；提交范围限定为本任务修改，推送后核对远端 SHA。

## 实施状态

| 范围 | 当前实现 | 后续验收 |
|---|---|---|
| P0 契约与工具链 | Rust 1.99.1 workspace；ABI/风险向量、Spot SBE schema 与协议 fixtures 接入回归 | 生产能力核验和双产品组合资格 |
| P1 行情与 IPC | Spot/USD-M 深度同步、JSON / Spot SBE 与精确事件发布、memfd、跨进程 JOIN；稠密索引和连续盘口副本；同 catalog 运行时追加及真实重新 JOIN | 扩容重启恢复与 catalog 变更、死亡回收组合、更多实例及双产品进程场景 |
| P2 风控与执行 | Spot 多资产与 USD-M 账户、真实 WAL 与恢复对象、签名 REST / 持久 WS 发送、私有事件入账；Spot 实际进程链包含终态费用核对 | 双产品组合、剩余订单类型、完整风险范围和新增组合模型 |
| P3 发布与恢复 | 动态 Shadow、真实 READY/消费水位、签名多域协调及持久管理权交接；自定义策略 schema3 回放、安装、回滚探测、旧实例退出；真实 CaptureOnly 子进程采集、完整窗口重算与持久封口 | 声明重建目标安装与发布接线、多域组合、协调器与 exec 重启、配置发布 |
| P4 故障与接管 | mTLS 有序镜像、异步/同步提交模式、guardian、源封口和冻结恢复；持久 Raft、认证 peer 网络、快照与成员管理 | 实际隔离与恢复接管、同步镜像故障、统一控制业务接线 |

历史双平台验证与 schema2 交接保留原版本范围；本轮验证使用原生 WSL2 Linux。模块边界与剩余工作见[规格索引](INDEX.md)。

## 2026-10-28 行情索引与恢复验收

固定标的目录采用冷建 mask/gather 索引，过大的位表回退到有界平坦哈希表；动态目录提供稳定的只追加稠密 ID 与冷路径扩容。MD 为每个 topic 预绑定发布槽位，策略端改用连续盘口集合与固定类别槽，深度更新复用双侧工作区。预留容量内 100 次深度更新从 300 次分配降为 0，数量查询使用二分；外部 ID、IPC 布局和旧检查点字节保持兼容。算法、适用范围和五轮合成基准见[标的索引规格](implementation/instrument-index.md)。运行时新增标的仍需受信目录发布和 JOIN 接线。

入口接线进一步消除了外部标的 `u64` 到盘口 `u32` 的数值限制：`BookKey.instrument` 现在是 owner 预绑定的本地编号，MD 从整份 feed 配置建立统一索引，执行进程从规范账户目录取得固定槽。发布器拒绝不同标的重复使用完整本地 key。高位 ID、同低 32 位标的、目录排序与重连均有回归；真实 SBE WSS → REST 快照 → DepthSync → 共享内存验证了 `u64::MAX` 仍完整出现在 IPC 中。本地编号不进入目录、快照或 WAL；深度来源 `stream_id` 的现有 `u32` 容量约束未变。

该回归暴露的 4096 档 JSON 发布栈溢出已通过共享解码工作区修复，原 8 MiB 测试绕行已移除。发布器现在使用固定记录数组、预留四类状态和三个事件源槽，每 topic 的解码与 IPC 档位工作区合计 128 KiB；缓存预算按实际保留容量与文本载荷核验。首次 Quote 发布由 3 次/3608 字节分配降为 0，首次 Trade/BBA/MarkFunding、JSON/SBE 的 inline、arena、4096 档及长后短帧同样零分配，实际发布在 2 MiB 栈通过。JSON 新旧 diff 入口共用有界扫描器，保留合法转义，未知字段容器嵌套最多 128 层；独立 serde oracle 覆盖协议接受性与解码值。网络接收和帧解析仍有分配，不在这次发布路径结论内。

发布路径改造后的完整 `check-rust.sh`、契约校验和 release 独立进程交接再次通过，结果仍为 S=2、C=10、目标序号 285、镜像与策略端各 12 条账户记录，费用闭合且无未知订单。全仓回归同时发现配置日志测试的 fork/exec 锁继承竞态；延迟 exec 200 ms 可稳定复现，测试级读写屏障修复后通过同一实验，生产锁未改。[发布热路径证据](evidence/20261028/publisher-hot.json)绑定 682 份源码与八个实际运行二进制；[阶段索引](evidence/20261028/README.md)区分本次结果和下面的历史快照。

本地盘口编号接线后，完整 `check-rust.sh`、设计契约和文档校验通过，并用更新后的 release 二进制再次通过独立进程 `spot-handover-custom` 场景：源 S=2、C=10，目标最终输入序号 285，复制端与策略端各核对 12 条账户记录，成交费用闭合且无未知订单。源码、二进制摘要和验证范围保存在[本地盘口身份接线证据](evidence/20261028/dense-book.json)。

补齐了原先返回 `Unsupported` 的 `CaptureOnly`：校验种子、实际行情与账户输入，错误不推进窗口，封口绑定真实输入链和账户摘要。采集只接受同一 boot 的本机单调时钟，使用 SDK 固定观察器；声明重建的 Host 和进程接线仍待完成。schema3 恢复依赖改用已有 64 MiB 单对象预算，类型化 replay 消息仍保持 16 MiB，修复合法策略二进制在启动、安装或回滚时被误拒绝的问题。

此前恢复修复阶段的 release 在原生 Linux 上通过两轮 `spot-handover-custom`：普通部署，以及旧/新策略 ELF 均补齐到 16 MiB＋1 字节的部署。两轮都完成来源回放、目标安装、第三身份回滚探测、旧进程退出、管理权移交和迟到成交；账户与镜像前缀及新策略消费水位均为 12，BTC 1.001、USDT 940，订单终态费用闭合且无 Unknown。大文件场景保持原有期限。

该恢复阶段的全 workspace all-targets、doctest、格式和 Clippy 检查通过，设计契约、OpenAPI、文档链接及 60 crate 的依赖 DAG 校验通过。真实 memfd、FD 传递和多进程测试在本机 Linux 执行；Docker daemon 未运行。验证中修复了 SHM 测试的无效 ABI 载荷，以及并行测试 fork/exec 暂时继承 WAL 锁的竞态，生产协议校验和文件锁均保持原规则。[恢复阶段证据](evidence/20261028/summary.json)保存二进制、源码摘要、场景结果、环境、测量参数和原始 CSV；其 `final` / `verified` 文件名限定于当时的源码快照。

上述索引、发布和恢复阶段的行为改动不在既有 TLA+ 源码锚点内，没有新增 TLC 结论。该阶段格式整理没有新增失效锚点；仓库已有的失效锚点保留原状，见[锚点审计](evidence/20261028/format-anchor-audit.json)。生产目标主机的延迟资格、完整 USD-M 场景、常规配置发布、跨主机隔离接管仍未完成。

## 2026-10-28 行情扩容基础接口

[扩容计划](implementation/market-expansion.md)绑定实际配置 revision/hash、bank、全部批准消费者及峰值预算；独立候选保持旧稠密编号，追加新标的。发布器预热新增行，提交时刷新旧行并核验新鲜度，先写完整新基线，再冻结和交换旧 bank。MD 控制服务暂停 JOIN，以真实注册 cursor 的 QUIESCED 证明等待退出，并检查后继 bank 健康后才重新开放。

消费者新增有界 `PendingJoin`，连接、发送和追平可逐步推进；原阻塞入口复用同一状态机。策略输入增加 `MarketTransition`，以真实 JOIN 基线、追平记录及完整消费摘要保存跨 bank 转换，持久提交后才更新副本。回放、来源比较和安装现在核对完整游标摘要，拒绝输入字节不同而最终盘口相同的伪等价结果。

这些接口已通过真实控制连接、memfd、重新 JOIN 和重放回归。完整 `check-rust.sh`、设计契约及新 release 的 Spot 独立进程场景通过：S=2、C=10，目标最终序号 279，镜像与策略端各 12 条账户记录，费用闭合且无未知订单。[阶段证据](evidence/20261028/market-expansion-core.json)绑定 696 份源码与八个二进制。

在该接口阶段，持续运行的 MD、执行、策略循环及双收据尚未装配，Spot 独立进程场景验证的是原链路兼容性。该阶段修改了 JOIN owner 源码，相关历史形式化证据保留旧范围，最终开发完成后统一复核。

## 2026-10-28 运行时行情扩容验收

MD 现可在保留旧网络 owner、连接和本地编号的同时预热新增来源；冷 worker 处理 DNS、证书与控制文件，固定 token 随 owner 保留。全部消费者核验精确批准并返回 Armed 后，MD 才暂停旧 JOIN，等待真实 cursor QUIESCED，提交完整后继 bank，并同步同 revision/hash 的 effective 文件。执行进程和两个策略进程经原认证控制连接接收 Arm/Commit，驱动有界 PendingJoin，按各自持久边界放行新行情。失败发生在退出前时保留旧流，退出后保持关闭。

控制决定先保存完整内容寻址对象，再将引用同步到原 WAL；保持原物理批次上限和总预算。文件扫描至少间隔 10 ms，未到扫描时刻零分配，未变化的大批准文件不重读。同步后重新采样时钟并核验当前 Binding；WAL 保存真实准入时刻，派发使用同步后的当前时间，过期决定保留审计历史且恢复不重新授权。

跨 bank 输入继续保存完整 JOIN 基线、资源身份、时刻和原始追平记录。新 `cursor_basis=1` 以最后一条真实生产者记录生成游标摘要，使不同消费者在同一前沿可比较；旧格式保留原摘要。没有追平记录时，策略保持暂停并等待新 bank 的真实记录。持久化后再核验会话、期限和连续性，才交换 Client；原始记录不同而盘口相同的输入仍拒绝。

原生 Linux 的 release `spot-market-expansion` 场景已通过：在同一 catalog 中追加 `u64::MAX` 标识的 ETHUSDT，MD、执行、Active 和 Shadow 的 PID/starttime 保持不变；三个消费者均完成真实 Activated 和 effective 收据。来源检查点 S=2，经扩容观察点 161 到实际 C=167，完整输入链前缀核验通过；随后完成旧 binary 回放、目标安装、独立回滚探测、旧策略退休与迟到成交。镜像与目标策略各核对 13 条账户记录，订单费用闭合且无 Unknown，BTC 为 1.001、USDT 为 940，新增 ETH 余额保持零。

输入审计累计有一条启动阶段的 `Clock` Gap，首次账户准入前已通过三类快照和 `Reactivated` 记录精确关闭，之后没有新 Gap，也没有 Quarantine。相对旧场景多出的一条账户记录来自周期 REST 查询，订单仍为 NEW、累计成交为零；[原始 WAL 审计](evidence/20261028/market-expansion-runtime-input-audit.json)保留序号、哈希链和对账绑定。日志没有记录更细的时钟采样原因。

完整 `check-rust.sh`、设计契约和 OpenAPI 校验通过，60 个 crate 的内部依赖保持 DAG；794 份构建输入在 release、进程运行和全仓检查之间没有漂移。[阶段证据](evidence/20261028/market-expansion-runtime.json)保存来源与二进制摘要、实际水位及故障回归。[扩容时序图](../assets/market-expansion.svg)展示持久化与观察放行的位置。

当前扩容限于已批准 catalog 和同一组运行进程。常规配置 API、跨 catalog 更新、扩容中途重启恢复与旧 bank 死亡回收继续按各模块推进；执行冷控制仍包含主线程文件同步，不宣称全控制路径硬实时。此次未改动既有模型锚点文件，也未新增 TLC 结论，最终形式化验证仍按文首约定统一进行。

## 2026-10-28 声明重建采集验收

独立 `--capture-only` 启动模式已接 SDK 与 Host：先完成真实 MD JOIN 和固定账户前缀，再建立种子，用固定观察器收集有界输入。封口请求绑定完整启动配置、合同、账户位置、挑战和期限；窗口逐行重算，完整九对象闭包同步后才发送回执。Host 重读全部依赖并核验同一实际子进程、报告游标与当前期限。正常停止完成 MD 与报告通道的真实退订，再确认子进程成功退出。

固定 release worker 的两个 Linux 验收通过。成功窗口包含三行 warm 输入，行情从 2 到 3、账户从 0 到 1，报告 ring 游标为 2；采集期没有创建用户策略或发送业务 READY。缺失启动配置或封口请求、完成件篡改、超时和 Drop 均按预期关闭。该测试的策略是独立进程，MD 的真实 JOIN server 与 memfd Publisher 位于父进程线程。完整 `check-rust.sh`、74 个 IPC 向量和设计契约通过，808 个构建输入在构建与验收间保持不变。

共用报告通道和退订逻辑也通过既有 release `spot-handover-custom` 场景：S=2→C=10，目标最终序号 285，旧进程正常退出，镜像与策略各有 12 条账户记录，费用闭合且无 Unknown。累计输入中的一条启动 `Clock` Gap 已由首条 `Reactivated` 覆盖，无 Quarantine。[采集阶段证据](evidence/20261028/capture-runtime.json)保存最终源码、二进制、水位和日志摘要，[采集时序图](../assets/rebuild-capture.svg)展示窗口与后续发布的边界。

App 仍拒绝 schema4 发布；真实工厂安装、安装后的 READY、窗口之后的连续接续和完整发布装配继续推进。新增采集行为的形式化验证保留到最终开发完成后；本阶段未修改历史模型或源码锁。

## 核心运行程序

`tickwire-app` 已装配系统时钟校准、共享额度、公共行情、私有流、账户输入 WAL、签名 REST 下单与恢复对账。对账按原订单状态查询、完整成交分页、费用闭合、账户及两类挂单快照的顺序执行；激活记录绑定已同步的输入和账户日志前缀，恢复不会重发旧意图。

独立策略进程通过真实 JOIN 和 SPSC 通道收发行情、完整规范意图及账户回报。完整恢复对象、真实消费输入和调用时刻在意图发布前保存。策略换代、回报积压与多策略轮询沿各自通道和授权推进。运行接口见主程序、策略进程和账户对账。

Linux 首轮完整成交验证得到 BTC 1.001、USDT 940；4 条账户记录与镜像逐条一致，9 条输入记录均完成处理，策略进程确认消费到账户水位 4。运行证据和脚本复跑保存源码、二进制、实际日志及离线报告。该版本尚未独立闭合手续费，也未执行最终源封口或策略交接；后续结果另行保存。

修复终态订单维护调度后，新的 Linux 进程运行完成订单查询、成交查询和 `FeesReconciled` 入账。离线报告确认订单终态、`fee_reconciled=true`、`unknown=false`，且无待处理订单；8 条账户记录与镜像逐条一致，16 条输入记录完整，策略确认消费至账户水位 8。费用闭合验证保存该次独立源码与二进制摘要。

## 模块入口

- 执行内核与服务接口，[执行规格](implementation/execution-core.md)。
- 盘口接口，[盘口规格](implementation/book-core.md)。
- 风险接口，[风险规格](implementation/risk-core.md)。
- IPC 接口，[IPC 规格](implementation/ipc-codec.md)。
- 共享 ring 接口，[ring 规格](implementation/shm-ring.md)与[Linux 映射规格](implementation/shm-mapping.md)。
- 控制会话、传输边界与物理日志；各自规格位于 `specs/implementation/`。
- [不可变资源](implementation/immutable-resource.md)、[Topic 快照构造](implementation/topic-snapshot-builder.md)、[快照接收](implementation/topic-snapshot.md)与[业务日志编码](implementation/journal-codec.md)。
- [JOIN 协调](implementation/join.md)、[业务日志与恢复](implementation/business-journal.md)、[恢复依赖](implementation/recovery-closure.md)与[Mock 中止重放](implementation/mock-abort.md)。
- [完整命令摘要与日志绑定](implementation/command-binding.md)。
- [交易所命令与事件](implementation/venue.md)、[非阻塞网络](implementation/transport.md)、[Binance 协议](implementation/binance-protocol.md)与[持久发送协调](implementation/gateway.md)。
- 账户接口、[账户规格](implementation/account.md)、[账户恢复编码](implementation/account-codec.md)与 [Spot 请求扩展](implementation/binance-request-extensions.md)。
- [账户日志](implementation/account-journal.md)、[恢复依赖绑定](implementation/account-recovery-binding.md)与[账户发送协调](implementation/account-gateway.md)。
- [Binance 行情与私有事件](implementation/binance-streams.md)、[盘口启动和重同步](implementation/binance-depth-sync.md)。
- 多域交接协调器、交接执行端、复制与冻结接管和 guardian。
- 形式验证工作区：`tla/`；每层保留模型、配置、实际运行记录和源码锚点。

## 验证与完成要求

核心运行链路完成后，统一执行 `cargo test --workspace`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo fmt --all -- --check` 和原设计契约检查。全部代码完成后，重新核对每个模块的模型与最终源码，逐模块运行 TLA+，再运行覆盖跨模块状态转换、并发交错和崩溃恢复的全局组合模型。模型使用固定 TLC 版本执行，范围、有限域、动作覆盖与未覆盖行为写进各层输出；发现反例后先独立核对真实实现，再修复并复跑相关模块及组合模型。下文数字为已有阶段的历史证据。

上一阶段 `04351db` 的 `./scripts/check-rust.sh` 在 macOS 通过 202 项测试，`./scripts/check-linux.sh` 在 Linux aarch64 通过 223 项测试，均含 1 项编译失败 doctest。macOS 输出、Linux 输出及 SHM 回归和执行模块失败回归记录保留实际证据：

| 模块 | Rust 测试数 | 覆盖内容 |
|---|---:|---|
| `tickwire-book` | 30 | 协议/存储/边界、确定性工作量和无分配；8,000 批参考模型、960,000 次查询及独立 40,000 批差分 |
| `tickwire-risk` | 18 | 17 项边界，1 项执行全部 21 数值向量/131 预期字段 |
| `tickwire-ipc` | 110 | 36 布局、63 测试向量、11 项语义/畸形输入测试；包含全部 25 semantic_cases |
| `tickwire-execution` | 16 | 预算、提交、发送、去重、费用尾款、未知冻结和恢复 |
| `tickwire-mock` | 13 | 12 项服务故障/恢复测试，1 项 Spot 本地完整流程 example 测试；含恢复失败仍须冻结旧引擎的回归 |
| `tickwire-shm` | macOS 14 / Linux 25 | 13 项协议、1 项引用生命周期 doctest；Linux 另有 11 项映射/多进程测试，含 1 项子进程辅助入口 |
| `tickwire-shm-sys` | macOS 1 / Linux 11 | macOS 拒绝不支持平台；Linux seals/权限/边界与跨进程发布，含 1 项子进程辅助入口 |

两个平台的格式与 Clippy 检查通过。原设计的 IPC、风险、配置、OpenAPI 和部署算术检查通过；链接与资产检查单独保留。盘口前后对照各有五轮相邻/散布负载，记录分位数、内存成本及少量散布改量退化，见性能报告。历史基线 CSV 和环境记录保持原测量版本。

TLC 1.7.4 对执行模块的三个有限模型运行了九组配置，均通过不变量检查，所有 Next 动作均生成过后继状态：

| 模型与配置 | generated | distinct | 最大深度 |
|---|---:|---:|---:|
| L0 提交门禁：primary | 21,077 | 2,236 | 15 |
| L0 提交门禁：scaling | 103,216 | 9,076 | 15 |
| L1 崩溃与回执：primary | 6,539 | 1,744 | 14 |
| L1 崩溃与回执：scaling | 417,807 | 96,828 | 22 |
| L1 正常等待回执：pending | 833,051 | 186,148 | 22 |
| L2 交易结果记录 v2：primary | 139,711 | 24,036 | 8 |
| L2 交易结果记录 v2：long | 1,591,624 | 280,458 | 9 |
| L2 交易结果记录 v2：boundary | 14,397,539 | 1,495,032 | 11 |
| L2 交易结果记录 v2：scaling | 14,278,433 | 1,485,288 | 11 |

[L0 记录](../tla/execution_models/submission/L0_commit_gate/03_run_log.md)、[L1 记录](../tla/execution_models/submission/L1_crash_receipt/03_run_log.md)与 [L2 记录](../tla/execution_models/submission/L2_economic_facts/03_run_log.md)包含动作覆盖、源码哈希和有限域。L2 增加成交、费用、终态、去重及算术边界，发现恢复重放失败时旧引擎仍保留交易授权的问题。实际 Rust 回归确认后，`MockRuntime::restart` 改为先冻结旧引擎；四组 v2 配置与回归均通过。模型按同步 API 边界执行，真实磁盘和 journal append/apply 之间的进程崩溃留待下一层。

共享 ring 的五组配置也全部通过，物理容量均为实际 ABI 最小值 64。wrap 配置从合法的 64 条发布前缀开始，覆盖满环后的槽位复用；双消费者 wrap 要等两者都释放第一条，才允许写入序号 65。各配置列出的动作均生成后继，详见 [ring 运行记录](../tla/execution_models/broadcast_ring/L0_publish_borrow_reclaim/03_run_log.md)。

| ring 配置 | generated | distinct | 最大深度 |
|---|---:|---:|---:|
| primary | 196,588 | 29,889 | 25 |
| pending_wrap | 84,451 | 12,834 | 21 |
| two_readers | 9,587,623 | 930,364 | 33 |
| wrap | 71,291 | 11,324 | 21 |
| wrap_two | 341,833 | 40,796 | 24 |

模型检查完整发布、活跃借用与未读记录不被覆盖、释放前缀、实际 cursor 退出证据以及 Freeze。身份固定、序号和借用窗口有限；CPU 弱内存、生产控制身份和跨 bank 轮换由后续验收覆盖。

单元、集成测试与形式化验证保存可复现的命令和结果；部分覆盖的用例标明实际范围。INV-1～INV-14 继续约束实现。

## 控制与物理日志阶段

新增模块均完成接口、Red/Green、规格与独立源码审核：

| 模块 | 验证 | 完成边界 |
| --- | --- | --- |
| tickwire-control | 13 项逻辑测试；Linux 独立 exec 子进程握手与重试测试，另 1 个显式 ignored helper 由父测试运行 | registry + 内核身份认证；租约关闭；存储确认 Received 后一次交付票据；准确终态重放 |
| tickwire-control-sys | macOS 不支持平台测试；Linux 15 项（含 4 个子进程辅助入口），fmt/clippy | SEQPACKET 一帧一包、SCM_RIGHTS、CLOEXEC、超时、畸形包 FD 回收、真实连接 peer credentials |
| tickwire-journal | macOS/Linux 各 21 项，另 1 个 ignored helper 由四个 SIGKILL 场景运行；fmt/clippy | 完整批写入并成功 sync_data 后的物理收据；分段/manifest；验证后修复末尾 |

控制会话证据、传输证据与日志证据保存命令和实际输出。Linux 综合用例持有未 reap 的 Child 句柄，比较实际子进程 PID/UID/GID；owner 版本从 0 变为 7 后，同一请求仍返回版本 1 的原响应，执行票据只交付一次。

请求登记 L0 的三组模型分别检查 370、1,570、13,442 个不同状态；会话组合 L1 分别检查 1,566、3,926、3,804 个状态，全部通过。物理日志 L0 三组分别检查 1,816、2,695、45,883 个状态，覆盖写入、同步、收据与恢复边界。每组 Next 动作均生成后继，完整范围见 [control_requests](../tla/execution_models/control_requests/ROADMAP.md) 和 [physical_journal](../tla/execution_models/physical_journal/ROADMAP.md)。这些模型尚未组合 owner 副作用、真实 SQL/KV 或跨主机复制。

## 盘口小批更新阶段

小批乱序改量直接定位已有价位，循环有序输入先旋转再复用相邻定位。全部 36 项测试、格式与 Clippy 检查通过；双侧原子提交、覆盖外 `Unknown`、40,000 批独立差分及零分配回归均保留。跨末尾批次的确定性工作量测试先失败，再随修复通过。

以 `df8bd2d` 为基线，相邻和散布两套相同负载各运行五轮，每负载每轮采样 200,000 次。M1 Pro/macOS 上，4096 档、双侧合计 16/64 项散布改量的每轮 p50 中位数为 958/4000 → 542/2000 ns；相邻改量的 p50 中位数均保持基线值。512 档、512 项散布改量仍有 +0.5% 的 p50 变化，两组运行范围重叠；全部原始分位数、最大值和被淘汰候选均保留在完整报告。

计时范围是 `Book::apply` 到读取有效 view 和最优买卖价结束，使用单个热缓存实例，未包含网络、解码、IPC 或持久化。当前结果支持保留该实现；多标的、真实录包和目标 Linux 主机的性能资格仍待 P5 验收。

盘口源码对应的 [TLA+ L0/L1](../tla/execution_models/book_state/ROADMAP.md) 八组配置通过。L0 的 Spot 主/扩展检查 1,392/8,499 个不同状态，USD-M 主/扩展为 1,880/10,524；L1 的 Bid 主/扩展为 1,471/7,050，Ask 主/扩展为 1,471/8,244。每组分别覆盖全部 19 或 15 个 Next 动作。L1 保留覆盖范围外的隐藏价位见证，检查已知数量/最优价与真实内容一致，以及 Unknown 不被当成零；实际排序算法、适配器正确性和未来 exec 行情门禁没有被这一有限模型证明。

## 不可变快照与业务日志阶段

macOS 和 Linux 的全仓格式、全部 targets、doctest 与 Clippy 检查通过，实际输出见阶段验证。

| 模块 | 专项验证 | 完成边界 |
| --- | --- | --- |
| 不可变资源 | Linux shm-sys 21 个入口、依赖 ring 24 项和 1 项 compile-fail doctest；macOS 2 项 | 私有 pwrite 完成后设置 `0x0f`；实际只读 FD 与安全只读切片；认证预期身份/hash、范围和零页尾校验 |
| Topic 快照接收 | 12 项纯 Rust；Linux 另有 2 个跨进程测试入口（含 helper） | 完整成员与共同 S，JOIN/资源/状态身份及期限；SCM_RIGHTS 交付后接收校验；错误资源拒绝与 FD/映射回收 |
| 业务日志 codec | 13 项测试、6 个记录/17 个整数测试向量、591 个截断与 99,840 个单字节变异 | 最多 201 字节确定性 CBOR；原订单代次与 writer 代次分开；稳定业务摘要和绑定 writer 的逻辑链 |

快照通过校验不会自行改变 owner/cursor 状态。业务 codec 也没有生成可直接授权发送的提交证明；真实 WAL、依赖闭包与执行恢复的适配器仍须连接这些已验证原语。

## Topic 快照构造阶段

快照构造器已实现完整成员集合、共同序号 S、状态与深度编码、摘要及 JOIN 绑定。先检查全部大小和容量，再分配输出；非 Live 成员保留在快照中，构造成功不授予交易资格。18 项新增构造与算术测试及 12 项既有接收测试在 macOS/Linux 通过，Linux 另通过 2 个既有跨进程接收入口，格式与 Clippy 通过。实际失败测试、最终输出和独立基准样本比对见验证报告。涉及 owner、pending gate 和 JOIN_READY 的状态迁移在 JOIN 协调模块中继续验证。

## JOIN 与业务恢复阶段

三进程 JOIN 已串起认证请求、快照构造、bank/snapshot 的 FD 交付、真实 cursor 登记、追平激活和协作退出。失败订阅保留原门禁，重复 READY 和退出确认返回原结果；当前每个 owner 最多累计登记 M 次，持久请求恢复与进程死亡回收仍待实现。

业务日志将原有 codec、真实 WAL 与执行内核连接起来，先验证授权和恢复依赖，再同步意图，发送前刷新时效检查；未发送中止先保存后释放。恢复按原事件顺序执行并保持冻结。外部授权由接口提供，测试采用明确的 Mock；完整签名证明、同步副本及恢复后的重新授权尚未实现。

macOS/Linux 全仓测试与 Clippy 通过，新增四组模型共 13 个配置全部通过。测试、源码摘要和模型范围见阶段验证。旧 Mock 恢复模型保留为历史版本，不把其原有通过结果当作当前实现的完整组合证明。

## 接续工作

按官方协议实现完整 Binance 连接器，并用协议 Mock 验证同一实现。继续组合 md/strategy/exec 业务进程，补齐 Arena、bank 轮换、退出回收、配置发布、完整风险账户、复制及接管逻辑。所有模块继续按接口、TDD、Specs、形式化验证的顺序推进。

## 完整命令与网络发送阶段

`tickwire-venue` 将完整订单参数、目录与资产单位、请求身份编码为不可变规范对象；交易所事件保留原始成交身份、报价金额、费用和已实现盈亏。`tickwire-transport` 使用真实非阻塞 TCP/TLS，首次接受应用字节前调用门禁；HTTP、WebSocket 和 FIX 文本分帧有明确容量和调度预算。

`tickwire-binance` 实现 HMAC/RSA/Ed25519 签名、Spot/USD-M 已列明的订单请求、错误分类和类型化响应。`tickwire-gateway` 将完整命令闭包、真实 WAL、最终门禁与同一 TLS 连接组合。Spot 和 USD-M REST 集成由本地协议服务器检查实际收到的参数及签名；零接受中止先保存，部分接受与恢复保留 Unknown。

当前验证使用明确的授权与对象存储 Mock，业务日志和网络传输执行真实 I/O。完整账户风险、业务 ACK/成交关联、行情与私有流会话、列表和剩余协议组合继续按各模块接口推进。测试记录见发送协调验证。

## 账户、恢复编码与 Spot 请求扩展阶段

`tickwire-account` 实现 Spot 多资产资源与 USD-M 单结算资产账户，维护策略归属、订单预留、成交和费用去重、终态累计、减仓与费用清算。准入结果绑定完整命令、账户代次和风险版本；恢复按原顺序重新计算准入并核对记录，完成后保持冻结。46 项测试在 macOS/Linux 通过。当前基线不导入已有活动订单，跨币种保证金、在线更换基线及部分复杂订单的风险计算仍待实现。

`tickwire-account-codec` 为完整账户基线和八类恢复记录提供有界、确定性的 CBOR 编码与摘要。16 项测试包含独立 Python 基准样本、截断和单字节变异、实际文件对象与 WAL 重开恢复；恢复后的资源与运行中的账户逐项比较。账户、恢复编码和发送协调的生产组合仍须在执行模块中接通。

Spot 请求增加 OCO、OTO、OTOCO、peg 与 SOR 的类型化参数和 REST/WebSocket 编码。新增 17 项测试包含十次实际 TLS 签名请求，以及旧命令字节兼容和旧解码器拒绝新增标签的检查。请求编码完成不自动赋予对应账户准入资格。

账户 L0/L1 共六组模型通过，全部 Next 动作均生成后继状态，覆盖迟到成交、重复观察、终态累计、费用与减仓预留。接口边界、完整有限域和运行记录见账户验证、恢复编码验证、请求扩展验证与[账户模型](../tla/execution_models/account/ROADMAP.md)。

## 账户日志、发送组合与盘口同步

账户日志按原顺序保存八类恢复记录，逐条准入绑定完整基线和此前日志前缀。发送协调器使用同一个账户管理资金，在依赖与 Admit 同步完成后才排队；部分接受后保留责任，零接受中止通过账户预校验并同步后才释放。恢复没有网络连接或自动重发。账户冻结后，健康连接仍允许满足当前授权和期限的撤单、查询。

Binance 普通 JSON 深度及私有事件已接入类型化解码；盘口协调器在订阅后缓存增量、取得 REST 快照、按产品序列规则桥接。旧连接和旧请求回调不改变当前盘口，溢出、断档和重连关闭可见性并重新同步。私有流登录续期、轮换、业务关联和完整进程接线仍继续实现。

本阶段新增组合模型独立于已有账户、物理日志和传输模型。各模型的状态、范围与证据以 `tla/execution_models/` 对应输出为准；旧模型的 PASS 不代替新增组合验证。
