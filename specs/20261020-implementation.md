# Tickwire 实现与验证

Status: active

实现以[详细设计](20261020-binance-ultra-low-latency.md)为依据。每个模块依次交付接口设计、先失败后通过的测试、实现规格和源码对应的 TLA+ 模型。外部存储通过抽象与 MockService 完成本地链路，交易所通过可扩展适配接口接入。

每完成一个可验证阶段即提交并推送远端，保存对应 Specs、测试和模型结果；提交范围限定为该阶段修改，推送后核对远端 SHA。

## 实施状态

| 范围 | 当前实现 | 后续验收 |
|---|---|---|
| P0 契约与工具链 | Rust 1.99.1 workspace；既有 ABI/风险向量接入 Rust 回归 | 双产品协议 fixtures、SBE/schema 锁定、生产能力核验 |
| P1 行情与 IPC | 批量有界盘口、小批散布改量、Spot/USD-M 深度解码与快照/增量重同步；固定 wire codec、Linux memfd、快照构造与接收、广播及跨进程 JOIN | Arena、bank 轮换、进程死亡回收与完整业务进程接入 |
| P2 风控与执行 | 定点风险公式、Spot 多资产与 USD-M 账户、真实账户 WAL 与恢复依赖绑定、账户发送协调、Binance REST 签名及私有事件解码 | 发送组合形式模型、行情风控、协议会话与业务回报关联、剩余订单类型和限流 |
| P3 发布与恢复 | 有序 journal Mock；真实物理 WAL/分段/同步恢复；业务日志适配、恢复依赖保存与验证；恢复后保持冻结 | 策略升级/回滚、多 exec 协调、对账、恢复后授权和配置发布 |
| P4 故障与接管 | 注入存储失败、收据丢失和交易所 ACK 丢失 | 副本、控制共识、guardian、实际隔离与灾难恢复 |

## 模块入口

- [执行内核与服务接口](../docs/implementation/execution-interface.md)，[执行规格](implementation/execution-core.md)。
- [盘口接口](../docs/implementation/book-interface.md)，[盘口规格](implementation/book-core.md)。
- [风险接口](../docs/implementation/risk-interface.md)，[风险规格](implementation/risk-core.md)。
- [IPC 接口](../docs/implementation/ipc-interface.md)，[IPC 规格](implementation/ipc-codec.md)。
- [共享 ring 接口](../docs/implementation/shm-ring-interface.md)，[ring 规格](implementation/shm-ring.md)与[Linux 映射规格](implementation/shm-mapping.md)。
- [控制会话](../docs/implementation/control-session-interface.md)、[传输边界](../docs/implementation/control-transport-interface.md)与[物理日志](../docs/implementation/journal-interface.md)；各自规格位于 `specs/implementation/`。
- [不可变资源](implementation/immutable-resource.md)、[Topic 快照构造](implementation/topic-snapshot-builder.md)、[快照接收](implementation/topic-snapshot.md)与[业务日志编码](implementation/journal-codec.md)。
- [JOIN 协调](implementation/join.md)、[业务日志与恢复](implementation/business-journal.md)、[恢复依赖](implementation/recovery-closure.md)与[Mock 中止重放](implementation/mock-abort.md)。
- [完整命令摘要与日志绑定](implementation/command-binding.md)。
- [交易所命令与事件](implementation/venue.md)、[非阻塞网络](implementation/transport.md)、[Binance 协议](implementation/binance-protocol.md)与[持久发送协调](implementation/gateway.md)。
- [账户接口](../docs/implementation/account-interface.md)、[账户规格](implementation/account.md)、[账户恢复编码](implementation/account-codec.md)与 [Spot 请求扩展](implementation/binance-request-extensions.md)。
- [账户日志](implementation/account-journal.md)、[恢复依赖绑定](implementation/account-recovery-binding.md)与[账户发送协调](implementation/account-gateway.md)。
- [Binance 行情与私有事件](implementation/binance-streams.md)、[盘口启动和重同步](implementation/binance-depth-sync.md)。
- 形式验证工作区：`tla/`；每层保留模型、配置、实际运行记录和源码锚点。

## 验证与完成要求

基础命令为 `cargo test --workspace`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo fmt --all -- --check`，并保留原设计契约检查。模型使用固定 TLC 版本执行，范围、有限域与未覆盖行为写进各层输出。

上一阶段 `04351db` 的 `./scripts/check-rust.sh` 在 macOS 通过 202 项测试，`./scripts/check-linux.sh` 在 Linux aarch64 通过 223 项测试，均含 1 项编译失败 doctest。[macOS 输出](../docs/validation/rust-tests.log)、[Linux 输出及 SHM 回归](../docs/validation/shm/README.md)和[执行模块失败回归记录](../docs/validation/execution-tdd-red.log)保留实际证据：

| 模块 | Rust 测试数 | 覆盖内容 |
|---|---:|---|
| `tickwire-book` | 30 | 协议/存储/边界、确定性工作量和无分配；8,000 批参考模型、960,000 次查询及独立 40,000 批差分 |
| `tickwire-risk` | 18 | 17 项边界，1 项执行全部 21 数值向量/131 预期字段 |
| `tickwire-ipc` | 110 | 36 布局、63 测试向量、11 项语义/畸形输入测试；包含全部 25 semantic_cases |
| `tickwire-execution` | 16 | 预算、提交、发送、去重、费用尾款、未知冻结和恢复 |
| `tickwire-mock` | 13 | 12 项服务故障/恢复测试，1 项 Spot 本地完整流程 example 测试；含恢复失败仍须冻结旧引擎的回归 |
| `tickwire-shm` | macOS 14 / Linux 25 | 13 项协议、1 项引用生命周期 doctest；Linux 另有 11 项映射/多进程测试，含 1 项子进程辅助入口 |
| `tickwire-shm-sys` | macOS 1 / Linux 11 | macOS 拒绝不支持平台；Linux seals/权限/边界与跨进程发布，含 1 项子进程辅助入口 |

两个平台的格式与 Clippy 检查通过。原设计的 IPC、风险、配置、OpenAPI 和部署算术检查通过；链接与资产检查单独保留。盘口前后对照各有五轮相邻/散布负载，记录分位数、内存成本及少量散布改量退化，见[性能报告](../docs/validation/book-optimization/report.md)。历史基线 [CSV](../docs/validation/book-baseline-macos-arm64.csv) 和[环境记录](../docs/validation/book-baseline-environment.json)保持原测量版本。

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

[控制会话证据](../docs/validation/control-session/README.md)、[传输证据](../docs/validation/control-transport/README.md)与[日志证据](../docs/validation/journal/README.md)保存命令和实际输出。Linux 综合用例持有未 reap 的 Child 句柄，比较实际子进程 PID/UID/GID；owner 版本从 0 变为 7 后，同一请求仍返回版本 1 的原响应，执行票据只交付一次。

请求登记 L0 的三组模型分别检查 370、1,570、13,442 个不同状态；会话组合 L1 分别检查 1,566、3,926、3,804 个状态，全部通过。物理日志 L0 三组分别检查 1,816、2,695、45,883 个状态，覆盖写入、同步、收据与恢复边界。每组 Next 动作均生成后继，完整范围见 [control_requests](../tla/execution_models/control_requests/ROADMAP.md) 和 [physical_journal](../tla/execution_models/physical_journal/ROADMAP.md)。这些模型尚未组合 owner 副作用、真实 SQL/KV 或跨主机复制。

## 盘口小批更新阶段

小批乱序改量直接定位已有价位，循环有序输入先旋转再复用相邻定位。全部 36 项测试、格式与 Clippy 检查通过；双侧原子提交、覆盖外 `Unknown`、40,000 批独立差分及零分配回归均保留。跨末尾批次的确定性工作量测试先失败，再随修复通过。

以 `df8bd2d` 为基线，相邻和散布两套相同负载各运行五轮，每负载每轮采样 200,000 次。M1 Pro/macOS 上，4096 档、双侧合计 16/64 项散布改量的每轮 p50 中位数为 958/4000 → 542/2000 ns；相邻改量的 p50 中位数均保持基线值。512 档、512 项散布改量仍有 +0.5% 的 p50 变化，两组运行范围重叠；全部原始分位数、最大值和被淘汰候选均保留在[完整报告](../docs/validation/book-small-batch/report.md)。

计时范围是 `Book::apply` 到读取有效 view 和最优买卖价结束，使用单个热缓存实例，未包含网络、解码、IPC 或持久化。当前结果支持保留该实现；多标的、真实录包和目标 Linux 主机的性能资格仍待 P5 验收。

盘口源码对应的 [TLA+ L0/L1](../tla/execution_models/book_state/ROADMAP.md) 八组配置通过。L0 的 Spot 主/扩展检查 1,392/8,499 个不同状态，USD-M 主/扩展为 1,880/10,524；L1 的 Bid 主/扩展为 1,471/7,050，Ask 主/扩展为 1,471/8,244。每组分别覆盖全部 19 或 15 个 Next 动作。L1 保留覆盖范围外的隐藏价位见证，检查已知数量/最优价与真实内容一致，以及 Unknown 不被当成零；实际排序算法、适配器正确性和未来 exec 行情门禁没有被这一有限模型证明。

## 不可变快照与业务日志阶段

macOS 和 Linux 的全仓格式、全部 targets、doctest 与 Clippy 检查通过，实际输出见[阶段验证](../docs/validation/snapshot-journal-stage/README.md)。

| 模块 | 专项验证 | 完成边界 |
| --- | --- | --- |
| 不可变资源 | Linux shm-sys 21 个入口、依赖 ring 24 项和 1 项 compile-fail doctest；macOS 2 项 | 私有 pwrite 完成后设置 `0x0f`；实际只读 FD 与安全只读切片；认证预期身份/hash、范围和零页尾校验 |
| Topic 快照接收 | 12 项纯 Rust；Linux 另有 2 个跨进程测试入口（含 helper） | 完整成员与共同 S，JOIN/资源/状态身份及期限；SCM_RIGHTS 交付后接收校验；错误资源拒绝与 FD/映射回收 |
| 业务日志 codec | 13 项测试、6 个记录/17 个整数测试向量、591 个截断与 99,840 个单字节变异 | 最多 201 字节确定性 CBOR；原订单代次与 writer 代次分开；稳定业务摘要和绑定 writer 的逻辑链 |

快照通过校验不会自行改变 owner/cursor 状态。业务 codec 也没有生成可直接授权发送的提交证明；真实 WAL、依赖闭包与执行恢复的适配器仍须连接这些已验证原语。

## Topic 快照构造阶段

快照构造器已实现完整成员集合、共同序号 S、状态与深度编码、摘要及 JOIN 绑定。先检查全部大小和容量，再分配输出；非 Live 成员保留在快照中，构造成功不授予交易资格。18 项新增构造与算术测试及 12 项既有接收测试在 macOS/Linux 通过，Linux 另通过 2 个既有跨进程接收入口，格式与 Clippy 通过。实际失败测试、最终输出和独立基准样本比对见[验证报告](../docs/validation/topic-snapshot-builder/report.md)。涉及 owner、pending gate 和 JOIN_READY 的状态迁移在 JOIN 协调模块中继续验证。

## JOIN 与业务恢复阶段

三进程 JOIN 已串起认证请求、快照构造、bank/snapshot 的 FD 交付、真实 cursor 登记、追平激活和协作退出。失败订阅保留原门禁，重复 READY 和退出确认返回原结果；当前每个 owner 最多累计登记 M 次，持久请求恢复与进程死亡回收仍待实现。

业务日志将原有 codec、真实 WAL 与执行内核连接起来，先验证授权和恢复依赖，再同步意图，发送前刷新时效检查；未发送中止先保存后释放。恢复按原事件顺序执行并保持冻结。外部授权由接口提供，测试采用明确的 Mock；完整签名证明、同步副本及恢复后的重新授权尚未实现。

macOS/Linux 全仓测试与 Clippy 通过，新增四组模型共 13 个配置全部通过。测试、源码摘要和模型范围见[阶段验证](../docs/validation/join-recovery-stage/README.md)。旧 Mock 恢复模型保留为历史版本，不把其原有通过结果当作当前实现的完整组合证明。

## 接续工作

按官方协议实现完整 Binance 连接器，并用协议 Mock 验证同一实现。继续组合 md/strategy/exec 业务进程，补齐 Arena、bank 轮换、退出回收、配置发布、完整风险账户、复制及接管逻辑。所有模块继续按接口、TDD、Specs、形式化验证的顺序推进。

## 完整命令与网络发送阶段

`tickwire-venue` 将完整订单参数、目录与资产单位、请求身份编码为不可变规范对象；交易所事件保留原始成交身份、报价金额、费用和已实现盈亏。`tickwire-transport` 使用真实非阻塞 TCP/TLS，首次接受应用字节前调用门禁；HTTP、WebSocket 和 FIX 文本分帧有明确容量和调度预算。

`tickwire-binance` 实现 HMAC/RSA/Ed25519 签名、Spot/USD-M 已列明的订单请求、错误分类和类型化响应。`tickwire-gateway` 将完整命令闭包、真实 WAL、最终门禁与同一 TLS 连接组合。Spot 和 USD-M REST 集成由本地协议服务器检查实际收到的参数及签名；零接受中止先保存，部分接受与恢复保留 Unknown。

当前验证使用明确的授权与对象存储 Mock，业务日志和网络传输执行真实 I/O。完整账户风险、业务 ACK/成交关联、行情与私有流会话、列表和剩余协议组合继续按各模块接口推进。测试记录见[发送协调验证](../docs/validation/gateway/README.md)。

## 账户、恢复编码与 Spot 请求扩展阶段

`tickwire-account` 实现 Spot 多资产资源与 USD-M 单结算资产账户，维护策略归属、订单预留、成交和费用去重、终态累计、减仓与费用清算。准入结果绑定完整命令、账户代次和风险版本；恢复按原顺序重新计算准入并核对记录，完成后保持冻结。46 项测试在 macOS/Linux 通过。当前基线不导入已有活动订单，跨币种保证金、在线更换基线及部分复杂订单的风险计算仍待实现。

`tickwire-account-codec` 为完整账户基线和八类恢复记录提供有界、确定性的 CBOR 编码与摘要。16 项测试包含独立 Python 基准样本、截断和单字节变异、实际文件对象与 WAL 重开恢复；恢复后的资源与运行中的账户逐项比较。账户、恢复编码和发送协调的生产组合仍须在执行模块中接通。

Spot 请求增加 OCO、OTO、OTOCO、peg 与 SOR 的类型化参数和 REST/WebSocket 编码。新增 17 项测试包含十次实际 TLS 签名请求，以及旧命令字节兼容和旧解码器拒绝新增标签的检查。请求编码完成不自动赋予对应账户准入资格。

账户 L0/L1 共六组模型通过，全部 Next 动作均生成后继状态，覆盖迟到成交、重复观察、终态累计、费用与减仓预留。接口边界、完整有限域和运行记录见[账户验证](../docs/validation/account/README.md)、[恢复编码验证](../docs/validation/account-codec/README.md)、[请求扩展验证](../docs/validation/binance-request-extensions/README.md)与[账户模型](../tla/execution_models/account/ROADMAP.md)。

## 账户日志、发送组合与盘口同步

账户日志按原顺序保存八类恢复记录，逐条准入绑定完整基线和此前日志前缀。发送协调器使用同一个账户管理资金，在依赖与 Admit 同步完成后才排队；部分接受后保留责任，零接受中止通过账户预校验并同步后才释放。恢复没有网络连接或自动重发。账户冻结后，健康连接仍允许满足当前授权和期限的撤单、查询。

Binance 普通 JSON 深度及私有事件已接入类型化解码；盘口协调器在订阅后缓存增量、取得 REST 快照、按产品序列规则桥接。旧连接和旧请求回调不改变当前盘口，溢出、断档和重连关闭可见性并重新同步。私有流登录续期、轮换、业务关联和完整进程接线仍继续实现。

本阶段新增组合模型独立于已有账户、物理日志和传输模型。各模型的状态、范围与证据以 `tla/execution_models/` 对应输出为准；旧模型的 PASS 不代替新增组合验证。
