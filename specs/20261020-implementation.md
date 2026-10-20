# Tickwire 实现与验证

Status: active

目标保留[详细设计](20261020-binance-ultra-low-latency.md)的完整 P0～P5 范围。每个模块依次交付接口设计、先失败后通过的测试、实现规格和源码对应的 TLA+ 模型。用户允许外部存储通过抽象与 MockService 完成本地链路，并要求交易所接入可扩展。

每完成一个可验证阶段即提交并推送远端，保存对应 Specs、测试和模型结果；提交范围限定为该阶段修改，推送后核对远端 SHA。该节奏于 2026-10-20 由用户授权。

## 实施状态

| 范围 | 当前实现 | 后续验收 |
|---|---|---|
| P0 契约与工具链 | Rust 1.99.1 workspace；既有 ABI/风险向量接入 Rust 回归 | 双产品协议 fixtures、SBE/schema 锁定、生产能力核验 |
| P1 行情与 IPC | 批量有界盘口与小批散布改量优化、Spot/Futures 独立序号策略、固定 wire codec；Linux memfd、不可变快照接收、SPSC/广播及 1～32 消费进程测试；Unix SEQPACKET/SCM_RIGHTS 与认证会话 | JOIN 基线构造与 owner/cursor 协调、Arena、bank 轮换、进程死亡回收与业务进程接线 |
| P2 风控与执行 | 定点风险公式、订单状态、预留/去重/发送门禁、服务接口与 Mock | 完整保证金账户/归属、行情风控接入、协议认证/网关、条件订单和限流 |
| P3 发布与恢复 | journal Mock 与冻结恢复原语；真实物理 WAL/分段/同步恢复；确定性业务事件与逻辑链编码 | 业务日志适配与恢复依赖闭包、策略升级/回滚、多 exec 协调、对账和配置发布 |
| P4 故障与接管 | 注入存储失败、收据丢失和交易所 ACK 丢失 | 副本、控制共识、guardian、实际隔离与灾难恢复 |
| P5 资格 | 尚未开始 | 测试网、Linux 多进程全链路、延迟、压力、72 小时长跑和受控上线 |

## 模块入口

- [执行内核与服务接口](../docs/implementation/execution-interface.md)，[执行规格](implementation/execution-core.md)。
- [盘口接口](../docs/implementation/book-interface.md)，[盘口规格](implementation/book-core.md)。
- [风险接口](../docs/implementation/risk-interface.md)，[风险规格](implementation/risk-core.md)。
- [IPC 接口](../docs/implementation/ipc-interface.md)，[IPC 规格](implementation/ipc-codec.md)。
- [共享 ring 接口](../docs/implementation/shm-ring-interface.md)，[ring 规格](implementation/shm-ring.md)与[Linux 映射规格](implementation/shm-mapping.md)。
- [控制会话](../docs/implementation/control-session-interface.md)、[传输边界](../docs/implementation/control-transport-interface.md)与[物理日志](../docs/implementation/journal-interface.md)；各自规格位于 `specs/implementation/`。
- [不可变资源](implementation/immutable-resource.md)、[Topic 快照接收](implementation/topic-snapshot.md)与[业务日志编码](implementation/journal-codec.md)。
- 形式验证工作区：`tla/`；每层保留模型、配置、实际运行记录和源码锚点。

## 验证与完成要求

基础命令为 `cargo test --workspace`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo fmt --all -- --check`，并保留原设计契约检查。模型使用固定 TLC 版本执行，范围、有限域与未覆盖行为写进各层输出。

上一阶段 `04351db` 的 `./scripts/check-rust.sh` 在 macOS 通过 202 项测试，`./scripts/check-linux.sh` 在 Linux aarch64 通过 223 项测试，均含 1 项编译失败 doctest。[macOS 输出](../docs/validation/rust-tests.log)、[Linux 输出及 SHM 回归](../docs/validation/shm/README.md)和[执行模块失败回归记录](../docs/validation/execution-tdd-red.log)保留实际证据：

| 模块 | Rust 测试数 | 覆盖内容 |
|---|---:|---|
| `tickwire-book` | 30 | 协议/存储/边界、确定性工作量和无分配；8,000 批参考模型、960,000 次查询及独立 40,000 批差分 |
| `tickwire-risk` | 18 | 17 项边界，1 项执行全部 21 数值向量/131 预期字段 |
| `tickwire-ipc` | 110 | 36 布局、63 黄金向量、11 项语义/畸形输入测试；包含全部 25 semantic_cases |
| `tickwire-execution` | 16 | 预算、提交、发送、去重、费用尾款、未知冻结和恢复 |
| `tickwire-mock` | 13 | 12 项服务故障/恢复测试，1 项 Spot 本地闭环 example 测试；含恢复失败仍须冻结旧引擎的回归 |
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
| L2 经济事实 v2：primary | 139,711 | 24,036 | 8 |
| L2 经济事实 v2：long | 1,591,624 | 280,458 | 9 |
| L2 经济事实 v2：boundary | 14,397,539 | 1,495,032 | 11 |
| L2 经济事实 v2：scaling | 14,278,433 | 1,485,288 | 11 |

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

全目标完成要以[验证方案](../docs/verification-plan.md)的 U-01～U-30、I-01～I-65、INV-1～INV-14 和 P0～P5 退出条件逐项提供证据。若一个用例目前只覆盖部分场景，仍标记为部分完成。真实主机、账户资格、凭证及隔离后端尚未提供，因此本地 Mock 回归不能填充生产资格项。

已有未跟踪性能审阅文档属于用户工作树内容，本轮不改写或提交。

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
| 业务日志 codec | 13 项测试、6 个记录/17 个整数黄金向量、591 个截断与 99,840 个单字节变异 | 最多 201 字节确定性 CBOR；原订单代次与 writer 代次分开；稳定业务摘要和绑定 writer 的逻辑链 |

快照通过校验不会自行改变 owner/cursor 状态。业务 codec 也没有生成可直接授权发送的提交证明；真实 WAL、依赖闭包与执行恢复的适配器仍须连接这些已验证原语。

## 接续工作

将已验证的控制连接与 FD 交付接入真实 md/strategy/exec 进程，补齐 JOIN 的不可变快照基线、Arena、bank 轮换与退出回收。物理 WAL 继续接到业务日志和恢复依赖闭包；配置发布、完整风险账户、可替换交易所网络适配器及 P4/P5 验收仍按原目标推进。
