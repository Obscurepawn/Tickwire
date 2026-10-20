# Binance 现货与合约生产交易系统

Status: done (v0.3 design artifacts)
Architecture direction: accepted
Owner: 用户与后续实现者
Updated: 2026-10-20

## Goal

完成 Rust 对接 Binance Spot 与 USDⓈ-M 超低延迟生产系统的详细设计基线，包括复制/主备、二进制 ABI、风控公式、生产参考部署，以及配置文件和控制 API。后续实现必须使用单元测试、集成测试和 vibe-tla；它属于下一开发阶段。本轮不以首个版本或实现简单为理由省略生产要求。

## Context

仓库没有交易引擎、Cargo workspace 或 TLA+ 模型。用户要求本地 Markdown 与适量图片，并明确要求说明进程职责及交互。在审阅后，用户确定多进程隔离、策略独立更新/新增、多策略并行，以及 Disruptor 式共享广播 ring 作为行情主方案。

## Scope

[主设计](../docs/design/binance-ultra-low-latency.md)、[共享内存机制](../docs/design/shared-memory-ipc.md)、[IPC ABI](../docs/design/ipc-abi.md)、[策略生命周期](../docs/design/strategy-lifecycle.md)、[复制与接管](../docs/design/replication-and-failover.md)、[风险计算](../docs/design/risk-calculations.md)、[部署与 SLO](../docs/design/deployment-and-slo.md)、[配置契约](../docs/design/configuration.md)和[验证方案](../docs/verification-plan.md)共同构成生产目标。交付范围包括 Markdown、图、机器可读契约、TOML 样例和设计校验脚本。

## Requirements and constraints

- 行情、各策略实例、各账户执行域、supervisor、guardian 独立进程；策略代码不进入 exec。
- 多策略可独立加入、升级、回滚和退出，OMS/交易连接不因策略更新重启。
- 行情共享广播 ring 单份 payload、多读者游标/gating/barrier；活引用不可覆盖。
- 慢读者以冻结旧 bank、切换健康读者和有证据回收处理，资源有界，耗尽按 topic/QoS 失效。
- 意图/回报独立通道，exec 单写资金与订单，未知意图持续占额，策略移交不重复发单或释放资金。
- 默认本地 durable_intent＋后台异步复制，逐单不经过 Raft 或远端确认；副本缺少可能已发送尾部时冻结。跨主机接管需要完整证据、实际隔离和对账。
- 全系统统一配置 schema；控制 API 更改配置后，目标运行时与本地 effective 文件达到同一 revision/hash 才成功；计划、授权投影、滚动换代和目录迁移有恢复协议。
- 完整延迟包含 IPC、调度、风控、持久化/复制与网络发送；不拿组件数字替代全链路。
- Rust、操作系统多进程行为与 TLA+/TLC 分别提供证据，不互相冒充。

## Acceptance criteria

以下验收针对本轮**详细设计补全**，实现与上线门禁仍未执行。AC-1～AC-10 为此前已交付的架构项；本轮新增 AC-11～AC-16。

- [x] AC-1：保留本地文章、币安官方协议及三个开源系统的可追溯依据。
- [x] AC-2：主设计同时覆盖现货与合约，并写清账户、协议和风险差异。
- [x] AC-3：订单未知、日志提交、实际隔离和恢复屏障有具体行为与不变量。
- [x] AC-4：定义多进程测量边界、消费者规模、发布/故障压力与完整链路 SLO。
- [x] AC-5：单元、多进程集成与 vibe-tla 的测试/模型任务覆盖生产目标。
- [x] AC-6：所有 Markdown、图源、图片和核验记录对应当前设计，检查通过。
- [x] AC-7：多进程与多策略为主架构，无单进程“先做再说”的上线替代路径。
- [x] AC-8：Disruptor 广播包含发布/释放、动态加入、依赖屏障、bank 轮换与故障回收。
- [x] AC-9：独立策略发布有存量订单/持仓原位移交、代次授权、回滚及跨 exec 协调。
- [x] AC-10：交叉审阅发现的问题已经修订，源码/库保证与设计推导清楚分开。

- [x] AC-11：复制与主备已确定算法/副本/提交/成员变更/恢复/fencing协议，并有机器可读配置。
- [x] AC-12：IPC ABI 有精确布局、消息编号、类型/错误码、兼容矩阵和可核验golden向量。
- [x] AC-13：风险计算给出公式、单位、舍入、来源和机器可核算数值案例。
- [x] AC-14：参考生产部署包含具体容量/SLO/故障预算，实际账户输入有类型和校验，未填写不能上线。
- [x] AC-15：详细设计跨文档一致，布局/算术/配置/链接核验与独立审阅完成。
- [x] AC-16：配置覆盖全部进程与资源；API 有版本、幂等、权限、发布和回滚，成功包含运行时应用与本地文件持久化，崩溃、继任实例、投影和目录迁移可恢复。

## Plan

| Task | Owner | Status | Notes |
|---|---|---|---|
| 既有本地/协议/开源调研 | 研究 Agent | done | 保留固定提交和来源 |
| SHM/Disruptor/iceoryx2 源码调研 | opensource_rust | done | 固定版本、权限、借用、ABI 与生产边界 |
| 多策略生命周期 | local_latency | done | 原位移交、回滚和多域协调 |
| 生产主设计、IPC 和验证方案 | 主 Agent | done | 按用户最新要求重写 |
| 架构/进程/恢复/发布图 | binance_protocols | done | 同步广播 ring、guardian 和独立 QoS |
| 复制/主备详细设计 | replication_v03 | done | 本地提交、异步副本、源封口和控制 Raft |
| IPC ABI 与 golden vectors | ipc_abi_v03 | done | 36 个布局、624 个字段、63 个向量 |
| 风控计算与数值案例 | risk_v03 | done | 21 个案例、131 项预期结果；修正空头损失和未来持仓退出费用 |
| 配置与控制 API | configuration_v03 | done | 16 个 API 操作、本地同步、权限投影、计划内继任和目录迁移 |
| 参考部署与详细设计整合 | 主 Agent | done | 具体容量/SLO、完整设计契约校验和交叉审阅 |
| P0～P5 实现与上线资格 | 未分配 | not started | 不能从文档完成推断实现完成 |

## Decisions

- 2026-10-20：现货与 USDⓈ-M 同时覆盖，Rust 实现，单元/集成/vibe-tla 为强制开发证据。
- 2026-10-20：用户要求本地 Markdown、适量图片，以及明确进程、职责和交互。
- 2026-10-20：用户否定以单 trader 简化部署，要求策略独立更新/新增；采用多进程主架构。
- 2026-10-20：用户明确多策略必须进入文档，并要求按真实上线系统设计，不以首版复杂度做妥协。
- 2026-10-20：用户偏好 Disruptor 式共享广播 ring；采用共享 payload、多游标和安全 bank 轮换，意图/回报保留独立通道。
- 2026-10-20：现有 Rust disruptor/rtrb 不能直接跨进程；固定 Tickwire ABI 的 SHM transport 为主实现方向，iceoryx2/Aeron 等作为证据与对照。
- 2026-10-20：用户明确关注重共识开销；默认采用本地持久意图＋后台异步复制，数据面不使用 Raft。控制 Raft 只协调低频配置、发布和执行权；异步缺尾部进入 EvidenceLost/Frozen，SyncReplica 仅作为显式提交模式。
- 2026-10-20：用户要求全系统灵活配置，并通过控制面接口修改；API 更新必须同步本地文件。采用全局版本、逐进程权限投影、运行时和文件双收据，禁止假成功。

## Current state

v0.3 详细设计已完成：复制/主备、最终 IPC ABI、风险计算、生产部署/SLO，以及全系统配置与控制 API 均有具体契约。配置原四项审阅问题已逐项复核闭合。设计脚本通过 36 个布局/63 个向量、21 个风险案例/131 项结果、17 个配置负例和完整 OpenAPI 校验；测试计划共有 30 项单元/性质用例与 65 项集成用例。

这里的 done 只表示设计交付完成。Rust 交易引擎、配置服务、真实多进程测试、延迟测量及 TLC 尚未实现或执行。

## Changed files

README.md；docs/design/ 的八份设计文档；docs/verification-plan.md；docs/research/ 中的主设计版本入口；docs/assets/ 图源、导出与核验文件；docs/design-review.md；specs/contracts/；config/；scripts/；本规格与 INDEX。

## Verification

命令、计数和当前图字节的核验结果见[设计交付核验](../docs/design-review.md)及[脚本说明](../scripts/README.md)。机器结果保存于 docs/validation/；Rust/集成/TLC 的计划用例尚未运行。

## Risks and open questions

参考配置、算法、协议和默认 SLO 已固定。实际部署仍要提供真实主机/NUMA、账户模式与能力、标的目录、资本与费用/档位证据、凭据引用、隔离后端和资格报告；这些是生产模板中显式必填的输入，不是尚未决定的架构选项。状态迁移与策略资源合同由具体策略发布文件提供。

## Next step

后续实现从 P0 的真实协议能力、部署输入和实验合同验证，以及 P1 的 Rust workspace/IPC 状态机开始。使用本设计的 golden vectors 建互通测试，按真实源码建立 TLA+ 模型，并推进 P2～P5 的执行、配置、发布、恢复与完整性能资格。
