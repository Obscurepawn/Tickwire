# Tickwire 实现与验证

Status: active

目标保留[详细设计](20261020-binance-ultra-low-latency.md)的完整 P0～P5 范围。每个模块依次交付接口设计、先失败后通过的测试、实现规格和源码对应的 TLA+ 模型。用户允许外部存储通过抽象与 MockService 完成本地链路，并要求交易所接入可扩展。

每完成一个可验证阶段即提交并推送远端，保存对应 Specs、测试和模型结果；提交范围限定为该阶段修改，推送后核对远端 SHA。该节奏于 2026-10-20 由用户授权。

## 实施状态

| 范围 | 当前实现 | 后续验收 |
|---|---|---|
| P0 契约与工具链 | Rust 1.99.1 workspace；既有 ABI/风险向量接入 Rust 回归 | 双产品协议 fixtures、SBE/schema 锁定、生产能力核验 |
| P1 行情与 IPC | 批量有界盘口、Spot/Futures 独立序号策略、固定 wire codec；Linux memfd 权限、固定记录 SPSC/广播及 1～32 消费进程测试 | 少量散布改量性能、Arena、bank 轮换、进程死亡回收与业务进程接线 |
| P2 风控与执行 | 定点风险公式、订单状态、预留/去重/发送门禁、服务接口与 Mock | 完整保证金账户/归属、行情风控接入、协议认证/网关、条件订单和限流 |
| P3 发布与恢复 | journal Mock 与冻结恢复原语 | 真实 WAL/闭包、策略升级/回滚、多 exec 协调、对账和配置发布 |
| P4 故障与接管 | 注入存储失败、收据丢失和交易所 ACK 丢失 | 副本、控制共识、guardian、实际隔离与灾难恢复 |
| P5 资格 | 尚未开始 | 测试网、Linux 多进程全链路、延迟、压力、72 小时长跑和受控上线 |

## 模块入口

- [执行内核与服务接口](../docs/implementation/execution-interface.md)，[执行规格](implementation/execution-core.md)。
- [盘口接口](../docs/implementation/book-interface.md)，[盘口规格](implementation/book-core.md)。
- [风险接口](../docs/implementation/risk-interface.md)，[风险规格](implementation/risk-core.md)。
- [IPC 接口](../docs/implementation/ipc-interface.md)，[IPC 规格](implementation/ipc-codec.md)。
- [共享 ring 接口](../docs/implementation/shm-ring-interface.md)，[ring 规格](implementation/shm-ring.md)与[Linux 映射规格](implementation/shm-mapping.md)。
- 形式验证工作区：`tla/`；每层保留模型、配置、实际运行记录和源码锚点。

## 验证与完成要求

基础命令为 `cargo test --workspace`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo fmt --all -- --check`，并保留原设计契约检查。模型使用固定 TLC 版本执行，范围、有限域与未覆盖行为写进各层输出。

当前 `./scripts/check-rust.sh` 在 macOS 通过 202 项测试，`./scripts/check-linux.sh` 在 Linux aarch64 通过 223 项测试，均含 1 项编译失败 doctest。[macOS 输出](../docs/validation/rust-tests.log)、[Linux 输出及 SHM 回归](../docs/validation/shm/README.md)和[执行模块失败回归记录](../docs/validation/execution-tdd-red.log)保留实际证据：

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

## 接续工作

盘口继续分析少量乱序散布更新的成本，并保留当前结构更新收益。共享内存下一步实现生产控制连接、FD 交付、Arena 与 bank 轮换，把当前库级闭环接成独立 md/strategy/exec 进程。真实 journal 与恢复闭包、配置发布、完整风险账户和可替换的交易所网络适配器仍是后续必做项。
