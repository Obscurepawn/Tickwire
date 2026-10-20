# Tickwire 实现与验证

Status: active

目标保留[详细设计](20261020-binance-ultra-low-latency.md)的完整 P0～P5 范围。每个模块依次交付接口设计、先失败后通过的测试、实现规格和源码对应的 TLA+ 模型。用户允许外部存储通过抽象与 MockService 完成本地链路，并要求交易所接入可扩展。

每完成一个可验证阶段即提交并推送远端，保存对应 Specs、测试和模型结果；提交范围限定为该阶段修改，推送后核对远端 SHA。该节奏于 2026-10-20 由用户授权。

## 实施状态

| 范围 | 当前实现 | 后续验收 |
|---|---|---|
| P0 契约与工具链 | Rust 1.99.1 workspace；既有 ABI/风险向量接入 Rust 回归 | 双产品协议 fixtures、SBE/schema 锁定、生产能力核验 |
| P1 行情与 IPC | 有界盘口、Spot/Futures 独立序号策略、固定 wire codec | memfd/mmap 权限、SPSC/广播、bank 轮换与真实多进程故障 |
| P2 风控与执行 | 定点风险公式、订单状态、预留/去重/发送门禁、服务接口与 Mock | 完整保证金账户/归属、行情风控接入、协议认证/网关、条件订单和限流 |
| P3 发布与恢复 | journal Mock 与冻结恢复原语 | 真实 WAL/闭包、策略升级/回滚、多 exec 协调、对账和配置发布 |
| P4 故障与接管 | 注入存储失败、收据丢失和交易所 ACK 丢失 | 副本、控制共识、guardian、实际隔离与灾难恢复 |
| P5 资格 | 尚未开始 | 测试网、Linux 多进程全链路、延迟、压力、72 小时长跑和受控上线 |

## 模块入口

- [执行内核与服务接口](../docs/implementation/execution-interface.md)，[执行规格](implementation/execution-core.md)。
- [盘口接口](../docs/implementation/book-interface.md)，[盘口规格](implementation/book-core.md)。
- [风险接口](../docs/implementation/risk-interface.md)，[风险规格](implementation/risk-core.md)。
- [IPC 接口](../docs/implementation/ipc-interface.md)，[IPC 规格](implementation/ipc-codec.md)。
- 形式验证工作区：`tla/`；每层保留模型、配置、实际运行记录和源码锚点。

## 验证与完成要求

基础命令为 `cargo test --workspace`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo fmt --all -- --check`，并保留原设计契约检查。模型使用固定 TLC 版本执行，范围、有限域与未覆盖行为写进各层输出。

当前 `./scripts/check-rust.sh` 通过 179 项测试；[原始输出](../docs/validation/rust-tests.log)和[执行模块失败回归记录](../docs/validation/execution-tdd-red.log)保留实际证据：

| 模块 | Rust 测试数 | 覆盖内容 |
|---|---:|---|
| `tickwire-book` | 23 | 22 项同步/存储/边界/参考模型测试，1 项无分配测试；参考模型比较 8,000 批更新、960,000 次价位查询 |
| `tickwire-risk` | 18 | 17 项边界，1 项执行全部 21 数值向量/131 预期字段 |
| `tickwire-ipc` | 110 | 36 布局、63 黄金向量、11 项语义/畸形输入测试；包含全部 25 semantic_cases |
| `tickwire-execution` | 16 | 预算、提交、发送、去重、费用尾款、未知冻结和恢复 |
| `tickwire-mock` | 12 | 11 项服务故障/恢复测试，1 项 Spot 本地闭环 example 测试 |

格式与 Clippy 检查通过。原设计的 IPC、风险、配置、OpenAPI 和部署算术检查通过；文档断链保留原来源 commit 修复后，链接与资产检查通过。盘口性能基准与正确性测试分开运行，原始数据和环境见 [CSV](../docs/validation/book-baseline-macos-arm64.csv) 与 [环境记录](../docs/validation/book-baseline-environment.json)。

TLC 1.7.4 对执行模块的两个有限模型运行了五组配置，均通过不变量检查，所有 Next 动作均生成过后继状态：

| 模型与配置 | generated | distinct | 最大深度 |
|---|---:|---:|---:|
| L0 提交门禁：primary | 21,077 | 2,236 | 15 |
| L0 提交门禁：scaling | 103,216 | 9,076 | 15 |
| L1 崩溃与回执：primary | 6,539 | 1,744 | 14 |
| L1 崩溃与回执：scaling | 417,807 | 96,828 | 22 |
| L1 正常等待回执：pending | 833,051 | 186,148 | 22 |

[L0 记录](../tla/execution_models/submission/L0_commit_gate/03_run_log.md)与 [L1 记录](../tla/execution_models/submission/L1_crash_receipt/03_run_log.md)包含动作覆盖、源码哈希和模型范围。验证覆盖提交后才发送、不可重复发送、收据丢失、崩溃恢复和未知冻结；成交与费用的资金守恒、实际磁盘持久化及进程隔离尚未纳入这两层模型。

全目标完成要以[验证方案](../docs/verification-plan.md)的 U-01～U-30、I-01～I-65、INV-1～INV-14 和 P0～P5 退出条件逐项提供证据。若一个用例目前只覆盖部分场景，仍标记为部分完成。真实主机、账户资格、凭证及隔离后端尚未提供，因此本地 Mock 回归不能填充生产资格项。

已有未跟踪性能审阅文档属于用户工作树内容，本轮不改写或提交。

## 接续工作

盘口沿[开源源码对照](../docs/research/order-book-implementation.md)比较排序批量合并、价格窗口与占用位图，使用同一参考模型与负载验收。随后实现实际 SHM 映射、广播/SPSC 和进程边界，把当前库级闭环接成独立 md/strategy/exec 进程。订单经济事实的 TLA+ L2、真实 journal 与恢复闭包、配置发布和可替换的交易所网络适配器仍是后续必做项。
