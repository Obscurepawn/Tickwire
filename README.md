# Tickwire

面向 Binance Spot 与 USDⓈ-M 的 Rust 低延迟交易系统。

Tickwire 把行情接入、策略计算和账户执行放在独立进程中，通过共享内存传递行情、通过有界通道提交交易意图。系统围绕确定性风控、发送前持久化、可核验恢复和策略独立换代构建，面向需要控制延迟与故障行为的交易基础设施开发。

**当前处于实现后期。** Spot 行情到订单、成交、终态费用核对的完整链路已在 Linux 独立进程中跑通。生产资格、目标主机的全链路延迟以及跨主机故障接管仍待验证。

[实现状态](specs/20261020-implementation.md) · [规格索引](specs/INDEX.md) · [开发约定](AGENTS.md) · [验证脚本](scripts/README.md)

## 项目定位

项目以 Linux 为主要运行平台，使用 Rust 2024 edition 和固定的 Rust 1.99.1 工具链。核心计算采用定点数与有界数据结构；订单簿、风控和执行状态转换保持确定性，热路径避免堆分配与 I/O。系统集成层负责网络、持久化和进程生命周期。

- **进程隔离**：每个策略实例独立运行，执行与账户按风险域隔离。策略代码不进入执行进程，策略换代保留现有订单与交易连接。
- **固定协议**：共享内存 ring 与 SPSC 通道使用固定二进制 ABI；布局、编码与兼容边界由机器契约和 golden vectors 校验。
- **可恢复执行**：下单前同步本地意图与恢复依赖，收到成交后持久化经济事实。重放按原顺序重算状态，恢复完成后保持冻结，经过对账与授权再继续运行。
- **低频控制**：Raft 协调配置、发布与执行权。默认逐单路径采用本地持久化和后台异步复制；需要远端持久确认时显式选择 `SyncReplica`。

## 当前状态

| 范围 | 已实现 | 后续工作 |
|---|---|---|
| Spot 业务链 | 公开行情、真实 JOIN、独立策略、签名 REST、私有事件入账、订单与成交查询、终态费用闭合 | 更多订单类型、会话恢复与故障组合验收 |
| 行情与共享内存 | JSON / Spot SBE 接入，完整深度与精确事件发布，快照加增量追平；同 catalog 内运行时追加标的、真实退出与重新 JOIN | 扩容重启恢复、跨 catalog 变更、死亡回收、生产缓存与 NUMA 资格 |
| 策略换代 | Shadow、完整输入记录、旧版本回放、持久安装与独立回滚探测；自定义 V1→V2 schema3 交接包含跨 bank 扩容输入；schema4 声明重建的同进程安装、独立回退和完整发布 | 多域故障组合、重启恢复、发布冷步骤的异步化 |
| 账户与恢复 | 定点风险、多资产账户、有序 WAL、依赖闭包、冻结恢复与有序前缀复制 | USDⓈ-M 完整业务场景、外部成交及强平 / ADL 等事实分类 |
| 控制与配置 | 认证连接、持久 Raft、mTLS、快照、成员管理、配置对象和本地物化 | 常规配置计划与提交、操作状态和 watch 的业务接线 |
| 部署与性能 | Linux 进程实验场、组件基准、契约校验和分层 TLA+ 模型 | 目标主机压力与尾延迟、掉电 / 断网注入、跨主机隔离接管 |

具体能力与未完成项以[模块规格](specs/INDEX.md)为准。[部署契约](specs/contracts/deployment-reference-v1.json)中的 SLO 是验收目标，组件基准和测试用例数量不作为全链路性能指标。

## 架构

数据面从公开行情进入 MD 进程，经共享内存广播到各策略，再由执行进程完成风控、持久化与发送。Supervisor 持有子进程和认证连接；Guardian 使用独立身份执行受审计的紧急查询与撤单；控制面管理配置和执行权。

![Tickwire 多进程架构：行情、独立策略、按账户隔离的执行进程，以及监督、控制、持久化和复制边界。](assets/architecture.svg)

| 模块 | 主要实现 |
|---|---|
| 行情接入与分发 | `tickwire-md`、`tickwire-binance-public`、`tickwire-market-source`、`tickwire-market-publisher` |
| 策略托管与换代 | `tickwire-strategy-sdk`、`tickwire-strategy-host`、`tickwire-release`、`tickwire-handover` |
| 风控、账户与交易 | `tickwire-risk`、`tickwire-execution`、`tickwire-account*`、`tickwire-gateway`、`tickwire-binance-execution` |
| IPC 与持久化 | `tickwire-ipc`、`tickwire-shm*`、`tickwire-join`、`tickwire-journal`、`tickwire-recovery` |
| 控制与进程管理 | `tickwire-control*`、`tickwire-config-*`、`tickwire-supervisor`、`tickwire-lifecycle`、`tickwire-guardian`、`tickwire-replica` |
| 运行时装配 | `tickwire-app`；本地实验场由 `tickwire-lab` 提供 |

[标的索引](specs/implementation/instrument-index.md)将完整的 `u64` 标的标识映射为进程内部连续索引。固定目录在冷路径构建 mask / gather 索引，查询核对完整原键；动态目录使用只追加索引，预留容量内插入保持已有位置，扩容在冷路径完成。MD 统一绑定本地盘口 key 和各 topic 的发布槽位，策略端用连续盘口集合与复用的深度工作区处理更新。IPC 中的稳定业务 ID 保持不变。每份 bank 的目录不可变，运行时追加通过受信目录发布和新 bank JOIN 完成。

行情发布器为每个 topic 复用 128 KiB 的解码与发布档位工作区，从深度或事件通知进入发布器到写入共享内存，健康路径不进行堆分配。回归覆盖首次事件、JSON/SBE、4096 档增量和 2 MiB 线程栈；网络接收和 WebSocket 帧解析仍有独立的存储开销。接口与预算见[行情发布规格](specs/implementation/binance-market-events.md)，实测范围见[发布路径验证](specs/evidence/20261028/publisher-hot.json)。

[运行时扩容](specs/implementation/market-expansion.md)已接入 MD、执行和策略的持续运行循环。新增来源先通过真实网络预热；全体消费者核验批准后，MD 等待旧游标实际退出，提交完整新基线，再开放重新 JOIN。消费者在自己的持久边界完成后恢复观察与回调。Linux 实验已验证扩容期间四个进程保持原 PID、三个消费者实际切换，以及扩容后的完整策略交接，见[运行证据](specs/evidence/20261028/market-expansion-runtime.json)。当前范围是同一 catalog 内追加标的；进程重启后的重新资格化和 catalog 变更仍待完成。

![运行时行情扩容时序：候选预热、全体消费者 Armed、实际 QUIESCED、新 bank 与 effective 持久化、重新 JOIN 和 Activated 收据。](assets/market-expansion.svg)

发送顺序以持久事实为边界：恢复依赖与 `Admit` 同步完成后，发送 owner 再检查当前门禁并向 TLS 交付字节。交易所输入先进入输入 WAL，账户事实同步后才发布新状态。持久收据证明落盘；交易权限由当前授权、租约和执行权门禁决定。

![订单数据流：策略意图、风险准入、本地持久化、首次发送，以及输入 WAL、账户 WAL 和策略回报的顺序。](assets/order-flow.svg)

schema3 将换代拆成可核对的阶段。旧版本从固定检查点 S 重放到实际切点 C，目标版本在 C 上迁移并应用配置，完成持久安装和独立回滚探测。协调器采纳结果后推进 Commit / Release，旧进程退出后再激活目标。订单创建者身份保留，管理权移交后由新实例继续处理迟到成交。

![schema3 交接时序：恢复 S、封口 C、旧版本回放、目标安装、独立回滚探测、提交管理权、释放和激活。](assets/schema3-handover.svg)

声明重建（schema4）先由独立 `CaptureOnly` 进程采集真实行情与账户输入，用固定观察器形成有界窗口。封口完成后才固定 Plans 与签名发布操作，Host 核对实际 PID、消费游标、完整依赖和期限。

![声明重建采集时序：真实 JOIN 与账户前缀、固定观察器、认证封口、完整窗口持久化和 Host 核验。](assets/rebuild-capture.svg)

目标策略在同一 PID 内调用真实工厂预热，保存原起点、完整输入链和检查点，再从窗口 C 之后继续处理行情与账户报告。独立旧版本探针完成后，目标保存回退闭包并发出新的 READY；协调器据此推进 Commit / Release，旧实例正常退出后开放目标执行。恢复封口 v4 保留原采集资料、安装根、回退根及后续输入。

Linux release 场景已验证 C10→D285 的连续接续，账户、镜像和策略的 12 条记录一致，迟到成交与费用闭合。安装和回退校验受当前账户、会话和授权期限约束；开发构建曾因冷步骤超时而关闭交接，后续将推进异步化与成本优化。范围和原始摘要见[声明重建规格](specs/implementation/strategy-rebuild.md)与[运行证据](specs/evidence/20261028/rebuild-runtime.json)。

![声明重建安装流程：固定窗口、同进程工厂预热与持久安装、C 后接续、独立回退、真实 READY、协调器发布和旧实例退出。](assets/rebuild-installation.svg)

## 快速开始

准备 Rust（通过 rustup 安装，仓库自动选择固定工具链）。首先构建并运行确定性 Spot 示例：

```sh
cargo build --workspace
cargo run -p tickwire-mock --example spot-roundtrip
```

该示例连接盘口、策略、定点风控、IPC、持久日志和 mock 交易所，覆盖成交、撤单与冻结恢复，无需 Binance 凭据。

完整进程实验使用本地 TLS / WebSocket 替身，实际启动 MD、策略、执行、复制与协调进程。以下脚本需要运行中的 Docker，构建和运行固定 Linux 镜像：

```sh
# Spot 下单、成交和费用闭合
./scripts/run-core-chain-linux.sh spot-fill

# 自定义 V1 → V2 策略的 schema3 交接
./scripts/run-core-chain-linux.sh spot-handover-custom

# 使用优化构建运行相同场景
./scripts/run-core-chain-linux.sh spot-handover-custom release

# 保持进程运行，追加高位 ID 标的，再完成跨 bank 的 schema3 交接
./scripts/run-core-chain-linux.sh spot-market-expansion release

# 声明输入窗口，同进程重建安装与 schema4 发布
./scripts/run-core-chain-linux.sh spot-rebuild-custom release
```

脚本将日志、账户报告和运行结果保存到 `.tools/lab-results/`，保留场景的 Linux volume 供检查。场景定义和原生 Linux 运行方法见 [tickwire-lab](crates/tickwire-lab/AGENTS.md)。

[reference.toml](config/reference.toml)描述模拟环境的结构与容量；[production.template.toml](config/production.template.toml)保留待填写的生产输入。实际进程配置由运行时装配与准入流程读取，格式及入口见 [tickwire-app](crates/tickwire-app/AGENTS.md)。

## 开发与验证

日常开发从目标 crate 的 `AGENTS.md` 和对应实现规格开始。行为变更先更新规格，再以 Red / Green 回归推进；ABI、风险、配置和部署变更同步机器契约。workspace 统一验证入口：

```sh
# 格式、所有目标的回归、文档测试和 Clippy
./scripts/check-rust.sh

# Linux 共享内存、FD 传递与多进程行为；需要 Docker
./scripts/check-linux.sh

# 定向回归与独立性能测量
cargo test -p tickwire-account
cargo bench -p tickwire-book --bench depth_apply
cargo bench -p tickwire-instrument-index --bench lookup
```

设计契约校验使用 Python 3.11+：

```sh
python3 -m venv /tmp/tickwire-env
/tmp/tickwire-env/bin/python -m pip install -r scripts/requirements-design.txt
/tmp/tickwire-env/bin/python scripts/verify-design-contracts.py --openapi
/tmp/tickwire-env/bin/python scripts/verify-documentation.py
```

执行、日志、账户、ring 和恢复等状态机另有 TLA+ 有限模型。模型与源码锚点一同维护，TLC 证据覆盖各模型声明的状态域和故障边界：

```sh
# 需要 Java；脚本校验并使用固定 TLC 版本
./scripts/check-tla.sh
python3 -m unittest discover -s scripts -p test_tlc_coverage.py
```

Raft 构建所需的 protobuf 3.x 由 [protoc-raft](scripts/protoc-raft)选择；Apple Silicon 环境使用随依赖提供的 x86_64 工具时需要 Rosetta。更多环境与单项命令见[脚本说明](scripts/README.md)。

## 文档导航

| 入口 | 内容 |
|---|---|
| [规格索引](specs/INDEX.md) | 按功能域查找模块、实现状态与机器契约 |
| [总体设计](specs/20261020-binance-ultra-low-latency.md) | 进程边界、延迟预算、故障与部署约束 |
| [实现进展](specs/20261020-implementation.md) | 阶段结果、验收证据与待办边界 |
| [模块实现规格](specs/implementation/README.md) | 接口、不变量、错误处理和验证要求 |
| [机器契约](specs/contracts/README.md) | IPC、风险、配置、部署的机器可校验定义 |
| [TLA+ 模型](tla/README.md) | 模型范围、源码锚点、运行方式与证据 |
| [贡献与工作约定](AGENTS.md) | 依赖方向、编码约束、规格与测试工作流 |

源码位于 `crates/`，每个 crate 有独立职责说明。`config/` 保存参考输入，`scripts/` 提供检查和实验入口，`assets/` 保存本文插图及其生成脚本。

## 许可

Tickwire 当前为私有、未发布项目。workspace 声明 `license = "UNLICENSED"`、`publish = false`，尚未授予开源许可。
