# AGENTS.md — Tickwire workspace 指南

本文件面向在本仓库工作的 AI Agent 与开发者，给出 workspace 级别的架构速览、命令、约定与改动检查清单。每个 crate 目录内另有一份 `AGENTS.md`，描述该 crate 的职责、公共 API、依赖与不变量；改动具体 crate 前**先读它自己的 `crates/<crate>/AGENTS.md`**。

- 项目：Tickwire —— 面向 Binance Spot 与 USDⓈ-M 合约的超低延迟交易系统（Rust）。
- 状态：实现后期。Spot 完整业务链已在 Linux 独立进程跑通（含费用闭合）；部分模块仍为 partial；生产资格与全链路性能尚未验证。以 [`specs/20261020-implementation.md`](specs/20261020-implementation.md) 与 [`specs/INDEX.md`](specs/INDEX.md) 为准。
- 许可：`UNLICENSED`、`publish = false`，当前为私有项目，未授予开源许可。

## 1. 架构速览

多进程隔离架构，数据面与控制面分离：

- **市场数据进程（md）**：`tickwire-md` 装配 `tickwire-market-source` / `tickwire-market-publisher` / `tickwire-binance-public`，把 Binance 公开行情归一化后通过共享内存 ring 发布。
- **策略进程（每策略实例一个）**：`tickwire-strategy-host` 托管基于 `tickwire-strategy-sdk` 的策略，经真实 JOIN 接入 ring 读行情、用 SPSC 通道发规范命令；策略代码不进入执行进程，可独立 Shadow/升级/回滚/退出（`tickwire-release` / `tickwire-handover`）。
- **执行/账户进程（按账户风险域独立）**：`tickwire-execution`（执行原语、ports 边界）、`tickwire-risk`（定点风控）、`tickwire-account*`（账户状态机/编码/日志/发送协调）、`tickwire-gateway`（业务 WAL + 授权 + 同一 TLS 连接持久发送）、`tickwire-binance-account` / `tickwire-binance-execution`（Binance REST/WS 落地与对账）。
- **控制监督与紧急执行进程**：`tickwire-supervisor`（持有/监督子进程与认证控制连接）、`tickwire-guardian`（独立受审计的查询/撤单紧急 owner）、`tickwire-control*`（认证会话 + 三 voter Raft，只管低频配置/发布/执行权）、`tickwire-config-*`（版本化配置、文件持久同步、授权投影）、`tickwire-lifecycle`（签名多域协调器）、`tickwire-replica` + `tickwire-fencing`（mTLS 复制、故障接管与隔离证据）。
- **进程间通信**：`tickwire-shm[-sys]`（Linux memfd 共享内存、单写映射、seals）、`tickwire-ipc`（固定布局 wire ABI）、`tickwire-snapshot`（不可变 topic 快照）、`tickwire-join`（JOIN/快照/激活协调）、`tickwire-control-sys`（SEQPACKET + SCM_RIGHTS 控制通道）。
- **持久化与恢复**：`tickwire-journal`（物理 WAL）、`tickwire-journal-codec` / `tickwire-account-codec`（确定性 CBOR）、`tickwire-business-journal` / `tickwire-account-journal`（业务/账户有序日志）、`tickwire-recovery`（不可变恢复依赖闭包）。
- **总装二进制**：`tickwire-app`（文件配置装配的实际运行时）。

宏观架构图与低延迟设计详解见 [`README.md`](README.md)；权威规格入口见 [`specs/INDEX.md`](specs/INDEX.md)。

## 2. crate 分层与依赖方向

60 个 crate 全在 [`crates/`](crates/)，workspace `members = ["crates/*"]`。依赖方向只能自下而上，**不得反向或成环**（`cargo metadata` 应始终为 DAG）：

| 层 | crate | 说明 |
|---|---|---|
| 无内部依赖内核 | `tickwire-book`（唯一 `#![no_std]`）、`tickwire-instrument-index`、`tickwire-ipc`、`tickwire-execution`、`tickwire-risk`、`tickwire-recovery`、`tickwire-venue`、`tickwire-transport`、`tickwire-clock`、`tickwire-config`、`tickwire-journal`、`tickwire-journal-codec`、`tickwire-binance-sbe`、`tickwire-fencing`(仅 clock) 等 | 纯逻辑/原语，无 I/O 或仅单一 OS 边界，热路径无堆分配 |
| OS / FFI 边界 | `tickwire-shm-sys`、`tickwire-control-sys`、`tickwire-clock-sys` | 集中的 `unsafe` 边界（workspace 对其余代码 `forbid unsafe`），大量测试 Linux-only |
| Binance 接入 | `tickwire-binance`、`-public`、`-private`、`-session`、`-clock`、`-quota`、`-account`、`-execution`、`tickwire-book`、`tickwire-binance-sbe` | 协议编解码、签名、公开/私有会话、对账 |
| 行情/IPC | `tickwire-md`、`tickwire-market-source`、`tickwire-market-publisher`、`tickwire-shm`、`tickwire-snapshot`、`tickwire-join` | ring 发布、JOIN、快照 |
| 策略 | `tickwire-strategy-sdk`（唯一带 `[features]`，`default=["runtime"]`）、`-host`、`-example`、`tickwire-release`、`tickwire-handover` | 策略进程与发布/交接 |
| 账户/执行/日志 | `tickwire-account[-codec/-gateway/-journal]`、`tickwire-business-journal`、`tickwire-gateway`、`tickwire-quota`、`tickwire-runtime[-policy]`、`tickwire-lifecycle`、`tickwire-lab` | 风控执行域与运行时托管 |
| 控制/配置/接管 | `tickwire-control[-peer/-raft/-store/-sys]`、`tickwire-config-fs/-journal/-materializer/-service`、`tickwire-supervisor`、`tickwire-replica`、`tickwire-guardian`、`tickwire-app` | 控制面与总装 |
| 测试 | `tickwire-mock` | 确定性外部服务替身（MockSql/Kv/Journal/Exchange/Runtime），仅供测试，禁止进生产 |

各 crate 的一句话职责、上游 tickwire 依赖、对应规格见 [`specs/INDEX.md`](specs/INDEX.md) 功能域索引表与各 crate 内 `AGENTS.md`。

## 3. 统一构建 / 测试 / 格式 / 静态检查

工具链固定为 **Rust 1.99.1**（[`rust-toolchain.toml`](rust-toolchain.toml)，含 `rustfmt`/`clippy`），无需手动切换。

```sh
# 一键：格式检查 + 全 workspace 回归（含 example、bench smoke、compile-fail doctest）+ Clippy
./scripts/check-rust.sh

# 等价的常用单命令
cargo build --workspace
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings

# 单个 crate
cargo test -p <crate>
cargo clippy -p <crate> --all-targets -- -D warnings
cargo fmt -p <crate>

# 本地完整 Spot 业务流程（无需交易所凭据）
cargo run -p tickwire-mock --example spot-roundtrip

# 盘口性能基准（不要把 check-rust 里的 bench smoke 当性能数据）
cargo bench -p tickwire-book --bench depth_apply
```

### Linux 集成测试（共享内存真实行为）

`tickwire-shm-sys` / `tickwire-control-sys` / `tickwire-join` / `tickwire-snapshot` 等的多进程行为只能在 Linux 验证（memfd、seals、映射权限、SCM_RIGHTS、SEQPACKET、peer credentials、1/2/4/8/16/32 消费者）：

```sh
./scripts/check-linux.sh   # 需要运行中的 Docker（macOS 可用 Colima）；缓存 .tools/，构建 target/linux-check
```

protoc：控制 Raft 固定依赖 `raft 0.7.0`，其 `protobuf-build 0.14.1` 需要 protobuf 3.x。[`.cargo/config.toml`](.cargo/config.toml) 已把 `PROTOC` 指向 [`scripts/protoc-raft`](scripts/protoc-raft)（Apple Silicon 使用随依赖提供的 x86_64 工具，需要 Rosetta）；也可用环境变量 `PROTOC` 覆盖。

### 设计契约校验（Python 3.11+）

机器契约位于 [`specs/contracts/`](specs/contracts/)，由 [`scripts/`](scripts/) 下脚本校验，不连 Binance、不读凭据：

```sh
python3 -m venv /tmp/tickwire-env
/tmp/tickwire-env/bin/python -m pip install -r scripts/requirements-design.txt
/tmp/tickwire-env/bin/python scripts/verify-design-contracts.py --openapi  # 汇总 IPC/风险/配置/部署 + OpenAPI
/tmp/tickwire-env/bin/python scripts/verify-ipc-contract.py
/tmp/tickwire-env/bin/python scripts/verify-risk-vectors.py
/tmp/tickwire-env/bin/python scripts/verify-config-contract.py
/tmp/tickwire-env/bin/python scripts/verify-documentation.py
```

### TLA+ / TLC 形式化验证

模型与证据在 [`tla/`](tla/)（执行提交、交易结果记录、广播 ring、控制会话、物理/业务日志、恢复依赖、盘口状态等分层模型）：

```sh
./scripts/check-tla.sh        # 固定 TLC 1.7.4（校验 jar SHA-256，需 Java）
python3 -m unittest discover -s scripts -p test_tlc_coverage.py
```

模型 PASS 只覆盖该层声明的源码锚点、状态域与故障边界；action coverage 与 Rust 代码覆盖率是不同统计。源码改动后先核对 `tla/**/anchors.lock` 的锚点哈希是否失效。

## 4. 代码约定

- **Rust edition 2024**；workspace 级 `[workspace.lints.rust] unsafe_code = "forbid"`。`unsafe` 只允许出现在三个 `*-sys` 风格的 OS 边界 crate 内（它们把不安全封装成安全 API），其余任何 crate 不得引入 `unsafe`。
- **热路径无堆分配、无 I/O**：`tickwire-book` / `tickwire-execution` / `tickwire-risk` 等 reducer/内核转换要求确定性、无分配；测试中有零分配回归。不要在这些路径加 `Vec::new`/`format!`/锁/系统调用。
- **定点数**：金额、价格、风控一律用定点（`tickwire-risk` 的 `Fixed` / 契约校验用 `Decimal`），禁止浮点表示资金；风险数值以 [`specs/contracts/risk-vectors-v1.json`](specs/contracts/risk-vectors-v1.json) 的 golden vectors 为准。
- **固定二进制 ABI**：IPC 布局、字段对齐、保留字节、消息编号由 [`specs/contracts/ipc-wire-v1.json`](specs/contracts/ipc-wire-v1.json) 与 `ipc-vectors-v1.json` 锁定，Rust 侧在 `tickwire-ipc`。**改任何布局/字段必须同步契约 JSON、golden vectors 与 `verify-ipc-contract.py`**，不得私自增删字段或改对齐。
- **有界、确定性编码**：业务/账户记录用有界 CBOR（`tickwire-journal-codec` ≤201 字节、`tickwire-account-codec` 的 `MAX_*` 常量），带截断/单字节变异测试；改动需保持向后兼容向量与逻辑哈希链语义（writer 代次与原订单代次分离）。
- **恢复默认冻结**：恢复重放按原顺序重算准入，完成后保持冻结；恢复失败先冻结旧引擎/旧授权，不自动重发、不自动释放资金。未知意图持续占额。
- **数据面不走共识**：逐单不经过 Raft/远端确认（默认本地 durable intent + 后台异步复制）；Raft 仅协调低频配置、发布与执行权。
- **收据 ≠ 授权**：journal/config 持久化收据只证明落盘，不赋予交易/部署授权；授权来自显式门禁（GateAuthority / PolicyGate / fencing 证明）。
- **配置成功的双收据**：控制 API 改配置必须运行时应用与本地 effective 文件达到同一 revision/hash 才算成功，禁止假成功。
- 命名、错误类型、容量常量优先遵循对应 `specs/implementation/<name>.md`；新代码先补/改规格再实现（见下节）。
- 不提交构建产物：`target/`、`.tools/`、`tla/tools/`、`tla/config.env`、`**/states/` 等已在 [`.gitignore`](.gitignore)。

## 5. 规格与 TDD 工作流（改动一个模块时）

1. 先读 [`specs/INDEX.md`](specs/INDEX.md)（阅读顺序、按域索引、契约清单）与目标模块的 `crates/<crate>/AGENTS.md`。
2. 模块实现规格在 [`specs/implementation/`](specs/implementation/)（机器契约在 [`specs/contracts/`](specs/contracts/)）；**修改行为先改规格**，再按接口 → Red/Green（TDD）→ 实现 → 规格复核推进。新增模块需在 `specs/INDEX.md` 与 `specs/implementation/README.md` 登记。
3. 改 ABI / 风险 / 配置 / 部署：同步对应 JSON 契约并跑相应 `scripts/verify-*.py`；这些 python 向量与 `cargo test` 互为补充，不能互替。
4. 改热逻辑（提交门禁、崩溃回执、经济事实、ring、盘口、账户、控制、日志）：同步 `tla/execution_models/` 模型与源码锚点，代码稳定后重跑 `./scripts/check-tla.sh`。
5. 文档相对链接必须有效；不要引用已删除的 `docs/` 目录，所有架构知识从 `specs/` 与源码获取。

### 改动检查清单（提交前最小集）

每完成一批可验证的开发工作并通过相应检查后，提交并 push 到当前跟踪的远端分支，保存阶段进度；向用户报告提交和推送结果。

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`（涉及共享内存/控制 FD 的改动追加 `./scripts/check-linux.sh`）
- [ ] 受影响的契约校验（IPC/风险/配置/部署 `verify-*.py`）通过，golden vectors 已同步
- [ ] 受影响的 TLA+ 模型锚点更新并 `./scripts/check-tla.sh` 通过（如适用）
- [ ] 相关 `specs/implementation/*.md`、`specs/INDEX.md` 与 crate 内 `AGENTS.md` 已同步
- [ ] 未引入 `unsafe`（除三个 sys 边界 crate）、未在热路径引入分配/I/O、未用浮点表示资金

## 6. 禁忌（不要做）

- 不要修改 `rust-toolchain.toml` 升级工具链、不要放松 `unsafe_code = "forbid"` 或 `-D warnings`。
- 不要让策略代码进入执行进程，不要让逐单交易经过 Raft，不要在数据面引入远端同步确认（`SyncReplica` 仅为显式可选模式）。
- 不要恢复或引用 `docs/`（已删除）；不要编造未经源码/规格核实的性能数字——未实测指标标注“目标值/待验证”。
- 不要把 `tickwire-mock` / `tickwire-lab`（仅 loopback 实验场）接入生产默认路径。
- 不要改动 `target/`、`.tools/`、`.git/`；不要跨账户共享资金/订单状态（执行进程按账户风险域隔离）。
- 不要用“文件解析成功/配置落盘”替代运行时准入、资格与恢复验证。

## 7. 顶层目录

| 路径 | 内容 |
|---|---|
| [`crates/`](crates/) | 60 个 crate（每个目录内有自己的 `AGENTS.md`） |
| [`specs/`](specs/) | 总体设计、实现总览、74 篇模块实现规格、7 个机器契约；入口 [`specs/INDEX.md`](specs/INDEX.md) |
| [`config/reference.toml`](config/reference.toml) | 模拟环境的结构与容量参考（示例入口，非生产部署） |
| [`config/production.template.toml`](config/production.template.toml) | 生产输入模板，`REQUIRED` 必须替换并通过准入/资格验证，否则拒绝上线 |
| [`scripts/`](scripts/) | `check-rust.sh` / `check-linux.sh` / `check-tla.sh` / `verify-*.py` 契约校验，说明见 [`scripts/README.md`](scripts/README.md) |
| [`tla/`](tla/) | TLA+ 模型、配置、TLC 运行证据与源码锚点，见 [`tla/README.md`](tla/README.md) |
| [`.cargo/config.toml`](.cargo/config.toml) | 固定 `PROTOC` 选择 |
| [`Cargo.toml`](Cargo.toml) | workspace、lints、release profile（`overflow-checks=true`、`lto="thin"`、`codegen-units=1`） |
