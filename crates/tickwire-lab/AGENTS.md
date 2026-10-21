# tickwire-lab 接手说明

## 1. 职责（架构位置：显式闭环实验室 / 集成编排）

仅 loopback 的显式部署实验室，**永不被生产默认加载**（lib.rs 首句："Explicit loopback-only deployment laboratory; never loaded by production defaults"）。负责在本机/独立 Linux 文件系统上编排 spot-fill、spot-handover、spot-handover-custom 三个确定性场景：生成密钥与场景材料、启动本地受限交易所对端（fixture，仍走真实 TLS/HMAC 编解码）、按生产签名协调器与 Host 调度进程、派生 release 文件、校验安装成本。

架构位置：集成/验证层，不是服务组件；二进制 `tickwire-lab` 由 `scripts/run-core-chain-linux.sh` 或开发者显式调用。

## 2. 公共 API 入口（库 + CLI）

- `lib.rs`：`Result`、`Scenario`（`read(path)` 强制 loopback IP 与绝对路径）、`ScenarioKind`（`SpotFill`/`SpotHandover`/`SpotHandoverCustom`，`handover()` 判定）、`Config`、`Service`。
- `generate`：`generate`（spot-fill）、`generate_handover`、`generate_custom_handover`（Ed25519 密钥/场景材料落盘并打印 JSON）。
- `fixture`：`serve(&Scenario)` 本地受限交易所对端（rustls + HMAC，真实客户端编解码）。
- `launch`：`run(&Scenario)` 进程 owner、`child_exec(config, gate, binary)` 子进程执行入口。
- `handover`：围绕生产签名协调器与 Host 的实验室调度（`relay_tests` 模块内联）。
- `preflight`：release 文件由旧子进程已认证、持久化 cut 派生；回调完成只由已启动 worker 与回滚探针产生。
- `flavor`：显式实验室策略选择（生产 worker 自选注册表）；`model`：账户/场所本地模型。
- CLI（`main.rs`）：
  - `tickwire-lab generate [--scenario spot-fill|spot-handover|spot-handover-custom] --out <dir> --bin-dir <dir>`
  - `tickwire-lab serve --config <scenario.json>`
  - `tickwire-lab run --config <scenario.json>`
  - `tickwire-lab child-exec <config> <gate> <binary>`（内部入口）

## 3. workspace 位置

- 上游依赖（Cargo.toml 实测）：workspace — `tickwire-account`、`tickwire-account-codec`、`tickwire-account-journal`、`tickwire-app`、`tickwire-binance-quota`、`tickwire-clock`、`tickwire-clock-sys`、`tickwire-fencing`、`tickwire-lifecycle`、`tickwire-ipc`、`tickwire-shm`、`tickwire-market-source`、`tickwire-md`、`tickwire-recovery`、`tickwire-release`、`tickwire-replica`、`tickwire-runtime-policy`、`tickwire-strategy-example`、`tickwire-strategy-sdk`、`tickwire-transport`、`tickwire-venue`；第三方 — ring、rcgen、rustls（TLS1.2）、serde、serde_json、sha2、sha1、base64。
- 下游：**无 crate 依赖本 crate**（独立二进制，grep 全仓仅命中自身）。

## 4. 对应规格

**无专门实现文档。** 场景语义在 [../../specs/implementation/strategy-handover.md](../../specs/implementation/strategy-handover.md) 中以"实际进程场景由 tickwire-lab 负责"描述（旧 Active 单 ACK 后暂扣成交、动态启动新 Shadow、核对同一账户前缀、签名 Prepare/Commit/Release、旧 PID 退出、管理权恰好一次、迟到成交新管理者单次入账）。crate 内有 `README.md`（crates/tickwire-lab/README.md）记录本地与 Linux 运行方式。

## 5. 构建与测试

```sh
cargo test -p tickwire-lab
cargo run -p tickwire-lab -- generate --out /tmp/lab --bin-dir target/debug
cargo run -p tickwire-lab -- run --config /tmp/lab/scenario.json
```

- `tests/install_cost_probe.rs`：2 个诊断测试，默认 `#[ignore]`，需要 `TICKWIRE_INSTALL_COST_FIXTURE` 指向保留的 Linux 运行产物；不是时序断言。
- `src/handover/relay_tests.rs`：2 个内联测试（经 `#[cfg(test)] mod relay_tests;` 接入）。
- 独立 Linux 全链路：`scripts/run-core-chain-linux.sh [spot-fill|spot-handover|spot-handover-custom] [dev|release]`（docker `rust:1.99.1-bookworm` 固定 digest，结果落 `.tools/lab-results/<run-id>/`；`.tools/` 不纳入交付）。

## 6. 关键不变量与注意事项

- **loopback-only**：`Scenario::read` 校验 `listen`/`mirror` IP 均为 loopback、`root`/`bin_directory` 为绝对路径，否则拒绝（"not a loopback fixture scenario"）；不得改成可绑外部地址。
- **不合成回调完成**：preflight 的回调完成证据只由真实启动的 worker 与回滚探针产生，实验室不得伪造。
- **release 文件派生自旧子进程已认证持久化 cut**：见 preflight.rs 模块注释；不得从内存快照直接打包。
- **确定性场景**：三个场景的顺序、签名、单次管理权移交由调度代码固定，不用固定等待时间代表完成（strategy-handover.md 要求）。
- **本地对端仍走真实协议栈**：fixture 用 rustls/ring HMAC，客户端认证与编解码不被替身绕过。
- 本 crate 不被生产默认加载；任何把 lab 当作服务依赖的接线都应拒绝。

## 7. 改动检查清单

1. `cargo test -p tickwire-lab` 全绿；需要保留产物的探针测试手动带 `TICKWIRE_INSTALL_COST_FIXTURE` 跑。
2. 改场景流程后，用 `scripts/run-core-chain-linux.sh spot-handover dev` 在独立 Linux 文件系统复核（macOS 本机只覆盖 spot-fill 类闭环）。
3. 改签名/交接顺序时联动 `tickwire-lifecycle`（Prepare/Commit/Release 签名）与 strategy-handover.md。
4. 保持 loopback 校验与"不伪造回调"两条红线；新增场景必须在 README.md 登记。
5. 依赖增删后核对 Cargo.toml 与 README.md 的本地运行命令一致。
