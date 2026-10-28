# tickwire-strategy-example

## 1. 职责（架构位置：策略进程侧 —— 静态注册示例策略）

独立的静态注册示例策略 `counting`，携带两个不同状态 schema 的真实可执行体（V1 `0x5457434e0001` / V2 `0x5457434e0002`），用于演示 `tickwire-strategy-sdk` 的注册、恢复、V1→V2 迁移、来源重放与声明重建 probe。注册本身不获得任何交易授权。lib 根文档：`Independent statically registered strategy example. Two actual executables carry different state schemas; neither receives a trading grant by registering.`

## 2. 公共 API 入口

- `Parameters`（`schema/enabled/minimum_observations/minimum_bid_ticks/command`，`bytes()`/`decode()`，只接受 Spot Limit New 命令）、`Factory`（实现 `restore::RestoreFactory` 与 `registered::UserFactory`）、`Counting`（实现 sdk `Strategy`）、`Registration`（`Registration::v1()` / `Registration::v2()`，实现 `sdk::registered::Registration`，`id() = "counting_example"`）、`SourceFactory`（实现 `sdk::replay::application::BusinessFactory`）。
- 常量：`V1 = 0x5457434e0001`、`V2 = 0x5457434e0002`、`MIGRATE = 1`。
- 模块：`rebuild`（从已核验账户前缀重建旧状态的显式 rebuilder）、`chain_tests`（crate 内链测试）。
- bins（`Cargo.toml` 未显式列 `[[bin]]`，按 src/bin 自动识别）：
  - `src/bin/tickwire-counting-worker.rs`：注册 V2 schema，调用 `registered::run(&Registration::v2(), args)`；
  - `src/bin/tickwire-counting-v1-worker.rs`：注册 V1 schema，调用 `registered::run(&Registration::v1(), args)`。
  两个 bin 都复用 SDK 的 `--bootstrap / --source-replay / --restore-probe / --replay-rollback` 模式。

## 3. workspace 位置

上游（`Cargo.toml` 实际声明）：`tickwire-strategy-sdk`、`tickwire-release`、`tickwire-recovery`、`tickwire-venue`；dev：`tickwire-ipc`、`tickwire-snapshot`。

下游（grep 命中）：`tickwire-lab`（实验场引用示例）；本 crate 不被生产运行时直接依赖。

## 4. 对应规格

- [../../specs/implementation/strategy-replay.md](../../specs/implementation/strategy-replay.md)（旧 binary/旧配置 S→C 重放、V1→V2 迁移、安装与回滚 probe 的示例载体）
- [../../specs/implementation/strategy-rebuild.md](../../specs/implementation/strategy-rebuild.md)（`rebuild.rs` 的固定经济动作重建要求：只接受有完整 Admit 证明的动作，缺失证明不推定“未发生”）

## 5. 构建与测试

```sh
cargo test -p tickwire-strategy-example --all-targets
cargo build -p tickwire-strategy-example --bins
```

bin 由合格 Host 以受信 Bootstrap 启动（同 SDK worker 模式），单独运行不获授权。`tests/source_replay.rs` 实际拉起 V1 worker 子进程重放持久输入，核对 PID、成功退出与完整 C 检查点；向量取自 `specs/contracts/ipc-vectors-v1.json`。crate 内单元测试覆盖规范配置拒绝未知字段/空白、C 点迁移保留计数/动作/spent。

## 6. 关键不变量与注意事项

- **一次性经济动作**：达到 `minimum_observations` 后用当前时间与行情 trigger 构造一次命令，`state.spent = true` 后置空；检查点保留原经济意图对应的 action 键。
- **C 点迁移只换 schema**：`State::at_c` 把 `schema` 改为目标值，保留 observations/reports/spent/action；迁移读旧 runtime 参数，新 ConfigApply 之后才换目标参数；新参数改变 action 键时拒绝安装（`Identity`）。
- **命令身份绑定目标**：`Registration::configure` 校验内建命令的 `config_revision == c.revision`、实例 ID、策略代次、逻辑策略与 `restore::Target` 一致，不符返回 `Identity`。
- **重建不推定缺失**：`rebuild.rs` 要求动作集合恰好一个且与 Admit 命令的 economic_intent 完全匹配；缺接受证据不能把历史动作初始化为未消耗。
- **前向重建共用真实工厂**：`UserFactory::begin_rebuild/consumed_actions` 复用既有声明重建的动作初始化与检查点解码；配置版本和完整参数必须与固定输入相同，不支持的模型拒绝。返回的 Counting 由 SDK 注册包装保留同一 Factory，预热计数和 consumed 状态接入持续 Engine，不重新 create 或预热。
- **配置严格解码**：`#[serde(deny_unknown_fields)]`，非规范 JSON（尾部空白）、不支持的产品/订单类型、enabled 却无 command 均拒绝。

## 7. 改动检查清单

1. `cargo test -p tickwire-strategy-example --all-targets` 通过。
2. 改迁移/重放语义联动 [strategy-replay.md](../../specs/implementation/strategy-replay.md)；改重建联动 [strategy-rebuild.md](../../specs/implementation/strategy-rebuild.md)。
3. 新增命令字段/状态 schema 时同步 `tickwire-venue` 规范编码与发布清单 `Capabilities`（本示例固定 Spot / Spot 仓位 / Limit）。
4. 本 crate 是示例，不得在此实现授权、网络发送或风险参数默认值。
