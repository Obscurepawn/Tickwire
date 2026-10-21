# AGENTS.md — tickwire-control-store

## 职责

控制面的持久请求登记层：在物理 journal（`tickwire-journal`）上落盘控制请求的准入（Received）与完成（Completion），为 `tickwire-control::RequestStore` 提供生产可用的 `DurableRequests` 实现。它只保存"某 subject/epoch/request_id 的请求已收到、终态响应是什么"这一事实，不做协商、不派发、不解释业务结果。

## 公共 API 入口

- `Config { physical: tickwire_journal::Config, writer_epoch: u64, owner: [u8;16], max_requests, max_pending, max_response_bytes, max_retained_bytes }`（`src/lib.rs`）：`validate()` 检查 writer_epoch、非零 owner、`max_pending <= max_requests`、响应帧长 320..=65536、保留字节预算。
- `DurableRequests<S: Store>`：
  - `DurableRequests::create(store, config)` / `DurableRequests::recover(store, config, required: Option<&PhysicalFrontier>)`。
  - 观察方法：`position() -> u64`、`poisoned() -> bool`、`pending() -> usize`。
  - 实现 `tickwire_control::RequestStore`：`lookup`、`insert_received(key, digest) -> bool`、`complete(key, digest, result)`。
- 记录封套：magic `TWCR0001`，tag 2 = Received（81 字节定长），tag 3 = Completion（state_version + 响应长度 + 响应体）。
- 错误直接复用 `tickwire_control::Error`。

## 在 workspace 中的位置

上游 `[dependencies]`：`tickwire-control`、`tickwire-ipc`、`tickwire-journal`。

下游消费者（全仓 grep `Cargo.toml` 核对）：`tickwire-md`、`tickwire-strategy-host`、`tickwire-app`。

## 对应规格

无专门实现文档。登记语义与一次交付规则见 [../../specs/implementation/control-session.md](../../specs/implementation/control-session.md)（本 crate 是其 `RequestStore` 的持久实现，规格正文不单独描述本层）。

## 构建与测试

```sh
cargo build -p tickwire-control-store
cargo test -p tickwire-control-store
cargo clippy -p tickwire-control-store --all-targets -- -D warnings
cargo fmt -p tickwire-control-store
```

`tests/durable.rs` 使用真实文件 `FileStore` 重开校验，跨平台可跑（不依赖 Linux 子进程）。

## 关键不变量与注意事项

- **先持久化后返回成功**：`insert_received` 先 `journal.append_batch(writer_epoch, &[Received])` 成功才在内存表插入并返回 `true`；`complete` 同样先落盘 Completion 再推进内存态（`lib.rs::append/insert_received/complete`）。
- **持久写失败即 poison**：任何 `append_batch` 错误把 `poisoned = true`，之后所有 `lookup/insert/complete` 返回 `Store` 错误，本实例不再推进；调用方需重开（`lib.rs::append`）。
- **重复 Received 幂等、冲突拒绝**：已存在同 key 时 digest 相同返回 `false`（不二次派发），digest 不同返回 `Conflict`（`insert_received`）。
- **Completion 一次性**：`complete` 先校验响应帧（flags 10|18、request_id/epoch/target/owner、`fd_count == 0`、StatusBody.state_version 一致），再要求旧记录 digest 匹配且未完成；已完成只接受字节完全相同的重复，否则 `Conflict`（`validate_completion/complete/restore`）。
- **恢复重建完整表**：`recover` 要求首条为配置封套、序号连续、exec_epoch 单调不降且不超过 writer_epoch；tag 2 重放为 pending，tag 3 重放完成并扣减 pending；重复 Received、重复完成在恢复路径同样拒绝（`lib.rs::restore`）。
- **容量**：`max_requests` 控制条目数、`max_pending` 控制未完成数（完成落盘后才释放）、`max_retained_bytes` 限制已落盘记录总量（`room`/`append`）。
- 本 crate `#![forbid(unsafe_code)]`，不持有网络与会话。

## 改动检查清单

1. `cargo build -p tickwire-control-store && cargo test -p tickwire-control-store` 通过。
2. 记录封套格式（`TWCR0001`、tag、字段偏移）改动必须同步 `recover`/`restore` 的重放逻辑，保证旧日志可恢复或显式拒绝。
3. 登记/去重/一次交付语义联动 TLA 模型 `tla/execution_models/control_requests/`（`L0_registry` pending/双请求场景）。
4. 物理批次与 frontier 语义遵循 `tickwire-journal`（`tla/execution_models/physical_journal/`），不要在本层另造同步语义。
5. 本 crate 不解释响应业务结果，不做授权与派发。
