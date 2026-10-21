# AGENTS.md — tickwire-config-journal

## 职责

配置面的本地业务日志：在物理 WAL 上保存固定 cluster/namespace/host/process/slot/locator generation 范围内的配置观察序列，按 `Prepared → Desired → RuntimeObserved → Effective` 四阶段记录。收据只证明观察内容已同步，**不授予运行时授权**，也不接入完整配置 agent。

## 公共 API 入口

- 配置与身份（`src/lib.rs`）：`Scope { cluster, namespace: Identity, host, process, slot: Identity, locator_generation }`、`Config { scope, stream_epoch, writer_epoch, max_operations, max_recorded_bytes }`、`Identity = [u8;16]`、`Hash = [u8;32]`。
- 事实类型：`Version { revision, global_hash, projection_hash, policy_revision, object_hash }`、`Prepared`、`FileFact { render_hash, bytes, device, inode }`、`RuntimeFact { instance, boot, host_boot, exec_generation, applied_event_seq, witness_ref }`。
- `Event::{Prepared(Box<Prepared>), Desired{operation,prepared_ref,file}, RuntimeObserved{operation,desired_ref,fact}, Effective{operation,runtime_ref,file}}`（schema 1 严格 JSON 封套，`deny_unknown_fields`）。
- `Phase::{Prepared, Desired, RuntimeObserved, Effective}`、`Receipt { sequence(), record_hash(), frontier() }`。
- `Journal<S: Store>`（`src/journal.rs`）：`Journal::create(store, config)` / `Journal::recover(store, config, required)`；`record(&Event) -> Receipt`、`validate_event(&Event)`（不写盘预检）、`phase(operation)`、`effective_record() -> Option<&Version>`、`durable_frontier()`、`scope()`、`operation(id) -> Option<OperationView>`、`operations()`、`active_operation()`。
- `OperationView`（`src/ledger.rs`）。
- `Error::{Invalid, Conflict, Capacity, Physical, Encoding, Poisoned}`。

## 在 workspace 中的位置

上游 `[dependencies]`：`tickwire-journal`；第三方 `sha2`、`serde`/`serde_json`。`tickwire-config-fs` 仅出现在 dev-dependencies（真实文件测试）。

下游消费者（全仓 grep `Cargo.toml` 核对）：`tickwire-config-materializer`。

## 对应规格

- [../../specs/implementation/config-local-journal.md](../../specs/implementation/config-local-journal.md)（四阶段顺序、链式引用、重试原收据、writer epoch、恢复边界、收据不授权）
- 物理后端见 [../../specs/implementation/config-journal-store.md](../../specs/implementation/config-journal-store.md)（由 `tickwire-config-fs::JournalStore` 实现）

## 构建与测试

```sh
cargo build -p tickwire-config-journal
cargo test -p tickwire-config-journal
cargo clippy -p tickwire-config-journal --all-targets -- -D warnings
cargo fmt -p tickwire-config-journal
```

`tests/journal.rs` 用真实 `FileStore` 重开校验（跨平台）。

## 关键不变量与注意事项

- **四阶段严格顺序**：每项操作依次 Prepared → Desired → RuntimeObserved → Effective；后三步分别引用前一步完整编码摘要；一个 scope 同时只有一个未完成操作；下一项 Prepared 的 base 精确等于最近 Effective 的完整版本，目标 revision 严格增加（`ledger.rs`、`config-local-journal.md`）。
- **同步重试幂等**：同一步相同字节返回原序号、原摘要、原物理同步前缀；不同内容拒绝；历史幂等映射占用固定预算（`journal.rs::record` + `ledger::check`）。
- **容量**：`Config.max_operations` 合法范围 1..=4096、`max_recorded_bytes` 合法范围 512..=64 MiB（由调用方在范围内给定，含 scope 头部与全部历史；`codec.rs` 校验）；投影对象 `projection_object_bytes` 1..=4 MiB；本地文件上限 8 MiB（`tickwire-config::materialized::MAX_FILE_BYTES`）。
- **恢复先完整物理验证再动文件**：`recover_checked` 在任何截尾/同步前调用应用校验；scope/编码/业务顺序/容量/writer 不符时原文件保持不变；`observed_exec_epoch` 包含已通过校验的未完成批次头，旧 writer 不能截去较新 writer 尾部继续写。
- **poisoned 关闭追加**：追加同步失败后日志关闭后续追加，内存保持已确认进度；重开按真实完整提交前缀重建，已写完整但应答丢失的事件可恢复并返回原收据。
- **收据不授权**：收据证明观察已同步；控制提交、文件当前状态、运行时应用证明由物化 owner 分别验证。

## 改动检查清单

1. `cargo build -p tickwire-config-journal && cargo test -p tickwire-config-journal` 通过。
2. 事件封套/阶段链/引用规则改动联动 `config-local-journal.md` 与 `ledger.rs` 重放校验。
3. 物理批次/frontier 语义遵循 `tickwire-journal`（`tla/execution_models/physical_journal/`）。
4. 新增阶段或事件字段必须同步 codec 严格重编码比对与恢复重放，保证旧日志要么恢复要么显式拒绝。
5. 本 crate 不产生交易权限、不启动进程、不开放运行时闸门。
