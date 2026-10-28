# tickwire-journal

## 1. 职责（架构位置：持久化恢复链 —— WAL v1 物理帧与同步本地批次）

底层物理 WAL：帧编解码、数据域批次（Begin/DataRecord/Seal）、真实文件 `sync_data`、分段（默认 256 MiB）与 `MANIFEST.json` 恢复。它是上层持久化适配器（账户日志、业务日志、控制/配置日志）的底层组件，本身不含业务授权。lib 根文档：`WAL v1 physical framing and synchronized local batches.`

## 2. 公共 API 入口

- `frame` 模块：`HEADER_BYTES=136`、`TRAILER_BYTES=40`、`FRAME_MAX_BYTES=1MiB`、`MAGIC=b"TWAL0001"`、`END_MAGIC=b"TEND"`；`FrameType`（BatchBegin=1/DataRecord=2/RaftEntry=3/RaftHardState=4/ControlTruncateSuffix=5/SnapshotPublish=6/ControlConfState=7/ControlCommitWatermark=8/DependencyPin=9/DependencyRelease=10/SourceFreezeSeal=11/BatchSeal=12）；`Header{frame_type,flags,stream_epoch,domain,segment_id,lsn,record_seq,exec_epoch,batch_number,previous_hash}`；`DecodedFrame{header,payload,total_len,hash}`；`encode(header,payload)`、`decode(input)`、`crc32c(input)`（纯软件 Castagnoli CRC-32C）。
- `Journal<S: Store>`（`journal.rs`）：`create(store, config)`、`recover(store, required)`、`recover_checked(...)`、`append_batch(epoch, payloads) -> PhysicalSyncReceipt`、`durable_frontier()`、`export_durable() -> DurableExport`、`close_for_recovery() -> ClosedJournal<S>`、`into_store()`、`config()`；`Config{stream_epoch,domain,segment_max_bytes}`（`Config::new`）；`PhysicalFrontier{stream_epoch,domain,segment_id,end_offset,end_lsn,last_seq,exec_epoch,batch_number,seal_hash}`；`PhysicalSyncReceipt::frontier()`；`Record{seq,exec_epoch,payload}`；`Recovery{records,frontier,discarded_tail_bytes,...}`（`observed_exec_epoch()`、`record_frontier(seq)`）；`DurableExport`、`ClosedJournal<S>`。
- `store` 模块：`Store` trait、`FileStore::open(root)`、`segment_path(id)`。
- 常量：`SEGMENT_MAX_BYTES=256MiB`、`BATCH_MAX_BYTES=64KiB`、`BATCH_MAX_RECORDS=32`。
- 无 bin/example。

## 3. workspace 位置

上游：无 `tickwire-*` 依赖（仅 `sha2/serde/serde_json`），是工作区最底层的持久化原语之一。

下游（grep 命中）：`tickwire-account-journal`、`tickwire-account-gateway`、`tickwire-account-codec`(dev)、`tickwire-business-journal`、`tickwire-gateway`、`tickwire-guardian`、`tickwire-lifecycle`、`tickwire-md`、`tickwire-replica`、`tickwire-runtime`、`tickwire-strategy-host`、`tickwire-config-fs/config-journal/config-materializer/config-service`、`tickwire-control-peer/control-raft/control-store`、`tickwire-binance-account/binance-execution`、`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/journal.md](../../specs/implementation/journal.md)（136B 头/40B 尾、8 对齐、12 种 frame type、空 Begin/Seal、64KiB/32 条批、一次 sync_data 才出收据、截断最后未完成批、manifest 孤儿段规则、PhysicalFrontier 绑定）

## 5. 构建与测试

```sh
cargo build -p tickwire-journal
cargo test -p tickwire-journal
```

无 bin/example。`tests/`：`frame.rs`（帧编解码/CRC/边界）、`journal.rs`（批次追加/同步/前缀）、`manifest.rs`（分段目录元数据）、`process.rs`（`#[cfg(unix)]` 真实子进程，在 sync 前后暂停并 SIGKILL，重开核对完整批保留）、`recovery_validator.rs`。

## 6. 关键不变量与注意事项

- **sync 后才出物理收据**：`append_batch` 先做数量/长度/epoch/溢出检查再 I/O；完整批后一次 `sync_data` 成功才返回 `PhysicalSyncReceipt`；I/O 错误后实例 poisoned（`journal.md`）。
- **完整封口批即使未收到收据也保留并重放**：`recover` 先验证所有段与可选外部前缀，才截断最后未完成批并重新同步；空中段、缺 manifest 段、更大段号差都拒绝；允许最多一个 `active_segment+1` 孤儿段。
- **尾部修复**：只截断最后未完成批（`discarded_tail_bytes`），已验证应用前缀在截断/同步前核对。
- **Frontier 是外部证据**：`PhysicalFrontier` 精确绑定域/stream/段/偏移/LSN/末 seq/epoch/批号/seal hash；本次扫描自行取得的前缀不构成独立证据，调用者需从先前收据或已验证外部存储提供。
- `FileStore` 独占文件锁序列化写者，正常 Drop 显式解锁，使并发启动子进程临时继承的 FD 不延长旧 owner 寿命；副本不独立拥有写权限，其后关闭不解除新 owner 的锁。新建目录 0700；调用者负责已有目录权限与可信父目录。
- v1 拒绝非零 flags，padding 必须为零；解码返回消费长度，允许缓冲后紧接下一帧。

## 7. 改动检查清单

1. `cargo build -p tickwire-journal && cargo test -p tickwire-journal` 通过（含 unix 子进程 SIGKILL 测试）。
2. 帧格式/批次/分段改动联动 [journal.md](../../specs/implementation/journal.md)；形式模型 `tla/execution_models/physical_journal/L0_batch_sync_recovery`。
3. 改动 `Store` 契约时同步所有上层后端（account-journal/business-journal/gateway 等）。
4. 本 crate 不做业务校验、授权或网络。
