# AGENTS.md — tickwire-replica

## 职责

故障接管面：有序前缀复制与完整本地闭包验证。源端（`SourceWorker`）从物理日志同步导出能力取输入，经 mTLS 网络把连续前缀复制到镜像（`Mirror`）；镜像先同步实际对象与完整闭包、再同步连续批次、最后发布前缀并签发 ACK。默认 `LocalDurableAsync`，`SyncReplica` 为可选同步模式。

## 公共 API 入口

- 输入前缀（`input.rs`）：`InputPrefix`、`InputEntry`、`INPUT_PREFIX_MAGIC`、`MAX_INPUT_RECORDS`。
- 源端（`source.rs`）：`CommitMode::{LocalDurableAsync{max_unreplicated_age_ns}, SyncReplica{mirror_id, assignment_epoch}}`、`SourceConfig { stream, profile_version, mode, max_records, max_pending_batches, max_pending_bytes }`、`SourceWorker`、`LocalPrefixProof`、`CommitEvidence`。
- 镜像（`mirror.rs`）：`MirrorConfig`、`Mirror<T: Store, S: BlobStore> { create/recover/create_files/recover_files, prefix(), stream(), is_closed(), is_poisoned(), hello(), has_object(), object_chunk(), publish_manifest(), append(batch, signer), ack(sequence, signer), original_records(), verify_source_complete(), materialize_input[_with_config](), verify_sync_complete(), close_sync() }`、`CompletePrefix { final_inputs(), prefix(), recovery_seal_hash(), fence_evidence_hash() }`、`PeerHello { prefix(), receiver_boot_id() }`、`MirrorRecoverySeal::verify`。
- 网络（`network.rs`）：`NetworkLimits`、`PeerClient::new/handshake`、`MirrorListener::bind/local_addr/mirror/poll`、`SourceSender { new/disconnect/upload_final/reconnect/poll }`、`WorkerProgress`。
- 协议（`protocol.rs`）：`ObjectRef`、`Format`、`Stream { validate/bytes/digest }`、`ManifestRef/Entry`、`Prefix { genesis/advance }`、`AppendBatch { bytes/decode/validate }`、`Signer { from_pkcs8/from_seed/public_key }`、`Signed { payload/bytes }`、`Ack`；常量 `MAX_BATCH_RECORDS=32`、`MAX_BATCH_BYTES=64KiB`、`CHUNK_BYTES=4MiB`。
- TLS（`tls.rs`）：`ClientTls/ServerTls/TlsIdentity`。
- 配置（`config.rs`）：`ReceiverConfig::from_file(path).open()/open_store()`、`NetworkConfig::build()`、`TlsFiles { client(server_name)/server() }`、`read_bounded`。
- worker（`worker.rs`）：`ExportWork/WorkerEvent/WorkerHandle/spawn_source_worker`。
- 重导出 `tickwire_journal::{ClosedJournal, DurableExport}`、`tickwire_recovery::{BlobStore, FileBlobStore, Hash, Persistence}`。
- 恢复/交接：`restore.rs`（恢复目录进行中标记、完成清单核对）、`handover.rs`（`HandoverSeal` 闭包逐条校验）、`association.rs`。
- `Error::{Invalid, Bounds, Gap{next}, Missing, Conflict, Frozen, Signature, Io, Journal, Recovery}`。

### 可执行入口

- `src/bin/tickwire-replica-receiver.rs` → bin `tickwire-replica-receiver`：`tickwire-replica-receiver CONFIG.json`，`ReceiverConfig::from_file → open` 后循环 `poll`。

## 在 workspace 中的位置

上游 `[dependencies]`：`tickwire-journal`、`tickwire-recovery`、`tickwire-handover`、`tickwire-release`、`tickwire-fencing`、`tickwire-clock`、`tickwire-venue`、`tickwire-binance-account`、`tickwire-account`、`tickwire-account-codec`、`tickwire-account-journal`（见 `Cargo.toml`）；第三方 `rustls 0.23`（ring）、`ring`、`serde`/`serde_json`、`sha2`。

下游消费者（全仓 grep `Cargo.toml` 核对）：`tickwire-app`、`tickwire-binance-execution`、`tickwire-lab`。

## 对应规格

- [../../specs/implementation/replica.md](../../specs/implementation/replica.md)（连续前缀复制 11 条规则：同步导出、固定提交模式、元数据先同步、对象/闭包先于批次、ACK 绑定、容量上限、输入前缀、恢复封口、冻结恢复、完成边界分离、恢复目录标记）

## 构建与测试

```sh
cargo build -p tickwire-replica
cargo test -p tickwire-replica
cargo clippy -p tickwire-replica --all-targets -- -D warnings
cargo fmt -p tickwire-replica
```

单测在 `src/tests.rs`（`#[cfg(test)] mod tests`），无 `tests/` 目录；mTLS/真实网络行为由集成环境覆盖。

## 关键不变量与注意事项

- **输入只能来自物理日志同步导出**：原 payload、序号、writer epoch 不能由调用方另外提供或替换（replica.md 规则 1）。
- **固定提交模式**：复制流固定提交模式、配置版本与源身份；同一源身份的持久配置拒绝重绑定；异步流不能生成同步恢复封口（规则 2）。
- **元数据先于前缀发布**：每个序号的复制元数据（所选输入前缀、闭包根）在发布本地前缀前同步；恢复复用原元数据，不能用新输入水位改写旧哈希（规则 3）。
- **镜像顺序**：先同步实际对象和完整闭包，再同步连续复制批次，最后发布前缀并签发 ACK；冲突、缺口、同步错误都不增加已确认水位（规则 4）。
- **ACK 绑定**：绑定源流、序号、链摘要、依赖摘要、镜像身份、启动代次、assignment；重连不能继续采用旧镜像启动代次的 ACK（规则 5）。
- **容量不丢责任**：源端实时任务、结果、对象组装均有上限；容量耗尽拒绝新任务或返回原任务，不丢弃旧责任后继续发送（规则 6）。
- **恢复封口要求新鲜隔离证据、两个已关闭写者及全部依赖**；最终输入前缀不能省略未转成账户记录的已持久输入（规则 8）。
- **完成边界分离**：账户同步收据、复制证据、执行授权、外部隔离证据分别保留；持久 ACK 不代替网络或凭据隔离（规则 10）。
- 传输采用双向认证加密（`tls.rs` mTLS）。

## 已知边界 / TODO

- `specs/INDEX.md` 标注本 crate 为**部分实现**：源/镜像、mTLS、协议、worker/接收者入口与默认异步已落地；**实际隔离（外部 fencing 设备回读）与故障接管验收尚未完成**，`SyncReplica` 恢复先封闭指定镜像旧入口的完整链路未以生产场景验收。改动接管/隔离路径前先核对 `replica.md` 对应章节与现有测试覆盖。

## 改动检查清单

1. `cargo build -p tickwire-replica && cargo test -p tickwire-replica` 通过。
2. 前缀/批次/ACK/闭包规则联动 `replica.md` 11 条；交接封口联动 `tickwire-handover`，隔离证据联动 `tickwire-fencing`。
3. 协议帧（`protocol.rs`）改动同步对端 `SourceSender`/`MirrorListener` 与 `Signer/Signed` 签名校验。
4. 新增提交模式必须同步 `CommitMode` 校验、恢复路径与 ACK 绑定，不得让异步流产出同步封口。
5. 本 crate 不签发执行授权、不声明账户安全，隔离动作由外部设备完成。
