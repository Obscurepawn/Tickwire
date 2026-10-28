# tickwire-recovery 接手说明

## 1. 职责（架构位置：恢复闭环）

不可变恢复依赖保存与本地私有验证闭包：`BlobStore` 抽象、内容寻址对象、manifest 编解码、输入前缀（TWINP001）编码、交接封口（HandoverSeal）与 `VerifiedClosure` 能力对象。`Persistence::publish` 先验证并同步全部依赖对象才产出 `VerifiedClosure`；重启后 `verify` 重读绑定、重核内容并确认同步完成。`VerifiedClosure` 无法由调用方直接构造，是"恢复授权"的唯一载体。

架构位置：恢复闭环的对象/清单层，向上供 `tickwire-runtime`（LocalAuthority）、`tickwire-release`、`tickwire-replica`、账户系与策略 SDK 使用；本身不做交易授权。

## 2. 公共 API 入口

- 常量：`Hash=[u8;32]`、`COMMAND_DEPENDENCY_KIND=13`、`ACCOUNT_BASELINE_DEPENDENCY_KIND=14`、`ACCOUNT_HISTORY_DEPENDENCY_KIND=15`、`MAX_OBJECT_BYTES=64MiB`、`MAX_CLOSURE_BYTES=256MiB`、`MAX_MANIFEST_BYTES=1MiB`、`MAX_OBJECTS=4096`、`MAX_EDGES=16384`。
- 绑定与清单：`ClosureBinding`（risk_domain_id/intent_id/intent_exec_epoch/business_digest/strategy_generation/config_revision/recovery_object）、`RecoveryMode`（DeterministicReplay/DeclaredRebuild）、`InputBoundary`（stream/first_sequence/previous_hash/last_sequence/last_hash）、`DependencyObject`（kind/content_hash/byte_length/schema_id/owner_identity）、`RecoveryManifest`（binding/logical_strategy_id/instance_id/action_grant_ref/operation_ref/mode/fault_model_id/pin_id/input_boundary/objects/edges）。
- 能力对象：`VerifiedClosure`（`binding()`/`manifest_root()`/`command_digest()`/`account_baseline_digest()`/`account_history_digest()`）。
- 存储：trait `BlobStore`（`get`/`put_sync`）、`StoreKey`（Object/Manifest/Binding/ReplicaEntry/ReplicaStream）、`SharedBlobStore`（Arc<Mutex>，锁只覆盖单次本地文件操作，不跨网络持有）、`MockBlobStore`（`fail_next_put` 注入同步失败）、`FileBlobStore`/`FileBlobReader`。
- `Persistence<S: BlobStore>`：`new`/`into_store`/`put_object`/`read_object`/`read_manifest`/`publish`/`verify`/`bind_replica_stream`/`put_replica_entry`/`read_replica_entry`。
- 辅助：`digest()`（SHA-256）、`binding_key()`、`encode_inputs`/`EncodedInputs`/`InputRecord`（inputs 模块）、`HandoverSeal`（handover 模块，`validate`/`bytes`/`decode`/`roots`/`verify_checkpoint`）。

## 3. workspace 位置

- 上游依赖：sha2 0.10.9、libc 0.2、serde、serde_json；**无 workspace crate 依赖**。
- 下游（grep `Cargo.toml` 核对）：`tickwire-account-codec`、`tickwire-account-gateway`、`tickwire-account-journal`、`tickwire-app`、`tickwire-binance-account`、`tickwire-binance-execution`、`tickwire-business-journal`、`tickwire-gateway`、`tickwire-handover`、`tickwire-lab`、`tickwire-release`、`tickwire-replica`、`tickwire-runtime`、`tickwire-strategy-example`、`tickwire-strategy-host`、`tickwire-strategy-sdk`。

## 4. 对应规格

- [../../specs/implementation/recovery-closure.md](../../specs/implementation/recovery-closure.md) — 恢复依赖保存与验证主规格（BlobStore、publish/verify、绑定不可变）。
- [../../specs/implementation/account-recovery-binding.md](../../specs/implementation/account-recovery-binding.md) — kind 14/15 账户基线/历史成对绑定与 kind 13 完整命令。

## 5. 构建与测试

```sh
cargo test -p tickwire-recovery
```

- `tests/closure.rs`：16 个测试（完整闭包 publish/reopen、命令依赖保留、账户对绑定、缺字节不可修补、绑定不可变/同字节幂等、缺 kind/环/错绑定拒绝、输入覆盖来自真实字节、同步失败不产绑定、重验证重建完成、canonical 拒绝截断/非最小 schema、分片按序且拒绝缺口重叠、畸形输入字节、描述符/图边界先于 blob 读取、部分发布重试、文件后端重开销钉与 corruption 检测）。
- `tests/handover_rebuild.rs`：v4 安装/回退根、缺安装与混用旧字段拒绝、v1–3 序列化兼容。
- 测试夹具：`tests/manifest.hex`。

## 6. 关键不变量与注意事项

- **publish/verify 同步后才出能力**：`publish` 先 `check()`（逐对象读回、校验长度与摘要、重建完成），再 canonical 编码、写 manifest、写 binding→root 销钉，最后才构造 `VerifiedClosure`；`verify` 重新走读回+check+再同步。调用方无法绕开同步直接拿到能力对象。
- **必备依赖集合**：kind 1..=12 必须全部出现（`kinds & 0xfff == 0xfff`）；kind 1 准入记录摘要必须等于 `ClosureBinding.business_digest`；kind 4 等于 `action_grant_ref`；kind 7 等于 `operation_ref`；kind 13（命令）最多一个。
- **账户依赖对**：出现 kind 14（基线）就必须同时有 kind 15（历史前缀）且各恰好一个，并必须配一个 kind 13 完整命令；缺任一项返回 `Invalid("account dependency pair")`。旧 manifest 没有 14/15 时两个 digest 为 `None`，调用方（账户协调器）必须拒绝。
- **文件写锁生命周期**：`FileBlobStore` 的唯一写 owner 正常 Drop 显式解锁，临时继承到未 exec 子进程的 FD 不延长 owner 寿命；共享包装器仍只在最终内层 store 销毁时释放。存活 owner 互斥、旧副本不能解锁替代 owner 由安全 FD 副本回归检查。
- **对象有序去重**：objects 按 `(kind, content_hash)` 升序，逆序或重复返回 `Invalid("object order/duplicate")`；`read_object` 校验读回字节长度与 `digest == content_hash`。
- **依赖图无环**：edges 为 (dependent, dependency)，索引越界/自环/逆序拒绝；Kahn 拓扑遍历未访问全部节点即 `Invalid("dependency cycle")`。
- **输入边界连续**：kind 12 对象经 `inputs::inspect` 解析后按 first_sequence 排序，必须与 `input_boundary` 的 stream、首序号、previous_hash 逐段衔接，末段正好到 last_sequence/last_hash；缺口、重叠、流不一致均拒绝。
- **绑定不可变**：同一 binding_key 重试写完全相同字节是幂等成功；内容或订单身份不同返回 `Conflict`；binding→root 是永久销钉。
- **容量先于读盘**：对象数/边数/总字节（≤MAX_CLOSURE_BYTES）校验在读取 blob 之前完成。
- **声明重建封口**：HandoverSeal v4 要求 `rebuild_installation_digest`，独立回退完成后增加 `rebuild_rollback_digest`；preflight/replay 旧字段全部为空，旧版本不接受重建字段，缺省字段不改变 v1–3 字节。底层格式允许初始 READY 尚无回退根；授权交接、复制和旧源恢复入口另要求完整安装与回退图。
- **replica 定位独立命名空间**：`ReplicaEntry`/`ReplicaStream` 用独立域分隔的 key，stream 全零或 sequence 0 拒绝。

## 7. 改动检查清单

1. `cargo test -p tickwire-recovery` 全绿。
2. 新增 dependency kind 时，同步更新 `validate_shape` 的必备位掩码、account-recovery-binding.md 与 `tickwire-runtime::LocalAuthority::prepare` 的对象装配。
3. 改 canonical 编码（codec.rs）必须保持确定性（字段顺序/无多余空白），旧 manifest 字节不得被新解码器破坏。
4. 改 `BlobStore` 语义时注意 `SharedBlobStore` 锁不得跨网络 I/O 持有；`MockBlobStore.fail_next_put` 只模拟一次同步失败。
5. 联动检查 `tickwire-release`（rebuild/replay 闭包）与 `tickwire-replica`（前缀镜像）对 VerifiedClosure 三个 digest 的消费。
