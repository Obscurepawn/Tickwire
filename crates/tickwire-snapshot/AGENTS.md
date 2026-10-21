# tickwire-snapshot

## 1. 职责

不可变归一化市场快照的**认证上下文校验（接收侧）**与 **topic 快照构造（builder 侧）**。它是 JOIN 链路中"冷基线"的装配与验收环节：owner 用 `build_topic[_with_coverage]` 把完整状态记录与深度封成一个不可变 topic 快照；consumer 用 `validate` 在已认证上下文里验收收到的字节，产出 `ValidatedTopic`。`#![forbid(unsafe_code)]`。

- `src/lib.rs`：接收校验 `validate`、`ValidatedTopic`、`Member`、`JoinContext`、`InstrumentSpec`。
- `src/builder.rs`：`BuildRequest`、`InstrumentSnapshot`、`BuiltTopic`、`build_topic`、`build_topic_with_coverage`、`MAX_TOPIC_SNAPSHOT_BYTES`。

## 2. 公共 API 入口

- 常量：`QUOTE = 1`、`TRADE = 2`、`MARK_FUNDING = 4`；`MAX_TOPIC_SNAPSHOT_BYTES = 64 * 1024 * 1024`。
- 接收侧：`InstrumentSpec{instrument_id, instrument_version, required_states}`、`JoinContext{expected_resource, catalog_hash, instruments, join, now_mono_ns}`、`validate(bytes, context) -> ValidatedTopic`；`ValidatedTopic`（`bytes()`/`header()`/`len()`/`is_empty()`/`member(i)`/`check_readiness(now)`）；`Member{entry, header, state_records, depth_levels, coverage}`。
- 构造侧：`BuildRequest`、`InstrumentSnapshot`（`with_states` 切换记录类型）、`BuiltTopic`（`bytes()`/`resource()`/`header()`/`join()`/`file_hash()`/`into_bytes()`）、`build_topic`、`build_topic_with_coverage`。

## 3. 在 workspace 中的位置

- 上游依赖：`tickwire-ipc`（布局/`validate_topic_bundle`）、`sha2`；Linux dev-dependency：`tickwire-shm-sys`、`tickwire-control-sys`（仅 `tests/immutable_linux.rs`）。
- 下游消费者（已核对 Cargo.toml）：`tickwire-join`、`tickwire-market-publisher`、`tickwire-market-source`、`tickwire-md`、`tickwire-strategy-example`、`tickwire-strategy-sdk`、`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/topic-snapshot.md](../../specs/implementation/topic-snapshot.md)（接收校验）。
- [../../specs/implementation/topic-snapshot-builder.md](../../specs/implementation/topic-snapshot-builder.md)（冷路径构造）。

## 5. 构建与测试

```sh
cargo build -p tickwire-snapshot
cargo test -p tickwire-snapshot
cargo clippy -p tickwire-snapshot --all-targets -- -D warnings
cargo fmt --check -p tickwire-snapshot
```

- `tests/builder.rs`（18 项）、`tests/validation.rs`（12 项）为纯字节跨平台。
- `tests/immutable_linux.rs`（2 项）带 `#![cfg(target_os = "linux")]`，走真实 0x0f 封页与 FD 校验，须 `./scripts/check-linux.sh` Docker 内运行。
- 无 bench。

## 6. 关键不变量与注意事项

- **上下文来自认证 offer 与本地注册表，绝不从收到的字节反推**（lib.rs:135 注释）；本函数不检查 FD，FD 权限/seals 由 join/shm-sys 负责。
- 资源身份逐项精确匹配：`ResourceHeader` 与 `context.expected_resource` 相等，resource_id/channel_id/channel_epoch/ring_epoch/feed_generation/topic_id/catalog_version/initial_membership_version/qos 与 `JoinBody` 一致，`catalog_hash` 匹配；大量身份字段必须非零（lib.rs:185-199）。
- 时间窗：`created < now < expires`，且 `now < join.readiness_deadline_mono_ns`；`check_readiness` 只做基线级判断，owner 仍需自行检查 cursor/resource 新鲜度与独立执行门禁。
- 仪器数 1..=4096；`required_states` 只能是 bit 0/1/2（QUOTE/TRADE/MARK_FUNDING）组合，其余位 `UnsupportedFeature`；每个成员快照 ID 唯一，`feed_state == 2` 且所需类别全部观测到才算 ready。
- snapshot_kind 只接受 6（legacy，无 exact side boundary，`coverage=None`）与 7（带 `DepthCoverage`）；kind-7 的 coverage 段必须存在。
- builder：`build_topic_with_coverage` 要求资源 `required_features & 64`（ExactDepthCoverage，feature bit 6），`build_topic` 要求未置位，二者不匹配返回 `UnsupportedFeature`；coverage 数量必须等于成员数；构造出的字节可被 `validate` 往返验收。
- 本 crate 不授予 JOIN Ready，不做业务状态新鲜度裁决。

## 7. 改动检查清单

1. 改成员布局/coverage 语义时同步 `tickwire-ipc` 的 `validate_topic_bundle` 与两份规格。
2. `cargo test -p tickwire-snapshot` 全绿后，Linux 用 `./scripts/check-linux.sh` 补跑 `immutable_linux.rs`。
3. builder 与 validator 必须保持往返一致：新构造格式要在 `validation.rs` 增加拒绝/接受用例。
4. clippy `-D warnings` 与 `cargo fmt --check` 通过。
