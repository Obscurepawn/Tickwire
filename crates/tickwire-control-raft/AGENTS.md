# AGENTS.md — tickwire-control-raft

## 职责

控制面共识层：基于固定三 voter 的持久 Raft 适配器（`raft 0.7.0`，Safe ReadIndex、pre_vote、check_quorum），把 Ready 的 entries/HardState 先持久化到物理 journal 再允许发送消息，并向业务层输出已提交命令的提交事实。本层只保证 Raft 日志与水位的确定性，不解释命令业务含义；业务接受/拒绝由上层 reducer 保存。

## 公共 API 入口

- `Config { physical: tickwire_journal::Config, node_id, voters: [u64;3], writer_epoch, max_command_bytes, max_retained_bytes }`（`src/lib.rs`）。
- `Node<S: Store>`：
  - 生命周期：`Node::create(store, config)` / `Node::recover(store, config)`。
  - 驱动：`drive_budget(budget: driver::ReadyBudget) -> Output`、`drive_with_snapshot(budget, restore: &mut R)`、`drive()`（默认 128 轮）、`tick()`、`campaign()`、`step(peer, message)`、`report_snapshot(peer, success)`、`report_unreachable(peer)`。
  - 提案/安全读：`propose(command) -> Proposal`、`request_read() -> ReadId`、`take_read(&ReadId) -> Option<ReadBarrier>`、`cancel_read(&ReadId) -> bool`。
  - 状态：`status() -> driver::DriverStatus`、`current_membership() -> driver::Membership`、`is_leader()`、`configured_voters()`、`applied_index()`、`snapshot_base_index()`、`history() -> Vec<Applied>`、`pending_applied_bound() -> driver::OutputBound`。
  - 快照：`supports_snapshots()`、`step_snapshot(peer, message, VerifiedSnapshot)`、`snapshot_cut(application_scanned)`、`current_snapshot()`、`installed_generation()`、`matches_installed(receipt)`。
- 输出类型：`Applied { index(), term(), command() }`、`Output { messages, applied, more_ready }`、`Proposal { matches(applied) }`、`ReadId`/`ReadBarrier { term(), index() }`。
- 模块：
  - `driver`：`ReadyBudget`、`DriverStatus`、`Membership`、`Role`、`OutputBound/OutputCharge`、`DriverOutput<A>`、trait `PeerDriver`、trait `RetainedSize`。
  - `membership`：`MembershipRequest/MembershipReceipt/MembershipProposal/MembershipAction`，`propose_membership`、`membership_receipt`、`peer_session`、`create_joining`、`retired()`。
  - `generation`：`SnapshotDirectory/StagingSnapshot/VerifiedSnapshot/GenerationStore`（staging→seal→compact 的快照代次目录）。
  - `snapshot`：`SnapshotLimits/ObjectRef/SnapshotCut/ApplicationRoot/InstalledGeneration/SnapshotDriveError`，trait `SnapshotRestore/SnapshotWriter/SnapshotReader`。
  - `storage`（私有）：`DurableStorage` 实现 raft `Storage`，Begin/Chunk/End 大更新写物理 journal。
- 重导出 `raft::eraftpb::{Message, MessageType}`；`Error::{Identity, Bounds, Store, Corrupt, NotLeader, NotReady, Protocol, Poisoned}`。

## 在 workspace 中的位置

上游 `[dependencies]`：`tickwire-journal`；第三方固定 `raft = 0.7.0`（`protobuf-codec`）、`protobuf = 2.28.0`、`slog`、`getrandom`、`sha2`。

下游消费者（全仓 grep `Cargo.toml` 核对）：`tickwire-control-peer`、`tickwire-config-service`。

## 对应规格

- [../../specs/implementation/control-raft.md](../../specs/implementation/control-raft.md)（持久边界、先持久化后发送、Safe ReadIndex、大更新 Begin/Chunk/End）
- 网络承载见 [../../specs/implementation/control-raft-service.md](../../specs/implementation/control-raft-service.md)（由 `tickwire-control-peer` 实现，本 crate 提供 `PeerDriver` 边界）

## 构建与测试

```sh
cargo build -p tickwire-control-raft
cargo test -p tickwire-control-raft
cargo clippy -p tickwire-control-raft --all-targets -- -D warnings
cargo fmt -p tickwire-control-raft
```

**protoc 依赖**：`raft 0.7.0` 的 `protobuf-codec` 在依赖构建期调用 protoc（protobuf 3.x）。仓库 `.cargo/config.toml` 已把 `PROTOC` 指向 `scripts/protoc-raft`（优先系统 libprotoc 3.x，否则用 registry 内随附的 3.x 编译器；Apple Silicon 走 x86_64 经 Rosetta，自定义环境可用 `PROTOC=/path/to/protoc` 覆盖）。macOS/Linux 本地单测可跑（`tests/cluster.rs` 三 voter 真实文件提交恢复、`tests/membership.rs` 成员变更、`tests/snapshot_ready.rs` 快照就绪）。

## 关键不变量与注意事项

- **先持久化后发送**：每个 Ready 的 entries 与 HardState 先 `persist` 落盘，之后才把 messages 放入输出；LightReady 的 commit 推进也先持久化（`lib.rs::drive_inner`）。持久写失败使本实例 `poisoned = true`，本轮消息全部不返回。
- **applied 水位**：控制历史的 applied 记录同步完成后才推进 Raft applied；`history()` 只输出 index ≤ applied 的非空 EntryNormal 命令，业务 reducer 另行保存接受/拒绝结果。
- **安全读**：`request_read` 走 Safe ReadIndex；`take_read` 要求同 term 且 `store.applied >= ReadState.index` 才返回 barrier；leader 变更时 `forget_stale_reads` 清空挂起读（`lib.rs`）。
- **提案幂等比对**：`Proposal::matches` 用 term+index+命令 SHA-256 三元组核对，不允许只按 index 认账（`lib.rs::Proposal`）。
- **入站消息硬校验**：`step` 检查 from/to、term 非零且 <2^63、只放行七种 Raft 消息、Entry 仅 EntryNormal/EntryConfChangeV2、命令 ≤ `max_command_bytes`、Append entries 连续且 term 单调；Heartbeat commit 不得越界（`lib.rs::step`）。
- **快照只接受本 owner staging 已材料化的闭包**：`step_snapshot` 核对 manifest 发送方/会话/term/ membership，Raft 接受后才让应用 prepare→install；`snapshot_cut` 要求 `application_scanned == store.applied`（`lib.rs::step_snapshot/snapshot_cut`）。
- **固定三 voter**：`configured_voters()` 绑定本持久存储的 `[u64;3]`；成员替换走 ConfChangeV2 持久条目，joint 多 quorum。
- 本 crate `#![forbid(unsafe_code)]`；不开放第二个 `Node` 构造或任意驱动入口。

## 改动检查清单

1. `cargo build -p tickwire-control-raft && cargo test -p tickwire-control-raft` 通过；protoc 环境异常时先检查 `PROTOC` 与 `.cargo/config.toml`。
2. 持久化/水位/大更新分块语义联动 `tickwire-journal`（`tla/execution_models/physical_journal/`）与 `control-raft.md`。
3. 成员变更（ConfChangeV2/joint/retired）改动需同步 `membership.rs`、`step`/`apply` 的双 quorum 校验与恢复路径。
4. 快照格式改动联动 `generation.rs` staging/seal/compact 与 `snapshot.rs` 的 `SnapshotRestore` 契约。
5. 本 crate 不做网络收发、不解释命令业务、不签发授权。
