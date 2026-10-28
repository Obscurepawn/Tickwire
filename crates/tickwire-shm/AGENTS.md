# tickwire-shm

## 1. 职责

共享内存 bank 的**单所有者发布与借用状态机**，是数据面零拷贝广播链路中"ring 协议"的状态内核 + 实际映射 SDK 两层：

- `src/lib.rs`：纯状态内核 `Producer<M>`/`Reader<B>`，无内存访问、无 OS 权限，只维护序号、成员登记、水位与生命周期（Ready/Active/Frozen）。
- `src/mapped.rs`：实际映射 SDK（`#[allow(unsafe_code)]`，是本 crate 唯一获准解除 workspace `unsafe_code=forbid` 的模块），把状态内核接到 `tickwire-shm-sys` 的 `WriterMapping`/`ReaderMapping` 与 `tickwire-ipc` 的固定记录布局。
- `src/arena.rs`：私有 arena 空闲块/退役队列（4 个 class），按与槽位相同的门禁前缀退役。

规格自述 **Status: implemented-partial**：固定记录广播、单消费者传输、受保护借用与协作退出已完成；arena payload/CRC 必选特性、完整 bank 轮换、pidfd/holder registry、真实业务进程接入、生产缓存行/NUMA 资格仍在 P1/P5。

## 2. 公共 API 入口

- 状态内核：`Producer<const M: usize>`（`new`/`activate`/`join`/`join_ready`/`observe_release`/`claim`/`publish`/`abort`/`prepare_freeze`/`freeze`/`detach`/`slot`/`published`/`membership_version`）、`Reader<const B: usize>`（`new`/`borrow_next`/`release`/`apply`/`start_quiesce`/`quiesced`/`watermarks`/`outstanding`）。
- 值类型：`BankKey`、`SubscriptionKey`、`Watermarks{read,applied,released}`、`Freeze`、`WriteClaim`（`first()`/`last()`，独占批次预留，drop 不发布也不释放）、`ReadLease`、`Quiesced`、`Error`。
- 映射 SDK（mapped.rs 重导出）：`Publisher<M>`（`create`/`key`/`header`/`published`/`membership_version`/`reader_fd`/`join`/`activate_join`/`cursor_watermarks`/`publish`/`publish_market`/`prepare_freeze`/`freeze`/`detach`/`detach_registered`）、`Subscriber<B>`（`cursor_fd`/`watermarks`/`borrow`/`apply`/`start_quiesce`/`quiesced`）、`RecordBorrow`（`sequence()`/`bytes()`/`depth_payload()`）、`MarketMessage<'a>{record, levels}`、`configure_market_arena`、`validate_bank`、`MappedError`。

## 3. 在 workspace 中的位置

- 上游依赖：`tickwire-ipc`（布局/校验）、`tickwire-shm-sys`（memfd/seals/原子映射）、`sha2`。
- 下游消费者（已核对 Cargo.toml）：`tickwire-join`、`tickwire-market-publisher`、`tickwire-market-source`、`tickwire-md`、`tickwire-strategy-host`、`tickwire-strategy-sdk`、`tickwire-lab`、`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/shm-ring.md](../../specs/implementation/shm-ring.md)（implemented-partial，含状态约束与验证记录）。
- 映射/seals 边界见 [../../specs/implementation/shm-mapping.md](../../specs/implementation/shm-mapping.md)（由 `tickwire-shm-sys` 实现）。

## 5. 构建与测试

```sh
cargo build -p tickwire-shm
cargo test -p tickwire-shm          # protocol.rs 状态内核跨平台；join_pending.rs / mapped.rs 仅 Linux
cargo clippy -p tickwire-shm --all-targets -- -D warnings
cargo fmt --check -p tickwire-shm
```

- `tests/protocol.rs`（13 项，纯状态内核，macOS 可跑：满/空/环绕、pending JOIN、最慢消费者、缓存失效、批次原子发布、乱序释放/应用、身份拒绝、冻结、序号上限）。
- `tests/join_pending.rs`（6 项）、`tests/mapped.rs`（11 项）带 `#![cfg(target_os = "linux")]`，依赖真实 memfd/seals/多进程，须用 [../../scripts/check-linux.sh](../../scripts/check-linux.sh) 在固定 Docker 镜像（rust:1.99.1-bookworm）内运行。
- 两套映射测试共用 `tests/common/mod.rs` 的有效固定 ABI FeedState 记录，先经 IPC 编码/解码核验，再检查序号和全部 512 字节；不得随机填充保留字段或放松生产校验来构造传输 fixture。
- 无 bench。

## 6. 关键不变量与注意事项

- **容量与消费者上限**：`Producer::new` 要求 capacity 在 64..=2^20 且为 2 的幂；`M` 必须 1..=32；claim 批次 `count` 1..=64（lib.rs:139、285）。
- **单写与唯一 claim**：同一时刻只能有一个未决 `WriteClaim`（`boundary()` 检查 `OutstandingWrite`）；`publish` 预检整批固定长度与连续序号，写完全部槽位后才 release-store 发布水位；`abort` 只收回预留，不发布。
- **水位与容量门禁**：只有整批最后序号与最小 released 水位之差不超过 capacity 才允许取得 claim；成员变更清空最小水位缓存，普通 cursor 更新只保留保守下界缓存（`observe_release` 不清缓存）。
- **受保护借用（核心别名安全）**：`ReadLease`/`RecordBorrow` 不可复制，guard 内部保存 `NonNull<[u8]>`，字节引用只在 `bytes()`/`depth_payload()` 中生成并与本次借用同寿命，无法逃逸 guard 生命周期；析构推进连续 `released` 前缀，`applied` 是独立的连续前缀；`B` 限制同时未完成两件事的记录数，防止乱序完成表提前复用槽位。`mem::forget` 会保留占用。
- **协作退出（Quiesce/Detach）**：`start_quiesce` 后拒绝新借用，`quiesced()` 要求 `outstanding == 0`；`detach` 消费认证过的 `Quiesced`，且 read==released、applied≤read、released 不低于成员旧值；纯状态内核的证明可由其他 `Reader` 构造，映射层不会仅凭它释放实际消费者容量门禁。超时与 socket EOF 都没有 detach 权限。
- **Freeze**：`prepare_freeze` 固定 successor/cutoff，`freeze` 要求无未决 claim 且 `published == cutoff`；冻结后 bank 只读，现有 guard 仍可读，后续发布被拒；跨 bank 切换的 successor/快照关系由控制模块建立。
- **JOIN pending**：`join` 在当前发布边界登记 pending 并立即纳入容量门禁；`join_ready` 必须在资源与快照 ReadReady 绑定验证后调用。
- mapped.rs 是唯一允许 unsafe 的接缝；新增映射访问必须先建立可信身份并保持单写/借用约束，不得在其他模块开放裸指针。
- arena：最多 4 个 class，`required_features & 2`，class_id 1..=4、block_bytes 128 对齐、payload_limit 单调递增 ≤65536；退役走 gated prefix。

## 7. 改动检查清单

1. 状态机改动先跑 `cargo test -p tickwire-shm protocol`（纯内核），再进 Linux 跑 `join_pending`/`mapped`。
2. 共享内存发布/回收交错模型见 TLA+ [../../tla/execution_models/broadcast_ring/L0_publish_borrow_reclaim](../../tla/execution_models/broadcast_ring/PLAN.md)，改发布/释放/回收/Freeze 交错时同步核对模型有限范围。
3. 触及 mapped.rs 映射协议时，对照 shm-ring.md 的已实现/待实现边界，不要把 P1/P5 项（完整 bank 轮换、pidfd/holder registry）提前当作已完成。
4. `cargo clippy -p tickwire-shm --all-targets -- -D warnings && cargo fmt --check -p tickwire-shm`。
