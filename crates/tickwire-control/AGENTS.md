# AGENTS.md — tickwire-control

## 职责

控制面的认证会话与请求登记纯状态层：在可信 launcher 提供的 `Policy` 之上完成 Hello/Welcome 身份协商，维护带租约的单所有者 `Session`，并在 `RequestStore` 上登记控制请求、签发一次性执行票据。本 crate 不做网络、不持有进程句柄、不持久化；传输、内核进程句柄与持久化分别由 `tickwire-control-sys`、`tickwire-supervisor`、`tickwire-control-store` 承担。

## 公共 API 入口

- 身份与策略（`src/lib.rs`）：
  - `Identity`：`instance/generation/role/logical_strategy/host_boot/namespace/build_hash/qualification_hash/manifest_hash/arch/trust_profile`，`valid()` 由内部调用，非零实例、1..=9 角色、非零三类哈希等。
  - `ProcessIdentity { pid, uid, gid }`：来自内核的对端身份，由调用方与 supervisor 活进程注册表比对。
  - `Policy { local, peer, peer_process, required_features, supported_features, control_epoch, lease_duration_ns, max_frame_bytes, nonce }`：可信 launcher 输入，`validate()` 检查双端身份、同 host_boot/namespace、nonce 非零、特征位与帧长（320..=65536）。
  - `encode_frame(envelope, body)`：编码并结构校验完整控制帧（magic `TWCTL001`、crc32c、总长 ≤65536），无状态变更。
- 握手与会话（`src/lib.rs`）：
  - `ClientHandshake::new(policy, request_id)` → `hello()` / `finish(actual_peer, bytes, now) -> Session`（客户端侧）。
  - `Session::accept(policy, actual_peer, bytes, now) -> (Session, Vec<u8>)`（服务端侧，返回 Welcome 帧）。
  - `Session`：`encode(envelope, body)`、`validate(bytes, now) -> ControlFrame`、`renew(nonce, revision, now) -> deadline`、`features()/deadline()/epoch()/peer()`。
- 请求登记（`src/requests.rs`，`pub use requests::*`）：
  - `RequestKey { subject, epoch, request_id }`、`Completion { response, state_version }`、`StoredRequest { digest, completion }`。
  - `trait RequestStore`：`lookup`、`insert_received(key, digest) -> bool`（仅首次插入 Received 返回 true）、`complete(key, digest, result)`（先比 digest，再一次性落完成，或接受字节完全相同的既有完成）。
  - `Session::admit<S: RequestStore>(store, bytes, now, current_version) -> Admission`；`Admission::{Execute(ExecutionTicket), Pending, Completed(Completion)}`。
  - `ExecutionTicket::complete(store, response, state_version)`：校验响应帧的 request_id/epoch/owner/operation/revision/expected_state_version/opcode 映射后调用 `store.complete`。
- `Error::{Wire, Identity, Negotiation, Protocol, Expired, ClockRegression, Revision, Bounds, Conflict, Version, Capacity, Store}`，`Result<T>`。

## 在 workspace 中的位置

上游 `[dependencies]`（`Cargo.toml` 实际声明）：`tickwire-ipc`；第三方 `sha2`。

下游消费者（全仓 grep `Cargo.toml` 核对）：`tickwire-control-store`、`tickwire-supervisor`、`tickwire-join`、`tickwire-market-source`、`tickwire-md`、`tickwire-strategy-sdk`、`tickwire-strategy-host`、`tickwire-app`。

## 对应规格

- [../../specs/implementation/control-session.md](../../specs/implementation/control-session.md)（会话身份与协商、租约、请求登记与一次交付）

## 构建与测试

```sh
cargo build -p tickwire-control
cargo test -p tickwire-control
cargo clippy -p tickwire-control --all-targets -- -D warnings
cargo fmt -p tickwire-control
```

`tests/session.rs` 为纯内存协商/租约/重试单测，macOS 可跑。`tests/transport_linux.rs` 带 `#![cfg(target_os = "linux")]`，用真实 Linux 子进程做 SEQPACKET/SCM_RIGHTS 握手，仅在 `./scripts/check-linux.sh` 的 Docker 环境执行；macOS 上该文件编译为空，不影响 `cargo test -p tickwire-control`。

## 关键不变量与注意事项

- **一次交付票据**：`admit` 仅在 `insert_received` 返回 true 时给出 `Admission::Execute(ExecutionTicket)`；票据不可构造、不可克隆。重复请求经 `lookup` 命中后按 digest 比对：digest 相同返回 `Pending`（已登记未完成）或 `Completed`（终态结果），digest 不同返回 `Conflict`——pending 重试需要 owner 对账，绝不第二次派发（`requests.rs::admit/replay`）。
- **租约与时钟回退永久关闭会话**：`check_time` 中 `now < last_now` 立即置 `closed` 并返回 `ClockRegression`；`now >= deadline` 置 `closed` 返回 `Expired`，已关闭实例所有方法失败（`lib.rs::check_time`）。
- **renew 只接受原 nonce + 严格递增 revision**：`revision <= self.revision` 返回 `Revision`；deadline 从当前 `now` 重新加 `lease_duration_ns`（`lib.rs::renew`）。
- **身份协商双端对齐**：Hello/Welcome 逐字段比对 role/arch/trust_profile/host_boot/namespace/logical_strategy/instance_generation/三类哈希；服务端 `accept` 还校验双方 arch 相同、必需特征位交集、帧长取双方最小值（`lib.rs::Session::accept`）。
- **运行时帧校验**：`check_frame` 逐帧核对 sender/target instance、control_epoch；opcode 1/2（握手帧）在协商后被拒绝；request_id 不允许为零；`expected_state_version`/`operation_revision` 不得越界（`lib.rs::check_frame`）。
- **RequestStore 实现要求**（trait 注释）：生产实现必须先持久化 Received/complete 再返回成功、保留终态结果、原子 insert-if-absent；歧义失败仍可 `lookup` 发现。本 crate 不提供持久化实现。
- 本 crate `#![forbid(unsafe_code)]`，不自行从 HELLO 建立注册表；进程活句柄由 launcher 持有以防 PID 重用。

## 改动检查清单

1. `cargo build -p tickwire-control && cargo test -p tickwire-control` 通过；Linux 握手改动另跑 `./scripts/check-linux.sh`。
2. 帧 ABI（opcode/flags/字段/响应 opcode 映射）改动联动 `tickwire-ipc` 的 wire 布局与 `control-session.md`。
3. 请求登记语义（去重、一次交付、终态保留）联动 TLA 模型 `tla/execution_models/control_requests/`（`L0_registry`、`L1_session`）。
4. 新增协商特征位需同步 `KNOWN_FEATURES`/`REQUIRED_FEATURES` 与对端 `accept`/`finish` 双侧校验。
5. 本 crate 不得引入网络、进程管理或持久化逻辑。
