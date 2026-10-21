# AGENTS.md — tickwire-binance-execution

## 职责
Binance 执行适配：在单线程 `Rc<RefCell<State>>` 共享状态中保存原始签名与已编码帧，为 `tickwire-gateway` 提供 `Encoder`（`RestEncoder`），并在首次发送门（`GateAuthority`）内追加原始签名检查、租约/请求期限复检与本地配额消费。它把校准时钟、配额端点与 WS 交易绑定状态粘合在一起，是命令真正发往交易所前的最后一道本地闸门；自身不直接收发网络。

## 公共 API 入口
- `SharedSessionState<C, B>`（`lib.rs`，Clone 共享同一状态，不可跨线程、不可伪造发送权）：`new(clock: C, quota: Endpoint, binding: CommandBinding, budget: B, config: Config)`、`encoder(credentials) -> RestEncoder`、`authority(inner: A) -> GateAuthority<A, C, B>`、`with_resources(f)`、`signed_stamp(digest)`、`websocket_control(...)`、`prepare_websocket/websocket_wire/install_websocket_binding`；实现 `tickwire-binance-account::ws::SessionState`。
- `RestEncoder<'a, C, B>`：实现 `tickwire_gateway::Encoder`（`encode(command) -> EncodedRequest`）。
- `GateAuthority<A, C, B>`：包装底层 `Authority`，在 `current_gate` 中先取内部门再执行 `finish_gate`；`commit_state` 在 WS binding 未就绪时返回 `Pending`。
- Trait：`SessionClock { source(); domain(); sample(); check_send_sample(stamp, recv_window_ns, budget_ns) }`（为 `BinanceTimeClient` 实现）、`DeliveryBudget { remaining_ns(command, stamp, now) -> Result<u64> }`（闭包自动实现）。
- `Config { recv_window_micros, max_http_bytes, max_signed_commands }`；自由函数 `credential_reference(credentials, generation) -> Result<Hash>`（只公开凭据引用摘要，不序列化密钥）。
- `Error::{Invalid, Busy, Identity, Capacity, MissingSignature, Denied, Clock, Quota, Encoding}`。

无 binary、无 example；测试为模块内 `src/tests.rs` 与 `src/tests/replication.rs`（随 `cargo test` 运行）。

## 在 workspace 中的位置
- 上游依赖（`[dependencies]` 中的 tickwire-*）：`tickwire-binance`、`tickwire-binance-account`、`tickwire-binance-clock`、`tickwire-binance-quota`、`tickwire-account`、`tickwire-account-gateway`、`tickwire-gateway`、`tickwire-recovery`、`tickwire-transport`、`tickwire-venue`；外部 `sha2`。
- 下游（已在各 `Cargo.toml` 核对）：`tickwire-app`、`tickwire-runtime`。

## 对应规格
- ../../specs/implementation/binance-execution.md

## 构建与测试
```sh
cargo test -p tickwire-binance-execution
cargo build -p tickwire-binance-execution
cargo clippy -p tickwire-binance-execution --all-targets -- -D warnings
cargo fmt -p tickwire-binance-execution
```
模块内测试覆盖：原始 stamp 与字节在重复编码/门重试后保留、过期签名不被当前校准替换、预算回调在同一最终样本上检查、未编码/变更命令不得获得发送门、策略与租约在全部回调后复检、WS 编码保留 request_id/原始 stamp/掩码帧、WS 门要求真实 binding 帧与当前 owner、真实 TLS 升级/binding 响应/pong、USDM WS 与 REST 对账使用不同 TLS 端点。无 OS 条件编译。

## 关键不变量与注意事项
- **容量上限**：`recv_window_micros` 1–60_000_000；`max_http_bytes` 1–16 MiB；`max_signed_commands` 1–4096，超出 `Invalid`；映射容量由构造参数限定，不删除旧记录后重签（出处：`SharedSessionState::new`、`binance-execution.md`）。
- **身份绑定**：`new` 校验 binding.owner_id/risk_domain_id 非零、account>0、`binding.domain == clock.domain() == quota.consumer().clock_domain()`、source 匹配；`bind()` 硬编码 `venue == 1` 并复检 risk_domain/account/product/environment（出处：`State::bind`）。
- **编码即采样签名**：首次编码时 `clock.sample()` 取 `signed` stamp，校验 source/domain/未过 deadline，再调 `tickwire-binance::request::prepare`；同 digest 重复编码直接返回缓存帧，不重新签名（出处：`encode`）。
- **门内最终检查顺序**：`prepare_gate` 先采样并取预算；内部 `current_gate` 跑完预算/策略回调后，`finish_gate` 复检 stamp/window 未变、`check_send_sample` 最终采样、monotonic 不倒退、未过 lease/deadline，最后 `quota.consume_command`（出处：`finish_gate`、`binance-execution.md`）。
- **WS 绑定校验**：`install_websocket_binding` 核对 plan、SignedWire、frame_bytes 与 frame_hash(SHA-256)；binding 缺失/不匹配时门拒绝（出处：`install_websocket_binding`、`websocket_gate_requires_actual_binding_frame`）。
- **418/429 冻结**：`SessionState::feedback` 收到 418/429 调用 `quota.freeze()`（出处：`feedback`）。
- **可重入失败即 Busy**：`try_borrow_mut` 失败返回 `Busy`，不 panic、不绕过门（出处：`with_resources` 等）。

## 改动检查清单
```sh
cargo test -p tickwire-binance-execution
cargo clippy -p tickwire-binance-execution --all-targets -- -D warnings
cargo fmt -p tickwire-binance-execution
```
改编码路径跑 `original_stamp_and_bytes_survive_repeat_encoding...` 与 `websocket_encoding_keeps_request_id...`；改门逻辑跑 `policy_and_lease_are_rechecked_after_all_callbacks` 与 `budget_callback_expiry...`；改 WS 绑定跑 `websocket_gate_requires_actual_binding_frame...`。执行契约见 `binance-execution.md`。
