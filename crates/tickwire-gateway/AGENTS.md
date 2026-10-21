# tickwire-gateway

## 1. 职责

持久命令准入与单所有者受保护网络发送：组合业务 WAL（`tickwire-business-journal`）、授权服务（`Authorization` trait）、交易所编码器（`Encoder` trait）与**同一 TLS 连接**，完成"先授权与 WAL 同步、再进发送槽、首次接纳前门禁复查"的发送协调。外部无法绕过本层取得可写连接。它是执行命令出网的最后一道协调闸，本身不含账户预算/策略权限实现（由 `Authorization` 提供，无默认放行）。`#![forbid(unsafe_code)]`。

- `src/lib.rs`：`JournaledSender<S, A, E>` 协调器、`Route`/`Config`/`ConnectionConfig`、`Encoder`/`EncodedRequest`、`Authorization`、`Status`。
- `src/binance.rs`：Binance REST 的 `RestEncoder` 与 `TimeProvider` 适配器。

## 2. 公共 API 入口

- `JournaledSender<S: Backend, A: Authorization, E: Encoder>`：`create(backend, config, authorization, encoder, connection_config)`、`recover(backend, config, authorization, encoder, required)`、`submit(record, command, deadline) -> Status`、`poll(now, budget)`、`cancel()`、`status(intent)`、`frozen()`、`journal_position()`、`read(out)`、`register`/`reregister`。
- 配置：`Route{venue, account, environment, product, authority}`、`Config{business, writer_epoch, route, max_intents}`、`ConnectionConfig{address, tls, limits, connect_deadline}`。
- 抽象/值：`Encoder::encode(command) -> EncodedRequest`、`EncodedRequest::new(digest, authority, bytes)`（digest 非零、bytes 非空且 ≤16MiB）、`Authorization{authorize, current_gate, recover}`、`Status{Queued, Sending, AwaitingResponse, Unknown, Aborted}`、`Error`/`Result`。
- `binance`：`TimeProvider::timestamp()`（无 unchecked 本地墙钟默认）、`RestEncoder::new(credentials, time, recv_window_micros, max_http_bytes)` 实现 `Encoder`。

## 3. 在 workspace 中的位置

- 上游依赖：`tickwire-binance`、`tickwire-business-journal`、`tickwire-execution`、`tickwire-journal`、`tickwire-journal-codec`、`tickwire-recovery`、`tickwire-transport`、`tickwire-venue`；dev `rcgen`、`ring`。
- 下游消费者（已核对 Cargo.toml）：`tickwire-account-gateway`（重导出 `ConnectionConfig`/`EncodedRequest`/`Encoder`/`Route`）、`tickwire-binance-execution`（实现 `Encoder`）、`tickwire-binance-account`（测试装置类型）。

## 4. 对应规格

- [../../specs/implementation/gateway.md](../../specs/implementation/gateway.md)（准入顺序、首次门禁、NoBytes/冻结/恢复、组合边界）。
- 命令持久化绑定背景见 [../../specs/implementation/command-binding.md](../../specs/implementation/command-binding.md)。
- TLA+：[../../tla/execution_models/gateway/L0_journaled_sender](../../tla/execution_models/gateway/PLAN.md)。

## 5. 构建与测试

```sh
cargo build -p tickwire-gateway
cargo test -p tickwire-gateway
cargo clippy -p tickwire-gateway --all-targets -- -D warnings
cargo fmt --check -p tickwire-gateway
```

- `tests/lifecycle.rs`（12 项：生命周期、过期门禁、冲突去重、新 writer 恢复、缺失命令对象、中止同步失败）、`tests/binance_rest.rs`（5 项：Spot/USD-M 签名、完整请求、WAL 与首次门禁顺序，loopback TLS 独立验签）；纯 loopback，跨平台，无 Linux-only，无 bench。

## 6. 关键不变量与注意事项

- **submit 顺序**（lib.rs:254-327）：`validate_binding` → `encoder.encode` → 核对 `wire.digest == command.digest()` 且 authority 等于 route.authority → 构造 `PreparedSend` 截止 → `authorization.authorize` 取得同步闭包 → `journal.commit_intent(record, closure)` 成功 → 入 entries → `connection.queue`。**只有同步命令闭包 + WAL 能到达网络队列**；编码失败不产生持久意图或输出。
- **首次接纳前门禁**：`poll` 先推进建连/握手/已有输出，`try_send` 接纳明文前调用 `current_gate` 复查新时间、执行权与账户状态；传输尚未接纳字节时每次重试都重新检查；一旦接纳继续发同一缓冲区，不重建命令。
- **不确定发送与 NoBytes**：零接受被拒先追加同步 `AbortedBeforeSend` 记录再置 `Aborted`；日志同步失败、部分接纳后取消、断线或冲突意图均**冻结协调器**，未中止意图保留为 `Unknown`。
- **去重**：相同意图相同内容返回当前状态，不再次排队；不同内容的相同意图冻结；`max_intents` 限制内存意图数，本层不淘汰去重历史。
- **恢复**：先验证物理与业务日志，再由授权服务验证每个完整闭包并加载原始规范命令（仍跑全部身份/摘要核验）；无持久中止的意图恢复为 `Unknown`，持久中止保持 `Aborted`；恢复实例无网络连接且保持冻结，旧命令不自动重发；可在更大 writer epoch 下恢复，订单原代次不变。
- `AwaitingResponse` 的完成点是本机 socket 接收完整报文，此时仍保留风险占用；读取网络数据不会自动确认订单或释放预留。
- `RestEncoder` 只编 REST；WSS 登录、请求关联、连接轮换在会话层扩展；凭证构造后不可变；`recv_window_micros` 1..=60_000_000。
- 连接、队列、字节进度由同一 `&mut self` 所有者访问。

## 7. 改动检查清单

1. 改准入/发送/恢复顺序时对照 gateway.md 与 TLA+ `L0_journaled_sender` 模型。
2. `cargo test -p tickwire-gateway`（含真实 loopback TLS）全绿；clippy `-D warnings`、`cargo fmt --check`。
3. WAL 刷盘时序/闭包绑定变化联动 `tickwire-business-journal` 与 `tickwire-recovery`；不要在本层实现默认放行授权。
4. 业务 ACK/私有成交/费用入账不在本层，保持组合边界。
