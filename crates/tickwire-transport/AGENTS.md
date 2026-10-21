# tickwire-transport

## 1. 职责

有界、显式驱动的**非阻塞网络传输**：基于 mio 的 TCP/TLS 连接状态机、HTTP/1.1 与 WebSocket 分帧、FIX 文本编帧、发送预算与首次输出门禁。它是所有出站网络（交易所 REST/WS、控制连接、网关发送槽）的统一传输底座，本身不含业务 ACK、venue 错误、重连、FIX 登录序号或 WS heartbeat 逻辑（那些归会话层）。`#![workspace]` lints，无 unsafe。

- `src/connection.rs`：`Connection`（建连、握手、唯一发送槽、drive 推进）。
- `src/http.rs`：请求编码与响应解码（CRLF 注入/TE-CL 歧义/chunk 限制）。
- `src/ws.rs`：客户端握手、帧编码/解码（mask、控制帧、片段、UTF-8）。
- `src/fix.rs`：FIX BodyLength/CheckSum 文本编帧。

## 2. 公共 API 入口

- `Connection`：`connect(address, security, limits, deadline)`、`register`/`reregister(registry, token)`、`queue(prepared) -> send_id`、`is_ready()`、`send_report()`、`take_finished()`、`cancel()`、`try_send(...)`、`read(out)`、`drive(now, budget) -> DriveReport`。
- 配置/状态：`Limits{max_send_bytes, max_receive_bytes, tls_buffer_bytes, max_accept_bytes}`（默认各 1MiB/16KiB）、`IoBudget{max_syscalls, max_bytes}`（默认 8/65536）、`Security{Plain, Tls{server_name, config}}`、`Failure{Deadline, GateDenied, Cancelled, Disconnected, Io, Tls}`、`SendState{Queued, MayHaveSent, Flushed, NoBytes(Failure), Failed(Failure)}`、`SendReport{id, state, accepted_bytes, total_bytes}`、`PreparedSend::new(bytes, deadline, max_bytes)`、`DriveReport`。
- 分帧模块：`http::{encode_request, Response, ResponseDecoder}`、`ws::{ClientHandshake, Opcode, Message, encode_client, Decoder, PollMessage}`、`fix::encode`。
- 重导出 `mio`、`rustls`。

## 3. 在 workspace 中的位置

- 上游依赖：无 `tickwire-*`；外部固定 `mio=1.2.4`（net/os-poll）、`rustls=0.23.46`（ring、tls12）、`httparse=1.10.1`、`base64=0.22.1`、`sha1=0.10.6`、`getrandom=0.3.4`；dev `rcgen`。
- 下游消费者（已核对 Cargo.toml）：`tickwire-gateway`、`tickwire-binance`、`tickwire-binance-public`、`tickwire-binance-private`、`tickwire-binance-execution`、`tickwire-binance-account`、`tickwire-binance-clock`、`tickwire-account-gateway`、`tickwire-guardian`、`tickwire-md`、`tickwire-runtime`、`tickwire-lab`、`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/transport.md](../../specs/implementation/transport.md)（排队/首次输出/不确定发送/本地完成/有界推进/TLS/HTTP/WS/FIX 行为表）。
- TLA+：[../../tla/execution_models/transport/L0_first_output](../../tla/execution_models/transport/PLAN.md)（首次接受、过期、历史 NoBytes 不复活）。

## 5. 构建与测试

```sh
cargo build -p tickwire-transport
cargo test -p tickwire-transport
cargo clippy -p tickwire-transport --all-targets -- -D warnings
cargo fmt --check -p tickwire-transport
```

- `tests/framing.rs`（12 项，纯分帧，跨平台）、`tests/network.rs`（13 项，loopback 真实 TCP/TLS，rcgen 私有 CA，跨平台）。
- 无 bench；无 Linux-only 用例。

## 6. 关键不变量与注意事项

- **无阻塞 I/O**：生产路径没有阻塞 connect/read/write；地址解析在连接前完成，外部 reactor 管理就绪与再调度；`connect_deadline` 是建连门禁。
- **唯一发送槽**：槽内已有未完成发送时 `queue` 返回 `Busy`；槽保存不可变 bytes，未进 TLS 前不算被接受。
- **首次输出门禁**：Ready 且未过期，紧接 `writer.write` 前同步检查门禁，闭包返回后复查截止；传输尚未接受字节时每次重试都重新检查；一旦接受继续发同一缓冲区，不重建命令。
- **不确定发送**：接受任意应用字节后失败只能保留 accepted 并报 `Failed`；`NoBytes` 仅在当前槽零接受时成立；全报文接受且 TLS 待写缓冲清空才 `Flushed`；同次 EOF 不覆盖已完成状态。
- **有界推进**：每次 drive 的系统调用数/字节数/解密交付量有上限；接收容量满后停止读取；`Limits` 任一项为 0 或超 16MiB 硬上限即 `Bounds`。
- **TLS**：根证书与主机名校验正常启用；`enable_early_data` 配置直接拒绝（`Protocol("early data disabled")`）。
- HTTP 拒绝 CRLF 注入、重复长度、TE/CL 歧义、超长 chunk、截断；HEAD 长度不分配 body。WS 校验 Upgrade/Accept、客户端随机 mask、服务端帧标志/最短长度/控制帧/片段/UTF-8，显式区分消息/缺输入/预算耗尽。FIX 校验 BodyLength 定位 CheckSum，错误校验和/残帧/超限拒绝。
- 报文 ID 在**连接内**递增；跨连接业务身份由上层提供。

## 7. 改动检查清单

1. 改发送状态机/首次门禁时对照 transport.md 行为表与 TLA+ `L0_first_output`。
2. `cargo test -p tickwire-transport`（含真实 TLS loopback）全绿；clippy `-D warnings`、`cargo fmt --check`。
3. 不要在本层加业务 ACK/重连/会话状态；保持"准备/解析 ≠ 排队订单被接受"的边界。
4. 升级 mio/rustls 固定版本需同步 Cargo.toml 与规格记录。
