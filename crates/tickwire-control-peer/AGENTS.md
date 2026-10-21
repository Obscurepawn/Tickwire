# AGENTS.md — tickwire-control-peer

## 职责

控制共识 owner 的有界认证 peer 传输与启动装配层：在非阻塞 TCP 上承载 mTLS 1.3 双向认证、会话绑定帧（Hello/HelloAck/Raft/快照分块）与 `PeerService<Replica<GenerationStore>>`，并提供两个可执行入口：纯 peer 节点（`tickwire-control-peer`）与配置服务（`tickwire-config-server`，即 spec 所称 config_runner，HTTPS + peer 共用同一持久业务/共识 owner）。

## 公共 API 入口

### 库模块

- `frame`（`src/frame.rs`）：`Kind::{Hello, HelloAck, Raft, SnapshotBegin, SnapshotChunk, SnapshotEnd, SnapshotApplied}`、`Frame { header, payload }`、`Header { kind, cluster, group, from, to, incarnation, challenge, sequence }`；常量 `HEADER_BYTES=128`、`MAX_PAYLOAD_BYTES=64MiB`、`MAX_ENTRIES=262144`；`Frame::encode/decode/raft()`。
- `tls`（`src/tls.rs`）：`PeerTls::new(scope, node_id, material, peers)`、`IdentityMaterial`（chain/private/roots/crls DER）、`PeerCertificate { node_id, server_name, leaf_sha256 }`、`AuthenticatedPeer { node_id(), leaf_sha256() }`、`Scope { cluster, group }`、`ALPN = b"tickwire-control-raft/1"`、`peer_uri(scope, node_id)`（`spiffe://tickwire/raft/<cluster>/<group>/<node>`）。
- `service`（`src/service.rs`）：`PeerService<D: PeerDriver>`，`new(driver, listener, incarnation, peers, tls, limits)`、`poll(now, budget) -> PollStatus`、`with_control(f)`、`take_applied(items, bytes)`、`current_membership()`、`replace_trust(tls)`、`status() -> DriverStatus`、`stop()`；`Limits`（握手/帧/空闲超时、有界队列）、`PeerBudget`、`PollStatus { connected, received, sent_bytes, application_backpressure, faults }`。
- `runner`（`src/runner.rs`）：`Mode::{Create, Join, Recover}`、`Configuration { cluster, group, node_id, initial_voters, writer_epoch, stream_epoch, mode, directory, listen, identity, peers, application, max_command_bytes, max_retained_bytes, bootstrap }`、`IdentityFiles/Peer/Application/Bootstrap`、`Prepared { node_id(), assemble() }`、`pub type Server = PeerService<Replica<GenerationStore>>`；`load(path)`、`prepare(config)`、`assemble(config)`、`run(config, turns)`。
- `config_runner`（`src/config_runner.rs`）：`Configuration { schema, peer_config_file, http, deployment, stop_file }`、`Http/Client/Deployment/EvidenceTrust/Issuer`、`Prepared`；`load(path)`、`prepare(config)`、`run(config, turns)`。
- `api_runner`（`src/api_runner.rs`）：`ApiOwner { assemble / assemble_prepared / poll / take_applied / with_control / replace_http_trust / replace_peer_trust / shutdown / http_addr / status }`，HTTPS server 与 peer I/O 共享同一 owner。
- `lane`（单节点对连接与会话）、`snapshot_transfer`（`Offer/Incoming/Outgoing` 分块闭包传输）。
- `Error::{Configuration, Input, Service, Owner, Peer, Api, Transport, Io, Stopped, …}`。

### 可执行入口（`src/bin/`，按文件名自动生成 bin）

- `tickwire-control-peer`：`tickwire-control-peer --config /absolute/node.json [--turns N]`，`runner::load → runner::run`，纯 peer 共识节点循环。
- `tickwire-config-server`：`tickwire-config-server --config /absolute/server.json [--turns N]`，`config_runner::load → config_runner::run`，启动 HTTPS + peer 同一 owner；相对上次快照 applied 前进 256 后经 `with_control` 调 `compact_snapshot()`；正常完成、STOP 标记、致命错误都走 `shutdown`。

## 在 workspace 中的位置

上游 `[dependencies]`：`tickwire-control-raft`、`tickwire-config-service`、`tickwire-journal`；第三方 `protobuf =2.28.0`、`rustls 0.23`（ring）、`x509-parser`、`mio 1.2`（os-poll/net）、`serde`/`serde_json`、`sha2`、`getrandom`；`libc` 在 `cfg(unix)`；`rcgen`/`ring`/`time` 仅 dev-dependencies（测试自签证书 fixture）。

下游消费者：无（二进制装配 crate，仅作为可执行目标被部署，不被其他 crate 以库形式引用）。

## 对应规格

- [../../specs/implementation/control-raft-service.md](../../specs/implementation/control-raft-service.md)（网络服务、会话绑定帧、mTLS/CRL、快照独立传输、背压预算）
- [../../specs/implementation/config-server.md](../../specs/implementation/config-server.md)（config_runner 启动组合、trust 一次加载、shutdown 与 compact 节奏）

## 构建与测试

```sh
cargo build -p tickwire-control-peer
cargo test -p tickwire-control-peer
cargo clippy -p tickwire-control-peer --all-targets -- -D warnings
cargo fmt -p tickwire-control-peer
```

`tests/`：`frames.rs`、`tls.rs`、`session.rs`、`network.rs`、`reconnect.rs`、`snapshot_network.rs`、`api_owner.rs`、`config_startup.rs`、`config_runner.rs`（真实 loopback TCP + 自签证书 fixture，`tests/common/`、`tests/fixtures/` 共享）；少数 Unix-only 权限断言用 `#[cfg(unix)]`，其余跨平台可跑。

## 关键不变量与注意事项

- **mTLS 身份与 peer credentials**：`PeerTls` 固定受信 CA、CRL、正常 server-name 校验、叶证书 SHA-256 摘要钉住与唯一节点 SPIFFE URI；关闭早数据与会话恢复；每次连接重新核验证书与 CRL，`replace_trust` 在下一次 I/O 前撤销不合格连接（`tls.rs`；`control-raft-service.md`）。
- **会话绑定帧**：TLS 完成后双方交换 Hello/HelloAck，两份随机 challenge + 双方身份 + incarnation 派生会话摘要；Raft 帧只接受该会话的下一序号，重连不按随机 incarnation 覆盖既有会话（`frame.rs`、`lane.rs`）。
- **每节点对仅一条有效双向连接**：较小 ID 拨号；重复连接或新 incarnation 只关闭新 lane；既有连接关闭/超时后才允许重连（`service.rs`）。
- **protobuf 入站硬校验**：解码前检查固定字段、唯一性、嵌套 entry 数量与长度；拒绝未知字段、旧 ConfChange、本地 Raft 控制消息；ConfChangeV2 另按规范成员请求校验；快照走独立闭包传输，不从普通 Raft 帧注入（`frame.rs`、`snapshot_transfer.rs`）。
- **有界背压**：`PeerBudget`/`Limits` 限制每轮 accepts/帧/字节/队列；服务预留容量后才调用 drive，核对实际输出数量、字节与两种 applied 水位，网络队列不承担业务结果存储（`service.rs`、`control-raft-service.md`）。
- **启动 trust 一次加载**：`prepare` 在绑定 listener 前读完并解码全部 peer/HTTP/source 材料为 owned 内容；后续路径替换不影响已准备 owner；TLS 私钥要求 group/other 权限为零、普通文件、单硬链接（`runner.rs::read_bounded`、`config_runner.rs`）。
- 本 crate 不实现第二个 `Node` 或任意驱动入口；`ReplicaControl` 只暴露类型明确的提案/引导/安全读。

## 改动检查清单

1. `cargo build -p tickwire-control-peer && cargo test -p tickwire-control-peer` 通过。
2. 帧格式/会话派生/ALPN 改动联动 `control-raft-service.md` 与对端 `tls.rs`/`lane.rs`；Raft protobuf 字段遵循 `tickwire-control-raft`。
3. 启动配置 schema（`node.json`/`server.json`）改动同步 `runner::Configuration`、`config_runner::Configuration` 与 `config-server.md`。
4. 快照分块传输改动联动 `tickwire-control-raft` 的 `generation`/`snapshot` 契约。
5. 本 crate 只做传输与装配，不解释配置业务、不签发部署授权。
