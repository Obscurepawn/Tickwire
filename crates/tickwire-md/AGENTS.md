# tickwire-md

## 1. 职责

独立 Binance 行情进程与认证全 topic 发布者：每标的一个有界网络 owner，完成准入、订阅、归一化、经 `tickwire-market-publisher` 发布到共享内存，并通过 `tickwire-market-source` 向认证订阅者交付。它是部署形态上的常驻二进制（`src/main.rs`），不是库；库层只提供配置装配、引擎循环与准入服务。`#![deny(unsafe_code)]`，仅 `server` 模块经 `#[allow(unsafe_code)]` discharge 既有接缝。

- `src/config.rs`：文件配置、资源头/客户端模板构造、准入文件读写。
- `src/engine.rs`：`run(c)` 单进程装配与显式驱动主循环。
- `src/server.rs`：认证会话服务、准入登记、`Server::poll`。
- `src/main.rs`：四个子命令 `--config` / `--resource ... --output` / `--client ... --output` / `--admit ... --directory`。

## 2. 公共 API 入口

- `config`：`Asset`、`Instrument`（`build(source) -> InstrumentBinding`）、`Feed`、`EventType`（`kind()`/`state()`）、`EventFeed`（`context(feed)`）、`Topic`、`Config`、`ResourceSpec`（`header_with_exact_events()`/`header()`）、`ClientTemplate::build() -> ConnectConfig`、`read`/`write`/`write_bytes`/`active_resource_path`。
- `engine::run(c: Config) -> Result<()>`。
- `server`：`Admission{client, pid, uid, gid, supervisor, supervisor_pid, snapshot_id}`、`admission_path(directory, pid, request_id)`、`register_admission(directory, admission) -> PathBuf`、`Server::new(c)`/`poll(topics, now)`/`invalidate_topic(topic, owner)`/`has_peers(topic)`；`TopicOwner = TopicPublisher<32>`。

## 3. 在 workspace 中的位置

- 上游依赖：`tickwire-market-source`、`tickwire-market-publisher`、`tickwire-binance`、`tickwire-binance-public`、`tickwire-binance-quota`、`tickwire-quota`、`tickwire-clock`、`tickwire-clock-sys`、`tickwire-transport`、`tickwire-venue`、`tickwire-book`、`tickwire-ipc`、`tickwire-shm`、`tickwire-join`、`tickwire-snapshot`、`tickwire-control`、`tickwire-control-sys`、`tickwire-control-store`、`tickwire-journal`；`serde`/`serde_json`/`libc`；dev `tickwire-binance-clock`、`rcgen`、`base64`、`sha1`。
- 下游消费者：本 crate 主要作为独立部署进程；Cargo.toml 声明依赖方为 `tickwire-lab`、`tickwire-strategy-host`（装配/实验用途），无生产库内调用。

## 4. 对应规格

- **无专门实现文档**（测绘结论：本进程没有独立 implementation 规格）。相邻链路参考：发布链见 [../../specs/implementation/binance-market-events.md](../../specs/implementation/binance-market-events.md)，正常退休 DETACH/QUIESCE 见 [../../specs/implementation/strategy-graceful-retirement.md](../../specs/implementation/strategy-graceful-retirement.md)。

## 5. 构建与测试

```sh
cargo build -p tickwire-md
cargo test -p tickwire-md
cargo clippy -p tickwire-md --all-targets -- -D warnings
cargo fmt --check -p tickwire-md
```

- `tests/market_events.rs`（1 项，真实 memfd 发布，非 Linux `ignore`）、`tests/sbe_publication.rs`（1 项，SBE 发布，非 Linux `ignore`）；两者复用 `tickwire-binance-public` 的网络测试装置。
- 真实 memfd/SEQPACKET 行为需 `./scripts/check-linux.sh` Docker 内运行；无 bench。

## 6. 关键不变量与注意事项

- **owner 边界**：`feeds`/`topics` 各 ≤32；`maximum_idle_ns` 非零且 ≤100_000_000（100ms），否则启动拒绝（engine.rs:226）。
- **资源身份核对**：每个 topic 资源头的 `producer_instance/producer_generation/host_boot_id/namespace_id` 必须等于配置 identity，`topic_id` 全局唯一，否则拒绝启动。
- **准入文件由可信 launcher 持有实际进程身份后写入**：`Admission` 授权一个合格 principal 与精确 topic/subscription，不是任意 HELLO；准入目录必须 0700（`register_admission` 检查 `mode & 0o077 == 0`），pid/supervisor_pid 非零、snapshot_id 非零、supervisor.role == 1；重复登记要求两份 JSON 逐字节相等。
- **HELLO 匹配**：opcode 1、flags 1、control_epoch 0、request_id 等于 pending admission 的 client.request_id，sender/target instance、实际 pid/uid/gid、对端身份全部匹配才建立会话；逻辑会话只提交已批准的精确请求到 JoinOwner 的幂等 reducer，不认证未知进程。
- 每标的独立有界网络 owner；配额走 `tickwire-quota` 文件配额存储；时钟用 `SystemClock` 绑定校准域。
- 本进程不做策略决策、不释放风险占用；业务入账归其他模块。

## 7. 改动检查清单

1. 改配置 schema/准入/引擎循环时，先确认相邻规格（binance-market-events、strategy-graceful-retirement）未被违反；本 crate 无独立规格可对照。
2. 真实链路测试走 `./scripts/check-linux.sh`；macOS 仅编译。
3. clippy `-D warnings`、`cargo fmt --check` 通过；新增 unsafe 仅限 server 接缝并写 SAFETY 注释。
4. 子命令行为变化需同步 `main.rs` usage 字符串与配置文件示例。
