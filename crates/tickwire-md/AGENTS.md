# tickwire-md

## 1. 职责

独立 Binance 行情进程与认证全 topic 发布者：每标的一个有界网络 owner，完成准入、订阅、归一化、经 `tickwire-market-publisher` 发布到共享内存，并通过 `tickwire-market-source` 向认证订阅者交付。它是部署形态上的常驻二进制（`src/main.rs`），不是库；库层只提供配置装配、引擎循环与准入服务。`#![deny(unsafe_code)]`，仅 `server` 模块经 `#[allow(unsafe_code)]` discharge 既有接缝。

- `src/config.rs`：文件配置、资源头/客户端模板构造、准入文件读写。
- `src/engine.rs`：`run(c)` 单进程装配与显式驱动主循环。
- `src/expansion.rs`：launcher 扩展计划的有界解码、revision/配置摘要、完整旧成员与来源、目标 bank 模板、实际消费者旧/新身份和峰值预算核对；`Prepared` 字段私有，最终描述绑定真实 cut。该准备接口尚未挂入 `run` 的持续控制循环。
- `src/server.rs`：认证会话服务、准入登记、`Server::poll`。
- `src/main.rs`：四个子命令 `--config` / `--resource ... --output` / `--client ... --output` / `--admit ... --directory`。

## 2. 公共 API 入口

- `config`：`Asset`、`Instrument`（`build(source) -> InstrumentBinding`）、`FeedIndex`（`new(feeds)`/`expanded(feeds)`/`book_key(feed)`）、`Feed`（`build(domain, index)`）、`EventType`（`kind()`/`state()`）、`EventFeed`（`context(feed)`/`build(feed, domain, index)`）、`Topic`、`Config`、`ResourceSpec`（`header_with_exact_events()`/`header()`）、`ClientTemplate::build() -> ConnectConfig`、`read`/`write`/`write_bytes`/`active_resource_path`。
- `engine::run(c: Config) -> Result<()>`。
- `expansion::{Plan, Consumer, Prepared, Finalized, config_hash}`：`Plan::prepare` 接收当前配置/索引/revision、真实 bank 与 topic 下标、Server 实际参与者和退休 bank 字节；`Prepared::finalize` 生成同一最终 bank 字节及各消费者 Admission，不冻结、不授予权限。
- `server`：`Admission{client, pid, uid, gid, supervisor, supervisor_pid, snapshot_id}`、`admission_path(directory, pid, request_id)`、`register_admission(directory, admission) -> PathBuf`、`Server::new(c)`/`poll(topics, now)`/`invalidate_topic(topic, owner)`/`has_peers(topic)`；`TopicOwner = TopicPublisher<32>`。

## 3. 在 workspace 中的位置

- 上游依赖：`tickwire-market-source`、`tickwire-market-publisher`、`tickwire-binance`、`tickwire-binance-public`、`tickwire-binance-quota`、`tickwire-quota`、`tickwire-clock`、`tickwire-clock-sys`、`tickwire-transport`、`tickwire-venue`、`tickwire-book`、`tickwire-ipc`、`tickwire-shm`、`tickwire-join`、`tickwire-snapshot`、`tickwire-control`、`tickwire-control-sys`、`tickwire-control-store`、`tickwire-journal`；`serde`/`serde_json`/`libc`；dev `tickwire-binance-clock`、`rcgen`、`base64`、`sha1`、`allocation-counter`。
- 下游消费者：本 crate 主要作为独立部署进程；Cargo.toml 声明依赖方为 `tickwire-lab`、`tickwire-strategy-host`（装配/实验用途），无生产库内调用。

## 4. 对应规格

- 扩容子域见 [../../specs/implementation/market-expansion.md](../../specs/implementation/market-expansion.md)。其余进程装配沿相邻规格：发布链见 [../../specs/implementation/binance-market-events.md](../../specs/implementation/binance-market-events.md)，正常退休 DETACH/QUIESCE 见 [../../specs/implementation/strategy-graceful-retirement.md](../../specs/implementation/strategy-graceful-retirement.md)。

## 5. 构建与测试

```sh
cargo build -p tickwire-md
cargo test -p tickwire-md
cargo clippy -p tickwire-md --all-targets -- -D warnings
cargo fmt --check -p tickwire-md
```

- `tests/market_events.rs`（1 项，真实 memfd 发布，非 Linux `ignore`）、`tests/sbe_publication.rs`（1 项，SBE 发布，非 Linux `ignore`）；两者复用 `tickwire-binance-public` 的网络测试装置。
- `tests/instrument_slots.rs`：多标的跨 topic 槽位、未知/外部槽拒绝原子性、深度/事件 wire 稳定 ID，以及 bank 轮换后槽位复用；复用真实 JOIN 装置且不含 unsafe，非 Linux `ignore`。
- `tests/publisher_hot.rs` 与 `tests/common/publisher_{hot,inputs,scenarios}.rs`：通过真实 JOIN 保留 memfd 发布前缀，在 writer 空闲时安全读取 wire/arena；`allocation-counter` 只测 `on_notice_slot/on_event_slot`，覆盖首次类别、JSON/SBE 12/13/4096 档和长短交替、bank 交接、准确缓存预算、错误解码和 ring/arena 耗尽。完整深度路径在独立子进程的 2 MiB 线程栈执行，防止栈溢出终止整个测试进程；不新增 unsafe，不把 fixture 构建或文件读取算入逐消息分配。
- 真实 memfd/SEQPACKET 行为需 `./scripts/check-linux.sh` Docker 内运行；无 bench。

## 6. 关键不变量与注意事项

- **owner 边界**：`feeds`/`topics` 各 ≤32；`maximum_idle_ns` 非零且 ≤100_000_000（100ms），否则启动拒绝（engine.rs:226）。
- **资源身份核对**：每个 topic 资源头的 `producer_instance/producer_generation/host_boot_id/namespace_id` 必须等于配置 identity，`topic_id` 全局唯一，否则拒绝启动。
- **准入文件由可信 launcher 持有实际进程身份后写入**：`Admission` 授权一个合格 principal 与精确 topic/subscription，不是任意 HELLO；准入目录必须 0700（`register_admission` 检查 `mode & 0o077 == 0`），pid/supervisor_pid 非零、snapshot_id 非零、supervisor.role == 1；重复登记要求两份 JSON 逐字节相等。
- **HELLO 匹配**：opcode 1、flags 1、control_epoch 0、request_id 等于 pending admission 的 client.request_id，sender/target instance、实际 pid/uid/gid、对端身份全部匹配才建立会话；逻辑会话只提交已批准的精确请求到 JoinOwner 的幂等 reducer，不认证未知进程。
- 每标的独立有界网络 owner；配额走 `tickwire-quota` 文件配额存储；时钟用 `SystemClock` 绑定校准域。
- feed 装配时先构建进程级 `FeedIndex`，按标的首次出现顺序将完整非零 `u64` 映射到本地零起 `BookKey.instrument`。所有 depth owner 与各 topic 从同一索引绑定 key；topic 的发布行排序可能不同，不能互换两类槽。同一标的跨 feed 复用编号，未知 ID 拒绝；深度 source 仍要求可无损表示为 `u32`，事件 source 保持原 `u64`。
- `FeedIndex::expanded` 在冷路径构建只追加候选；完整旧集合必须保留，即使目标配置重排也不改变旧编号。候选失败不改活动索引，经济规格与授权由扩展计划另行校验。
- 每个 topic 预绑定 `InstrumentSlot`，行情通知直接定位发布行；稳定标的 ID 仍用于源身份核验和 wire，不把本地数组索引当全局 ID。新增上游 `tickwire-instrument-index` 仅参与冷路径装配，`engine/dense_tests.rs` 覆盖完整 ID、目录排序、源装配、重复与未知边界。
- 本进程不做策略决策、不释放风险占用；业务入账归其他模块。

### 扩容控制连接

- `Server::active_admissions(topic)` 返回真实 Active peer 的完整绑定；同 topic 未完成接入/退出和无法归属的 HELLO 阻止计划确认。`begin_expansion(topic,operation,expected,owner,now)` 复核精确参与者与当前 bank 后暂停 JOIN，为每个订阅生成 operation 绑定的唯一 QUIESCE。
- `server/expansion.rs` 保留最多 128 项操作历史，每项最多 128 个规范 Admission SHA-256 摘要，不复制全部历史配置。peer outbox 仅保存一个完整 QUIESCE，try_send 背压/EINTR 保留原字节；EOF、超时或其他发送错误标记操作失败且保持关闭。
- `expansion_quiesced(topic,operation,owner)` 以实际 `JoinOwner::all_detached` 锁存成功；`resume_expanded_topic(topic,operation,owner)` 还核对真实后继 bank 与集合版本，不能用 `has_peers` 代替退出证明。暂停中的无 gate 新请求拒绝不污染健康 topic。
- 首次和重复 resume 都要求后继 `!needs_bank && state.live()`；故障后重试返回错误并锁存关闭，历史后继记录不清除当前故障门禁。
- `server/expansion_tests.rs` 通过真实 Server/PendingJoin、共享 cursor、候选预热/提交和新 bank re-JOIN，验证精确审批、多消费者、整包背压、伪造证明、EOF/超时及高位 `u64` 新成员；规格见 [../../specs/implementation/market-expansion.md](../../specs/implementation/market-expansion.md)。

## 7. 改动检查清单

1. 改配置 schema/准入/引擎循环时，核对扩容规格 market-expansion 与相邻规格 binance-market-events、strategy-graceful-retirement。
2. 真实链路测试走 `./scripts/check-linux.sh`；macOS 仅编译。
3. clippy `-D warnings`、`cargo fmt --check` 通过；新增 unsafe 仅限 server 接缝并写 SAFETY 注释。
4. 子命令行为变化需同步 `main.rs` usage 字符串与配置文件示例。
