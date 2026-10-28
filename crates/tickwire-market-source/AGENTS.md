# tickwire-market-source

## 1. 职责

与运行时/策略代码**无关的认证归一化市场流**抽象：协议身份构造、本地市场副本（`MarketReplica`/`MarketView`）、正常退休（DETACH/QUIESCE）流程与共享内存源 `ShmSource`。它是策略侧/Host 侧订阅 MD 发布的统一接入层，向上只暴露 `MarketSource`/`MarketSink` 抽象，不依赖策略或 runtime 实现。`#![deny(unsafe_code)]`，仅 `shm` 模块经 `#[allow(unsafe_code)]` discharge 既有 SDK 接缝。

- `src/protocol.rs`：认证身份、offer/accept、cursor 头、激活 ACK/pending/status 控制字节构造。
- `src/replica.rs`（含 `checkpoint.rs`）：`MarketReplica`、`MarketView`、`FeedMark`、checkpoint/restore、连续序号应用与失效。
- `src/retirement.rs`：subscriber 请求的 DETACH（opcode 21）与 publisher QUIESCE（20）的构造/校验。
- `src/shm.rs`：`ConnectConfig`、`Client`（完成态 JOIN）、`ShmSource`（`MarketSource` 实现）。

## 2. 公共 API 入口

- 抽象：`MarketSource`（`domain()`/`poll(now, maximum_age_ns, budget, sink)`/`is_live(now, maximum_age_ns)`/`stop(sink)`）、`MarketSink`（`observed(observation) -> bool`/`invalidated(invalidation) -> bool`）、`PollReport{records, runnable, next_deadline_ns}`。
- 观测：`FeedKey{topic_id, instrument_id}`、`FeedObservation{key, instrument_version, feed_generation, ring_epoch, sequence, published_at, received_at, live}`、`Invalidation{key, feed_generation, ring_epoch}`。
- `protocol`：`IdentityConfig`、`Endpoint`、`Instrument`、`JoinBootstrap`、`wire_bytes`、`descriptor`、`offer_id`、`resource_offer`、`accept_offer`、`cursor_header`、`ActivationStatus`、`activation_ack`、`activation_pending`、`activation_status`、`activated`。
- `replica`：`MarketReplica`（`load`/`checkpoint`/`restore_checkpoint`/`invalidate`/`sequence`/`view`/`apply`/`apply_dense`/`view_dense`）、副本本地 `DenseId`、`MarketView`（`last_trade`/`best_quote`/`mark_funding`/`state_age_ns`/`bids`/`asks`/`best_bid`/`best_ask`/`quantity`/`continuous`/`live`/`trigger`/`trigger_at`）、`FeedMark`。
- `retirement`：`body`、`request`、`verify_request`、`applied`、`verify_applied`。
- `shm`：`ConnectConfig`、`Client{consumer, replica, connection, session, baseline, baseline_bytes, baseline_now, catchup, bank, join, domain}`、`ShmSource::new(client)`/`client()`/`into_client()`。

## 3. 在 workspace 中的位置

- 上游依赖：`tickwire-instrument-index`、`tickwire-ipc`、`tickwire-shm`、`tickwire-join`、`tickwire-snapshot`、`tickwire-clock`、`tickwire-control`、`tickwire-control-sys`、`serde`/`serde_json`、`sha2`、`libc`。仅测试使用 `allocation-counter` 测量当前线程分配。
- 下游消费者（已核对 Cargo.toml）：`tickwire-md`、`tickwire-runtime`、`tickwire-strategy-host`、`tickwire-strategy-sdk`、`tickwire-lab`、`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/strategy-graceful-retirement.md](../../specs/implementation/strategy-graceful-retirement.md)（共享行情订阅的正常退休：DETACH/QUIESCE/STATUS 绑定与 EOF 非证据）。
- [标的索引](../../specs/implementation/instrument-index.md)与[归一化市场事件](../../specs/implementation/binance-market-events.md)（稳定外部 ID、本地稠密存储、深度工作区与检查点兼容）。

## 5. 构建与测试

```sh
cargo build -p tickwire-market-source
cargo test -p tickwire-market-source
cargo clippy -p tickwire-market-source --all-targets -- -D warnings
cargo fmt --check -p tickwire-market-source
```

- `tests/replica_dense.rs` 覆盖稀疏高位 ID、未知 ID 失效、旧检查点字节兼容、满容量删除插入、双侧失败原子性、独立有序映射差分、覆盖范围与零分配更新/查询；另由 `tickwire-md`、`tickwire-strategy-sdk/host` 集成测试与 Linux 链路覆盖。
- 无 bench。

## 6. 关键不变量与注意事项

- **每个实现自持注册 cursor 与认证控制租约**；`stop` 必须先失效下游 feed 再丢弃 source。
- **副本连续性**：`MarketReplica` 按全 topic 连续序号应用；身份/代次/序号或容量错误使视图失效（`invalidate`），不修补缺口；JOIN 从完整快照 S 接收 S+1。
- **本地稠密索引**：固定目录按外部 ID 排序后构建 `FixedIndex` 与连续 `Vec<Book>`；每帧映射一次，`DenseId` 仅用于所属副本目录，ABI 与 `TWMR0001` 仍保存稳定外部 ID。状态类别固定为 1/2/4/8，序列化次序不变。
- **连续价位与原子更新**：买降卖升，数量查询二分；两侧先合并到 topic 共用工作区并完成容量/非交叉校验才提交。工作区最多 320000 字节，已有盘口容量内更新及查询不分配。任何失败保持原盘口，覆盖外缺失仍为未知。
- **退休（DETACH/QUIESCE）**：subscriber DETACH（opcode 21, flags 1）携带自己完整 Join 身份，**不带虚构水位**；publisher 先取得真实 mapped QUIESCED 证明，再由 `JoinOwner::quiesced` 核验，成功后 STATUS（49, Response|Applied）绑定 DETACH 请求 ID、控制代次、membership、ring 与完整 QUIESCED body 的 SHA-256；worker 逐字段核对后才向 Host 发 report QUIESCE。
- **EOF 永远不是证据**：单纯连接关闭、超时、伪造响应、控制超时都走保守故障处理，不构成 QUIESCED。
- `Client::connect` 是 unsafe 接缝：调用者必须先建立可信认证会话与完整身份，再进入接入流程。
- `ConnectConfig` 携带精确批准的 MD bank 头、catalog_hash、仪器集合、cursor_id、subscription_generation、request_id；仪器数 1..=4096。

## 7. 改动检查清单

1. 改退休流程时对照 strategy-graceful-retirement.md 与 `tickwire-join` 的 quiesced 精确匹配条件。
2. 副本应用/失效逻辑改动需在 strategy-sdk/host 侧补连续序号与失效用例。
3. `Client::connect` 周边新增访问保持 SAFETY 注释；clippy `-D warnings`、`cargo fmt --check`。
4. 本层不做策略决策、不释放风险占用；业务入账归其他模块。
