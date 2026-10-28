# tickwire-market-publisher

## 1. 职责

完整深度发布、精确 JOIN 基线与显式失败 bank 交接的 topic 发布器：把 `tickwire-binance-public` 交付的深度/事件归一化为 `tickwire-ipc` 固定记录，经 `tickwire-shm` 发布到 ring，用 `tickwire-snapshot` 构造精确 JOIN 基线，并在故障时通过 `FaultSink` 同步失效下游、准备失败 bank 交接。它是数据面发布侧的"topic  cut"装配层，单线程独占，JOIN 期间由 owner 独占借用。`#![forbid(unsafe_code)]`。

- `src/lib.rs`：`TopicPublisher<M>`、`TopicState`、`PublicationSink`、`InstrumentConfig`、`Limits`、`Fault`/`FaultSink`。
- `src/events.rs`：`EventSelection`、`event_record`、事件类别配置与通知适配。
- `src/expansion.rs`：冷路径严格超集候选、独立预热、真实静默后的原子目录切换；`src/expansion/tests.rs` 保存真实 memfd 与完整快照回归。

## 2. 公共 API 入口

- `TopicPublisher<const M: usize>`：`new(...)`、`split() -> (&mut JoinOwner<M>, &TopicState)`、`state()`、`needs_bank()`、`require_rebuild()`、`sink(instrument, now_fn, faults) -> PublicationSink`、`on_notice(instrument, notice, now, faults)`、`on_notice_ref(...)`、`replace_bank(replacement, now) -> Result<JoinOwner<M>>`。
- `TopicState`：`sequence()`、`ready_for_snapshot()`、`cached_live()`、`publications()`、`live()`、`snapshot(...)`。
- 配置/故障：`InstrumentConfig{instrument: InstrumentBinding, key: BookKey, market: Market, time_unit}`、`Limits{levels_per_side, cache_bytes, snapshot_bytes, max_delta_bytes}`、`Fault{topic, feed_generation, instrument, published}`、`FaultSink::invalidate(fault)`。
- `PublicationSink`：`take_error()`；`events::{EventSelection, event_record}`。
- `instrument_slot(id)` 在装配时绑定 `InstrumentSlot`；`on_notice_slot/on_event_slot` 直接访问连续行并复核稳定 ID，既有按 ID 的入口仍可使用。
- `reserved_cache_bytes()` 查询构造时核验的保留堆载荷：行/状态/深度容量、目录和context文本、标的索引以及共享工作区。它不含分配器元数据、内联体、调用栈、SHM/JoinOwner与临时JOIN快照；这些有各自预算。
- `prepare_expansion(registry, additions, limits, peak_cache_bytes) -> TopicExpansion`；候选支持 `configure_event`、`warm_notice`、`warm_event_notice`、`ready(now,max_age_ns)`、`reserved_cache_bytes()`。`commit_expansion(candidate,replacement,now,max_age_ns,faults)` 刷新旧行并切换到新的真实 bank，返回旧 JoinOwner。

## 3. 在 workspace 中的位置

- 上游依赖：`tickwire-binance`、`tickwire-binance-public`、`tickwire-book`、`tickwire-instrument-index`、`tickwire-ipc`、`tickwire-join`、`tickwire-shm`、`tickwire-snapshot`、`tickwire-venue`、`sha2`。
- 下游消费者（已核对 Cargo.toml）：`tickwire-md`、`tickwire-strategy-host`、`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/binance-market-events.md](../../specs/implementation/binance-market-events.md)（七种输入、精确换算、事件类别、发布时间与恢复；本发布链路由该规格覆盖）。

## 5. 构建与测试

```sh
cargo build -p tickwire-market-publisher
cargo test -p tickwire-market-publisher
cargo clippy -p tickwire-market-publisher --all-targets -- -D warnings
cargo fmt --check -p tickwire-market-publisher
```

- 本 crate 无独立 `tests/` 目录；发布行为由 `tickwire-md` 的集成测试（`tests/market_events.rs`、`tests/sbe_publication.rs`、`tests/instrument_slots.rs`，真实 memfd，Linux-only）与上层应用链路覆盖；槽位测试核对跨 topic 映射、拒绝原子性及 bank 轮换后的稳定 ID。
- 无 bench。

## 6. 关键不变量与注意事项

- **精确换算**：价格/数量按可信 `InstrumentBinding` 的 tick/step 换算，非整倍数与整数溢出拒绝；源 symbol、product、catalog、source/connection 身份全部保留，不做大小写/Unicode 归一化。
- **事件类别**：每标的保存 Quote、Trade、MarkFunding、FeedState 四类；旧/新编码同类不能重复；独立 BBA 配置可替代深度派生 Quote，但不替换深度 levels 或 coverage；聚合 FeedState 要求配置类别可用，深度连续性仍由真实 DepthSync 确定。
- **故障失效同步**：`fault()` 置 `needs_bank = true`、state dirty，并**同步**经 `FaultSink::invalidate` 关闭执行门禁（与 ring 容量无关）；`Fault` 携带 topic/feed_generation/instrument/published 供下游定位。
- **replace_bank 链核对**：仅在 `needs_bank` 时允许；核对 channel/topic/catalog_version/producer_instance/producer_generation 不变、新 feed_generation 严格更大、`predecessor_ring_epoch == old.ring_epoch`、`cutover_seq == owner.published()`、`first_seq == published+1`、membership_version、feature 位 0x42、catalog_hash 与仪器集合完全一致；通过后冻结旧 owner、把存量状态重发到新 bank。
- 单 owner 固定产品/标的/类别；Connect/Upgrade/Subscribe/Pong 使用现有 PublicPermit，零接受重试重新申请；ExplicitSubscribe 以精确 null ACK 确认。
- JOIN 仍从完整快照 S 接收 S+1，SBE depth20 不代替完整快照。
- 标的索引用 `tickwire-instrument-index::FixedIndex` 冷构建；槽位仅对本地目录有效，ABI/快照/Trigger 保留稳定 ID。bank 轮换不能重排标的集合；未知 ID 不会创建新行。
- 发布目录把完整 `u64 instrument_id` 绑定到独立的本地 `BookKey`，允许 `BookKey.instrument == 0`；装配时拒绝不同目录行重复完整 `(venue, instrument, source)` 键，不能按外部 ID 截断生成本地键。错误键视图在修改行之前拒绝并沿既有路径失效 bank；高位 ID、错键与缓存保持回归见 `tickwire-md/tests/instrument_slots.rs`。
- 每 topic 冷预留一个总4096档的`DepthWorkspace`和4096项IPC深度数组，共128KiB档位载荷；缓存盘口仍可每侧5000档。普通发布使用最多3项固定记录/引用数组，所有首次及后续状态容量均预留，事件source使用3个固定槽。健康发布入口至SHM无堆分配；网络接收层不在该结论范围内。测试见`tickwire-md/tests/publisher_hot.rs`，包含真实memfd、2MiB栈、inline/arena、容量边界和失败bank交接。
- `cache_bytes`先按容器及共享工作区的最低容量预检，构造后按实际capacity与文本载荷复核，超限拒绝。工作区不进入snapshot/WAL；长期持有的借用不能跨下一次解码。
- topic 扩展候选只能预热新增行，断线/Gap/错误使对应状态不可用；ready 不使用独立事件刷新深度年龄。commit 要求旧 bank 所有登记真实 Detached、当前所有旧/新必需状态新鲜、完整快照预算充足、新 bank 绑定当前 cut 且目录/代次递增；候选写失败不改旧发布器，冻结失败同步 fault。old+candidate retained heap 受显式峰值预算，SHM/退休 bank 另管。新目录会重排本地 topic 槽，调用方重新绑定 InstrumentSlot；完整 BookKey 与外部稳定 ID 不变。

## 7. 改动检查清单

1. 改事件类别/发布/故障交接时对照 binance-market-events.md，并跑 `tickwire-md` 的两个 Linux 集成测试（`./scripts/check-linux.sh`）。
2. `replace_bank` 链字段改动必须同步校验条件与 `tickwire-join`/`tickwire-shm` 的代次语义。
3. clippy `-D warnings`、`cargo fmt --check` 通过；本 crate 无独立 TLA 模型，并发正确性归 join/shm 模型。
