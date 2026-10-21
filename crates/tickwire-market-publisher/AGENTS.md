# tickwire-market-publisher

## 1. 职责

完整深度发布、精确 JOIN 基线与显式失败 bank 交接的 topic 发布器：把 `tickwire-binance-public` 交付的深度/事件归一化为 `tickwire-ipc` 固定记录，经 `tickwire-shm` 发布到 ring，用 `tickwire-snapshot` 构造精确 JOIN 基线，并在故障时通过 `FaultSink` 同步失效下游、准备失败 bank 交接。它是数据面发布侧的"topic  cut"装配层，单线程独占，JOIN 期间由 owner 独占借用。`#![forbid(unsafe_code)]`。

- `src/lib.rs`：`TopicPublisher<M>`、`TopicState`、`PublicationSink`、`InstrumentConfig`、`Limits`、`Fault`/`FaultSink`。
- `src/events.rs`：`EventSelection`、`event_record`、事件类别配置与通知适配。

## 2. 公共 API 入口

- `TopicPublisher<const M: usize>`：`new(...)`、`split() -> (&mut JoinOwner<M>, &TopicState)`、`state()`、`needs_bank()`、`require_rebuild()`、`sink(instrument, now_fn, faults) -> PublicationSink`、`on_notice(instrument, notice, now, faults)`、`on_notice_ref(...)`、`replace_bank(replacement, now) -> Result<JoinOwner<M>>`。
- `TopicState`：`sequence()`、`ready_for_snapshot()`、`cached_live()`、`publications()`、`live()`、`snapshot(...)`。
- 配置/故障：`InstrumentConfig{instrument: InstrumentBinding, key: BookKey, market: Market, time_unit}`、`Limits{levels_per_side, cache_bytes, snapshot_bytes, max_delta_bytes}`、`Fault{topic, feed_generation, instrument, published}`、`FaultSink::invalidate(fault)`。
- `PublicationSink`：`take_error()`；`events::{EventSelection, event_record}`。

## 3. 在 workspace 中的位置

- 上游依赖：`tickwire-binance`、`tickwire-binance-public`、`tickwire-book`、`tickwire-ipc`、`tickwire-join`、`tickwire-shm`、`tickwire-snapshot`、`tickwire-venue`、`sha2`。
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

- 本 crate 无独立 `tests/` 目录；发布行为由 `tickwire-md` 的集成测试（`tests/market_events.rs`、`tests/sbe_publication.rs`，真实 memfd，Linux-only）与上层应用链路覆盖。
- 无 bench。

## 6. 关键不变量与注意事项

- **精确换算**：价格/数量按可信 `InstrumentBinding` 的 tick/step 换算，非整倍数与整数溢出拒绝；源 symbol、product、catalog、source/connection 身份全部保留，不做大小写/Unicode 归一化。
- **事件类别**：每标的保存 Quote、Trade、MarkFunding、FeedState 四类；旧/新编码同类不能重复；独立 BBA 配置可替代深度派生 Quote，但不替换深度 levels 或 coverage；聚合 FeedState 要求配置类别可用，深度连续性仍由真实 DepthSync 确定。
- **故障失效同步**：`fault()` 置 `needs_bank = true`、state dirty，并**同步**经 `FaultSink::invalidate` 关闭执行门禁（与 ring 容量无关）；`Fault` 携带 topic/feed_generation/instrument/published 供下游定位。
- **replace_bank 链核对**：仅在 `needs_bank` 时允许；核对 channel/topic/catalog_version/producer_instance/producer_generation 不变、新 feed_generation 严格更大、`predecessor_ring_epoch == old.ring_epoch`、`cutover_seq == owner.published()`、`first_seq == published+1`、membership_version、feature 位 0x42、catalog_hash 与仪器集合完全一致；通过后冻结旧 owner、把存量状态重发到新 bank。
- 单 owner 固定产品/标的/类别；Connect/Upgrade/Subscribe/Pong 使用现有 PublicPermit，零接受重试重新申请；ExplicitSubscribe 以精确 null ACK 确认。
- JOIN 仍从完整快照 S 接收 S+1，SBE depth20 不代替完整快照。

## 7. 改动检查清单

1. 改事件类别/发布/故障交接时对照 binance-market-events.md，并跑 `tickwire-md` 的两个 Linux 集成测试（`./scripts/check-linux.sh`）。
2. `replace_bank` 链字段改动必须同步校验条件与 `tickwire-join`/`tickwire-shm` 的代次语义。
3. clippy `-D warnings`、`cargo fmt --check` 通过；本 crate 无独立 TLA 模型，并发正确性归 join/shm 模型。
