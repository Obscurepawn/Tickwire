# AGENTS.md — tickwire-book

## 职责
全 workspace 唯一 `#![no_std]` crate，实现单所有者（single-owner）有界订单簿 reducer：接收标准化的快照/增量 `Level` 数组，维护 bid/ask 双侧定容数组，并按独立的 venue 序列策略判定 Apply / IgnoreStale / Gap。它不拥有 transport、解码器、时钟或分配器，只提供状态机与只读 `BookView`，是 Binance 深度链路（`tickwire-binance` 的 `DepthSync`、`tickwire-binance-public` 的深度 owner）以及其他行情消费方的盘口热路径核心。

## 公共 API 入口
全部导出在 `src/lib.rs`（单文件实现）：

- `Book<P, const LEVELS: usize, const MAX_UPDATES: usize>`：盘口主体；`Book::new(key, generation)`、`install_snapshot(Snapshot)`、`apply(DepthEvent) -> Result<ApplyOutcome>`、`begin_rebuild(new_generation)`、`invalidate(generation, InvalidReason)`、`view(now_ns, maximum_age_ns) -> Result<BookView>`、`state()`、`generation()`、`last_update_id()`、`diagnostic_levels(side)`。
- `BookView<'a>`：只读视图，`known_bids()` / `known_asks()` / `coverage(side)` / `quantity(side, price) -> QuantityKnowledge` / `best_bid()` / `best_ask()` / `age_ns()`。
- 输入类型：`BookKey { venue, instrument, source }`、`PriceTicks(pub i64)`、`QtyLots(pub i64)`、`Level { price, quantity }`、`Side::{Bid, Ask}`、`Snapshot<'a>`、`DepthEvent<'a>`。
- 序列策略：`SequencePolicy` trait（`classify(SequenceInput) -> Result<SequenceDecision, SequenceError>`），内置 `BinanceSpot`、`BinanceUsdM` 两个 sealed 策略实现；`SequenceInput`、`SequenceDecision::{Apply, IgnoreStale}`、`SequenceError::{InvalidRange, MissingPrevious, Gap}`。
- 状态/错误：`BookState::{AwaitingSnapshot, Bridging, Live, Invalid(InvalidReason)}`、`BookError`、`ViewError::{NotLive, Stale, ClockRegressed}`、`ApplyOutcome::{Applied, IgnoredStale}`。
- 覆盖语义：`QuantityKnowledge::{Known, Unknown}`、`BestLevel::{Known, Empty, Unknown}`、`SideCoverage::{Complete, Through(PriceTicks), Unknown}`。

无 binary、无 example、无外部依赖。

## 在 workspace 中的位置
- 上游依赖：无（`[dependencies]` 为空；`#![no_std]`，仅用 `core`，测试时才 `extern crate std`）。
- 下游（已在各 crate 的 `Cargo.toml` 中核对）：`tickwire-binance`、`tickwire-binance-public`、`tickwire-app`、`tickwire-market-publisher`、`tickwire-md`、`tickwire-mock`、`tickwire-runtime`。

## 对应规格
- ../../specs/implementation/book-core.md
- ../../specs/implementation/book-state-model.md

## 构建与测试
```sh
cargo test -p tickwire-book
cargo build -p tickwire-book
cargo clippy -p tickwire-book --all-targets -- -D warnings
cargo fmt -p tickwire-book
```
基准（`benches/` 下三个文件均为 `harness = false` 的自定义 `main`，使用 `std::time::Instant` 自计时并打印 CSV 的 p50/p990/p999/MAX，未使用 criterion）：
```sh
cargo bench -p tickwire-book --bench depth_apply
cargo bench -p tickwire-book --bench batch_apply
cargo bench -p tickwire-book --bench sparse_apply
```
`benches/compare.py`、`benches/summarize.py` 是结果汇总辅助脚本，不是 bench target。测试在 `tests/`：`book.rs`（序列桥接矩阵/原子性/容量）、`batch.rs`、`small_batch.rs`、`differential.rs`（与独立 BTreeMap oracle 重放比对）、`no_alloc.rs`（全局分配计数，验证生产路径零分配）。

## 关键不变量与注意事项
- **`#![no_std]` 且生产路径零分配**：状态全部放在定容数组内，不允许引入 `alloc`/`std` 依赖；`tests/no_alloc.rs` 用全局 allocator 计数守护。新增字段或中间结构只能用栈上定容类型（出处：`src/lib.rs` 顶部 `#![no_std]` 与 `tests/no_alloc.rs`）。
- **Spot 序列规则（`BinanceSpot::classify`）**：`last_update_id <= local_id` 判为 IgnoreStale；桥接期 `first_update_id > local_id + 1` 报 Gap；其余 Apply（出处：`src/lib.rs` `BinanceSpot`）。
- **USDM 序列规则（`BinanceUsdM::classify`）**：要求 `previous_update_id` 存在；桥接期 `first_update_id > local_id` 报 Gap，Live 后 `previous != local_id` 报 Gap；`last < local` 或 Live 时 `last == local` 为 IgnoreStale（出处：`src/lib.rs` `BinanceUsdM`）。
- **原子性**：容量/畸形批次校验在修改任一侧之前完成，失败不得部分改动 bid 或 ask（测试：`book.rs` 的 `capacity_checks_final_batch_before_any_side_changes`、`malformed_batches_do_not_partially_modify_either_side`）。
- **Unknown 语义**：截断快照只证明边界以内的价格；`QuantityKnowledge::Unknown` / `BestLevel::Unknown` / `SideCoverage::Unknown` 表示"不知道"，不能当作 0 或空（出处：`SideCoverage::contains` 与 `view()`）。
- **世代（generation）只增**：`begin_rebuild` 必须提供新世代；旧快照不能重新激活当前盘口；错误世代/错误 `BookKey` 不得污染 Live 盘口（测试：`wrong_key_and_generation_cannot_poison_live_book`、`rebuild_requires_new_generation`）。
- **时钟回退拒绝**：`apply`/`view` 检测 backward clock，报 `ClockRegressed`（出处：`BookError::ClockRegressed`、`ViewError::ClockRegressed`）。
- 源码 `//!` 注释的接口契约指向 `specs/implementation/book-core.md`；不要再恢复对已删除 `docs/` 目录的引用。

## 改动检查清单
```sh
cargo test -p tickwire-book
cargo clippy -p tickwire-book --all-targets -- -D warnings
cargo fmt -p tickwire-book
```
改序列策略或状态迁移后，必须同时跑 `tests/book.rs`（桥接矩阵）与 `tests/differential.rs`（oracle 重放）；`book-state-model.md` 的 TLA/有限状态模型覆盖八组配置，改动状态机后需重新核对该模型是否仍 PASS。
