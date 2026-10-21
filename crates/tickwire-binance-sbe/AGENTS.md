# AGENTS.md — tickwire-binance-sbe

## 职责
固定 schema 的 Binance Spot SBE（Simple Binary Encoding）行情帧解码器：不做动态反射，所有字段偏移由编译期常量给出，负责把官方 `stream_1_0.xml` 编码的 trade / bestBidAsk / depth20 partial / depth diff 四种模板解码为借用 `Frame`，并可按受信目录（instrument catalog）的 price tick / quantity step 归一化为整数 ticks/lots。它是 `tickwire-binance::feed::decode_sbe_diff` 的底层帧解码层，只在 Spot SBE 深度/事件链路被调用。

## 公共 API 入口
- `decode_frame(bytes, limits: Limits) -> Result<Frame<'_>>`：整帧（含消息头、组长度、所有行、尾部 symbol）全部校验通过后才返回帧；`Limits { max_frame_bytes, max_depth_levels, max_trades }`。
- `Frame<'_>`：`kind()`、`symbol()`、`event_time_ns()`、`transaction_time_ns()`、`price_exponent()`、`quantity_exponent()`、`first_update_id()`、`last_update_id()`、`bids()`、`asks()`、`trades()`、`quote()`。
- `Kind::{Trades, BestBidAsk, PartialDepth, DepthDiff}`；`LevelView { price_mantissa, quantity_mantissa }`；`Levels<'a>`（`len()`/`iter()`）；`Trade { id, price_mantissa, quantity_mantissa, is_buyer_maker }` + `is_best_match()`；`Trades<'a>`；`Quote { bid, ask }`。
- 归一化：`ExpectedInstrument<'a> { identity: Identity, symbol, price_tick: DecimalUnit, quantity_step: DecimalUnit }`、`Identity { instrument_id, instrument_version, catalog_version, catalog_hash, source_id, generation }`、`normalize_depth::<N>(expected, frame, received_mono_ns) -> Result<DepthUpdate<N>>`；`DepthKind::{Diff{first,last}, Partial{last}}`、`Coverage::{Unknown, Through(i64)}`、`DepthUpdate<N>`（`identity()`/`kind()`/`bids()`/`asks()`/`coverage()`）。
- 十进制：`DecimalUnit::new(mantissa, exponent)`（仅正）、`decimal_to_units(mantissa, exponent, unit) -> Result<i64>`，无浮点、无四舍五入中间值。
- `pub mod schema`：编译期常量 `SOURCE_SHA256`、`SCHEMA_ID = 1`、`VERSION = 0`、`MESSAGE_HEADER_BYTES = 8` 及四模板块/字段偏移。
- `Error::{Bounds, Capacity, Schema, Version, Template, BlockLength, Value, Utf8, Identity, NotIntegral, Overflow, Kind}`。

无 binary、无 example、无外部依赖。

## 在 workspace 中的位置
- 上游依赖：无（`[dependencies]` 为空，仅 `#![forbid(unsafe_code)]`）。
- 下游：仅 `tickwire-binance`（`src/feed/sbe.rs` 的 `decode_sbe_diff` 使用本 crate）。已在全部 `Cargo.toml` 中核对，无其他消费方。

## 对应规格
- ../../specs/implementation/binance-sbe.md

## 构建与测试
```sh
cargo test -p tickwire-binance-sbe
cargo build -p tickwire-binance-sbe
cargo clippy -p tickwire-binance-sbe --all-targets -- -D warnings
cargo fmt -p tickwire-binance-sbe
```
测试 `tests/codec.rs` 用 `tests/fixtures/` 下的 `trades.hex` / `quote.hex` / `partial.hex` / `diff.hex` 字面量帧覆盖：四模板解码、精确 mantissa/exponent 换算、大数量级取消不溢出、字段与组行保留、所有截断/尾部字节拒绝、schema 版本/模板/根块长精确校验、组长度与限额先于行检查、枚举/null 指数/负时间/溢出拒绝、归一化 diff 保留删除序列与受信身份、partial 不声称全覆盖、错误身份/容量网格/kind 拒绝、partial 排序/正数量/不跨盘校验、有理数网格换算、短输入不 panic。

重新生成 `src/schema.rs`（仅在官方 XML 变更时，脚本会先校验 `schema/source.json` 锁定的 SHA-256，XML 变动即失败）：
```sh
python3 tools/generate.py
```
`tools/fixtures.py` 用于刷新 `tests/fixtures/*.hex`。

## 关键不变量与注意事项
- **schema 固定**：schema id=1、version=0、littleEndian；官方 `schema/stream_1_0.xml` 的 SHA-256 锁定在 `schema/source.json`（`SOURCE_SHA256` 常量同源）。未知模板、错误版本、错误块长度一律拒绝（出处：`binance-sbe.md`、`decode.rs`、`tests/codec.rs::schema_version_template_and_root_size_are_exact`）。
- **全帧校验后才返回**：`Frame` 只在整个帧、每个组行、尾部 symbol 都验证通过后产出；截断、多余尾部字节、重复/异常组长度全部 `Bounds`/`BlockLength`/`Schema` 拒绝。
- **深度容量上限**：`MAX_DEPTH_LEVELS = 5000`、`MAX_TRADES = 65_536`、`MAX_FRAME_BYTES = 16 MiB`；`Limits` 自定义值不得越界（`decode.rs`）。
- **整数换算精确性**：`decimal_to_units` 先 GCD 约去公因数再处理 10 的幂，禁止浮点；非整除结果返回 `NotIntegral`，溢出返回 `Overflow`（出处：`decimal.rs`）。
- **身份绑定**：`normalize_depth` 要求 instrument/catalog/source/generation 非零且 `frame.symbol()` 与 `ExpectedInstrument.symbol` 完全相等，否则 `Identity`。
- **partial（top-20）不声称完整覆盖**：`coverage()` 对 diff 返回 `None`，对 partial 返回 `Through(last price)`；排序要求 bid 递减、ask 递增且不交叉（`normalize.rs`）。
- 本 crate 不含网络、时钟或授权逻辑；instrument 的 tick/step 由调用方受信目录提供，本层不验证交易所原值合理性。

## 改动检查清单
```sh
cargo test -p tickwire-binance-sbe
cargo clippy -p tickwire-binance-sbe --all-targets -- -D warnings
cargo fmt -p tickwire-binance-sbe
```
改动模板偏移或解码路径后，重跑 `tests/codec.rs` 全部用例；若更新官方 XML，先改 `schema/source.json` 锁值再运行 `tools/generate.py`，并同步刷新 fixtures。
