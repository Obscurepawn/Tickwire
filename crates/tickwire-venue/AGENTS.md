# tickwire-venue

## 1. 职责

不可变、有界的交易所命令与事实：确定性 CBOR 身份编码、规范命令/事件字节与 SHA-256 内容摘要。它是与具体交易所无关的**规范类型边界**——把可编辑的 `Command` 草稿封成只读的 `CanonicalCommand`，把交易所事实封成 `CanonicalEvent`，供执行器与闭包存储绑定；本 crate 不执行网络发送、不做资金记账、不授予交易权限。`#![forbid(unsafe_code)]`，唯一外部依赖 `sha2`。

- `src/types.rs`：命令/事件领域类型（请求身份、策略上下文、标的绑定、订单规格、触发/移动止损/列表/算法单等）。
- `src/account.rs`：账户输入与观察事实（收入、钱包、持仓、账户身份）。
- `src/cbor.rs`/`src/codec.rs`：确定性 CBOR 读写与版本编码。
- `src/validate.rs`：构造与解码共用的结构/经济校验。

## 2. 公共 API 入口

- `CanonicalCommand`（`new(command)`/`decode(bytes)`/`command()`/`bytes()`/`digest()`）、`CanonicalEvent`（`new(event)`/`decode`/`event()`/`bytes()`/`digest()`）。
- 常量：`Hash = [u8; 32]`、`SCHEMA_VERSION = 1`、`MAX_CANONICAL_BYTES = 16_384`。
- `types`：`Command{context, body}`、`CommandBody`（新单/Cancel/Query/Amend/Modify/CancelReplace/List/Algo 等）、`VenueEvent`/`VenueEventBody`、`RequestIdentity`、`StrategyContext`、`InstrumentBinding`、`OrderSpec`、`SpotOrder`、`ModifyOrder`、`Trigger`/`Trailing`、`OrderType`、`TimeInForce`、`Side`、`Product`、`Environment`、`Decimal`、`BoundedText/BoundedSymbol/BoundedMessage/BoundedList` 等。
- `account`：`CanonicalAccountInput`、`AccountInput`/`AccountInputBody`、`AccountIdentity`、`AccountObservation`、`IncomeRecord`、`WalletObservation`、`PositionObservation`、`ExternalTrade`、`MAX_ACCOUNT_INPUT_BYTES = 60KiB`、`MAX_ACCOUNT_OBSERVATION_ASSETS = 64`、`MAX_ACCOUNT_OBSERVATION_POSITIONS = 128`。

## 3. 在 workspace 中的位置

- 上游依赖：无 `tickwire-*`（仅 `sha2`）。
- 下游消费者（已核对 Cargo.toml）：`tickwire-gateway`、`tickwire-market-publisher`、`tickwire-md`、`tickwire-binance`、`tickwire-binance-public`、`tickwire-binance-private`、`tickwire-binance-execution`、`tickwire-binance-account`、`tickwire-binance-quota`、`tickwire-account`、`tickwire-account-codec`、`tickwire-account-gateway`、`tickwire-account-journal`、`tickwire-handover`、`tickwire-replica`、`tickwire-runtime`、`tickwire-runtime-policy`、`tickwire-guardian`、`tickwire-strategy-example`、`tickwire-strategy-host`、`tickwire-strategy-sdk`、`tickwire-lab`、`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/venue.md](../../specs/implementation/venue.md)（内容身份、编码合同、命令/事件/费用规则）。
- [../../specs/implementation/usdm-account-facts.md](../../specs/implementation/usdm-account-facts.md)（USD-M 账户事实编码）。

## 5. 构建与测试

```sh
cargo build -p tickwire-venue
cargo test -p tickwire-venue
cargo clippy -p tickwire-venue --all-targets -- -D warnings
cargo fmt --check -p tickwire-venue
```

测试（纯字节，跨平台，无 Linux-only，无 bench）：`commands.rs` 12、`events.rs` 7、`spot_extensions.rs` 7、`strict.rs` 5、`contract.rs` 5、`account_input.rs` 3、`amendment.rs` 2、`structures.rs` 2、`vectors.rs` 4。

## 6. 关键不变量与注意事项

- **不可变与只读**：`Command` 是可编辑草稿；`CanonicalCommand` 私有持有草稿副本、编码与摘要，只暴露只读访问；`Command`/`CanonicalCommand` 都不实现 `Copy`。构造器和解码器执行相同语义检查，摘要始终由内部完整编码派生。
- **确定性 CBOR**：顶层固定数组 `["tickwire.venue-command", 1, CommandContext, CommandBody]`（事件为 `["tickwire.venue-event", version, identity, body]`）；`decode` 重新编码后要求字节逐字相等，否则 `NonCanonical`；事件版本：普通 1、Structure 2、Amendment/AmendmentRejected 3，版本不符 `Schema`。
- **身份字段**：请求身份含风险域、venue、环境、账户、产品、意图 ID、原执行代次、策略代次、配置版本、deadline、recovery_object；策略上下文保留 logical_strategy_id/instance_id/strategy_intent_seq/authorization_epoch/risk_policy_version/order_config_version/created_mono_ns/economic_intent_id/hedge_group_id/leg_id/recovery_manifest_hash；WAL 序号由执行器独立分配。
- **symbol 按原 UTF-8 字节保留**，禁止空白与控制字符，不做大小写或 Unicode 归一化。
- **列表最多三腿**：OCO `[above, below]`、OTO `[working, pending]`、OTOCO `[working, pendingAbove, pendingBelow]`，顺序固定。
- **费用**：最多八种资产，每项带资产 ID/版本/原始符号/最小单位指数/有符号现金流；同事件同资产重复行拒绝，由适配器选独立费用事件或经核对净额；费用不强制转成单一结算资产；未知量用 `None`，绝不从价格乘数量补造；已实现盈亏用独立 `AssetDelta`，不归入手续费。
- 连接代次变化改变事件来源字节；经济去重按原始交易所事实身份，重连保留原归属与入账键。
- `MAX_CANONICAL_BYTES = 16384` 硬上限，超限 `Bounds`。

## 7. 改动检查清单

1. 改命令/事件 schema 或 CBOR 编码时，先更新 venue.md 编码合同并在 `contract.rs`/`vectors.rs` 补 golden 向量。
2. `cargo test -p tickwire-venue` 全绿；clippy `-D warnings`、`cargo fmt --check`。
3. 新增命令变体必须同时覆盖构造 `new` 与 `decode` 往返（NonCanonical/Schema 拒绝）。
4. 本层不做授权与记账；权限/预算检查归 `Authorization`/风险模块。
