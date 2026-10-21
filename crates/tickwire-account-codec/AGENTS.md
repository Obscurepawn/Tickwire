# tickwire-account-codec

## 1. 职责（架构位置：账户风险域 —— 有界不可变恢复对象编码）

把账户基线与恢复记录编码为确定性、有界、不可变的 CBOR 对象，提供完整字节摘要与规范校验；本身不含持久化授权。基线 `["tickwire.account-baseline",1,Snapshot,Catalog]`（≤2 MiB），记录 `["tickwire.account-record",1,baseline_sha256,sequence,RecoveryRecord]`（≤60 KiB，为物理 WAL 64 KiB 批留 begin/data/seal 余量）。lib 根文档：`Bounded immutable account recovery objects.`

## 2. 公共 API 入口

- `CanonicalBaseline`：`new(snapshot, catalog)`、`decode(bytes)`、`snapshot()/catalog()/bytes()/digest()`；构造时先 `validate::baseline` 排序/去重，再用 `Account::from_snapshot` 校验单位/目录/风险参数。
- `CanonicalRecord`：`new(baseline, sequence, record)`、`decode(bytes)`、`baseline()/sequence()/record()/bytes()/digest()`；`decode` 要求重编码字节与输入逐字节相等（`NonCanonical`）。
- `InputSeal`：`new(baseline)`、`apply(&record)`（校验同基线、序号连续并累计哈希链）、`position()/digest()/object_bytes()`。
- `Hash=[u8;32]`；`Error::{Bounds,Truncated,NonCanonical,Field,Text,IntegerOverflow,TrailingData,Schema,Duplicate,Account,Venue}`。
- 上限常量：`MAX_BASELINE_BYTES=2MiB`、`MAX_RECORD_BYTES=60KiB`、`MAX_ADMISSION_ORDERS=3`、`MAX_OWNERS=1024`、`MAX_BRACKETS=128`。
- 私有模块：`cbor`（Writer/Reader）、`codec`（各类型编解码）、`validate`（基线/记录校验）。无 bin/example。

## 3. workspace 位置

上游（`Cargo.toml` 实际声明）：`tickwire-account`、`tickwire-risk`、`tickwire-venue`；第三方 `sha2`；dev：`tickwire-journal`、`tickwire-recovery`。

下游（grep 命中）：`tickwire-account-journal`、`tickwire-account-gateway`、`tickwire-handover`、`tickwire-strategy-sdk`、`tickwire-strategy-host`、`tickwire-binance-account`、`tickwire-binance-execution`、`tickwire-runtime`、`tickwire-replica`、`tickwire-app`、`tickwire-lab`。

## 4. 对应规格

- [../../specs/implementation/account-codec.md](../../specs/implementation/account-codec.md)（对象/编码、基线集合排序唯一、i128 最短大整数、禁止浮点/不定长/未识别标签）

## 5. 构建与测试

```sh
cargo build -p tickwire-account-codec
cargo test -p tickwire-account-codec
```

无 bin/example。`tests/`：`baseline.rs`（每个前缀截断 + 尾部数据拒绝）、`records.rs`（每记录全前缀截断、尾部字节、非规范、越界 Bounds）、`nonincreasing.rs`、`amendment.rs`、`account_inputs.rs`、`ingress.rs`、`input_seal.rs`、`file_replay.rs`（WAL 重开）、`vectors.rs`（独立参考基线字节/哈希与最小 i128 现金流）、`legacy_encoding.rs`（旧格式拒绝）。

> 独立基准 hex/sha256（spot/futures/minimum-cashflow/unsorted 基线及 `legacy-records.hex`）原存于已删除的 `docs/validation/`，已迁入本 crate `tests/data/`，`vectors.rs`/`legacy_encoding.rs` 改为 `include_str!("data/...")`；修改基准字节时必须同步更新对应 `.sha256`。

## 6. 关键不变量与注意事项

- **摘要 = 完整顶层编码的 SHA-256**：`baseline_sha256` 非零，记录 `sequence` 从 1 开始；编码器查非零，持久化协调器另查记录-基线-物理日志-前后序号对应。
- **规范往返**：`decode` 后重新 `new` 并比较 `bytes()`，不等即 `NonCanonical`；拒绝非最短整数、不定长项、未知标签、尾随数据。
- **有界**：基线 >2 MiB、记录 >60 KiB 直接 `Bounds`；基线集合（资产 `(wallet,asset)`、仓位 `(instrument,side)` 等）排序并拒绝重复键。
- **金额保真**：`[atoms,scale]` 保留原单位与尾零；交易所十进制 `[coefficient,exponent]` 保留规范形式；i128 最小值用标签 2/3 最短大整数，负幅按 `-1-value`。
- **完整命令/事件内嵌为字节串**，解码仍调用原模块完整规范检查。
- `InputSeal` 只累计摘要，不能代替缺失的基线或记录（见 handover-inputs.md）。

## 7. 改动检查清单

1. `cargo build -p tickwire-account-codec && cargo test -p tickwire-account-codec`（可编译目标）通过。
2. 改编码格式联动 [account-codec.md](../../specs/implementation/account-codec.md)；新增记录类型同步 `tickwire-account::RecoveryRecord` 与 `validate`。
3. 改动字节布局必须更新独立基准向量（hex/sha256），保持旧格式拒绝测试。
4. 本 crate 不做持久化、授权或网络。
