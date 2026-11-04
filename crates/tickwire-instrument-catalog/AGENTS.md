# tickwire-instrument-catalog

## 职责与边界

共享的有界标的经济目录。校验原始制品 SHA，严格解析经济正文，派生独立的目录语义身份，并生成可与实际 venue 类型精确比较的只读绑定。无 I/O，无授权，不包含账户费用、钱包或风险策略。

仅依赖 `tickwire-venue`、serde、serde_json、sha2，遵循 workspace 的 unsafe 禁止规则。规格见 [instrument-catalog.md](../../specs/implementation/instrument-catalog.md)。

## 公共 API

`Catalog::decode(raw, expected_artifact_sha256)`；`Artifact { digest, bytes }` 与 `Artifact::catalog()`；`Catalog::artifact()`。目录提供 raw/canonical 字节、两种摘要、版本、venue、环境、市场、资产及标的的只读 getter，以及按完整 u64 ID 查找和完整经济字段匹配。`Instrument::product/binding` 不接受外部声明的目录摘要。

## 不变量

- 原始正文及规范正文各至多 2 MiB，深度至多 16，资产 1..4096，标的 1..16384。
- 身份十进制字符串支持完整非零 u64，拒绝符号、前导零和溢出；金额由 venue Decimal 校验并要求规范正十进制文本。
- 未知/重复 JSON 字段、浮点、尾随、重复 ID 或 symbol、悬空资产引用和不兼容产品/合约均拒绝。
- 数值 ID 排序、固定字段顺序的规范正文决定带域语义摘要；原始 SHA 保存空白和字段顺序差异。
- Lookup 与匹配只读有序数组，不在热查询重新解析或分配。完整目录不等于某个账户或订阅的容量与授权。

## 验证

`cargo test -p tickwire-instrument-catalog`、`cargo clippy -p tickwire-instrument-catalog --all-targets -- -D warnings`、`cargo fmt -p tickwire-instrument-catalog -- --check`。测试覆盖合法高位 ID、费用资产、完整字段比较、哈希分离及以重新计算合法原始 SHA 的畸形/经济语义反例。

共享向量位于 `specs/contracts/instrument-catalog-v1.json`，Rust `tests/vectors.rs` 与独立标准库脚本 `python3 scripts/verify-instrument-catalog.py` 使用同一原始正文、规范字节与双摘要。脚本使用 Decimal 校验定点规则，拒绝 JSON 浮点与重复字段，另外执行实际容量边界。
