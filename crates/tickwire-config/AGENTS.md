# AGENTS.md — tickwire-config

## 职责

配置面的纯值层：把 TOML/JSON 转换到同一受限值树，产出 RFC 8785 键序规范字节与 SHA-256，并提供候选、补丁、准备、投影、locator、交付、变更授权、容量与制品目录等纯逻辑类型。无内部依赖、无 I/O、不启动进程、不授予任何部署或运行时授权。

## 公共 API 入口

- 根类型（`src/lib.rs`）：`Hash = [u8;32]`、`Format::{Json, Toml}`、`Limits { max_bytes=16MiB, max_depth=32 }`、`Error::{Encoding, Capacity, Scalar, Duplicate, Depth, Schema, Revision, Semantic}`。
- `Candidate { parse(format, bytes, limits) -> value()/canonical_bytes()/hash() }`；`validate_structural()`（编译时内嵌 `specs/contracts/system-config.schema.json`，Draft 2020-12 + format，关闭网络/文件引用）、`validate_static() -> validation::Resources`、`normalized_configuration(limits)`（显式补 `RoleBinding.cluster_scope=false` 后重新校验并算摘要）。
- `patch`：`Head { new(revision, candidate) / etag() / hash() }`、`ScopedPatch { parse / canonical_bytes / base_etag / hash / apply }`。
- `prepared`：`Input<'a>`、`PreparedCandidate { prepare / base_hash / candidate / changes }`。
- `plan`：`ComputedPlan::compute(base_revision, base, prepared, delivery, PlanOptions)`；`ApplyCeiling/ExposurePolicy/Action/Precondition/TargetPlan/ProjectionBinding`。getter 提供完整基线、候选、逐目标依赖动作、待验前提与规范摘要；`MAX_BYTES=1MiB`。revision 仅为调用方值，结果没有 Ready 或提交授权。
- `plan::EncodedPlan::decode(bytes, expected_digest)` 严格重读规范计划；`ComputedPlan::verify_encoded` 使用其固定选项和完整输入重算后逐字核对。`rules_digest()` 绑定编译时嵌入的 20 项规则源码/契约/构建输入，独立于既有计划字节，由服务归档外层保存。
- `projection`：`PinnedPolicy { decode / revision / reference }`、`Projection { value / canonical_bytes / hash / global_hash / policy_revision }`、`project(...)`。
- `projection_object`：`ObjectBinding` 与严格投影对象编解码。
- `locator`：`Locator { build / decode(format, bytes) / validate_successor / fields / canonical_bytes / hash / toml }`、`Fields`，`MAX_BYTES=16KiB`。
- `materialized`：`Kind`、`Stamp`、`ProjectionFile { render / decode / bytes / render_hash / value }`，`MAX_FILE_BYTES=8MiB`。
- `delivery`：`ProjectionLimits/ProjectionSet/ProjectionDelivery/DeliveryTarget/DeliveryKind`。
- `changes`：`Class/Role/Authority/RoleGrant/Change/ChangeSet`（`between(old,new)`、`authorize_plan/authorize_commit`、`visible_changes`）。
- `capacity`：`RetainedResources/HostInventory/HostCapacity/CapacityAssessment::check`。
- `artifacts`：`ArtifactCatalog/Dependency/DependencyReport/ParameterBinding/ParametersValidated`，`validate_parameters`、`reference_hash`。
- `validation`：静态语义检查入口 `check(value) -> Resources`。

## 在 workspace 中的位置

上游 `[dependencies]`：无内部 tickwire 依赖；第三方 `serde_json`、`sha2`、`jsonschema`、`toml`（strict 解析在 `strict.rs`）。

下游消费者（全仓解析 `Cargo.toml` 核对）：`tickwire-config-service`、`tickwire-config-materializer`。

## 对应规格

- [../../specs/implementation/config-candidate.md](../../specs/implementation/config-candidate.md)（值树、规范字节/摘要、静态校验、normalized 缺省补齐）
- [../../specs/implementation/config-locator.md](../../specs/implementation/config-locator.md)（固定 locator 内容、generation 递增、前驱摘要、`validate_successor`）
- 契约：[../../specs/contracts/system-config.schema.json](../../specs/contracts/system-config.schema.json)（编译时 `include_str!` 内嵌），配套脚本 `scripts/verify-config-contract.py`。

## 构建与测试

```sh
cargo build -p tickwire-config
cargo test -p tickwire-config
cargo clippy -p tickwire-config --all-targets -- -D warnings
cargo fmt -p tickwire-config
```

`tests/`：`candidate.rs`、`static_semantics.rs`、`patch.rs`、`prepared.rs`、`plan.rs`（纯内存闭包/依赖矩阵，fixture 在 `plan/support.rs`）、`plan_archive.rs`（严格重读、集合/字段/版本/预算拒绝、全动作往返、重算拒绝内容伪造，另只读 workspace 文件核对规则摘要）、`projection.rs`、`delivery.rs`、`changes.rs`、`capacity.rs`、`artifacts.rs`、`locator.rs`、`independent_review.rs`。契约另跑 `python3 scripts/verify-config-contract.py`（含生产模板 REQUIRED 未填必须拒绝的规则）。

## 关键不变量与注意事项

- **规范字节决定身份**：JSON 在构造对象时检查重复键（含转义后相同键）；拒绝越界整数、TOML 日期时间、无效编码；`-0` 与 `0` 同身份，`-0.0`/`-0e0` 浮点语法拒绝；对象键按 UTF-16 单元排序，数组顺序保留（`config-candidate.md`、`strict.rs`）。
- **输入预算**：候选 ≤16 MiB、容器深度 ≤64（默认 32）、值节点 ≤262144；策略参数另限 16 层；超出返回 `Capacity/Depth`。
- **schema 只使用本地引用**：`validate_structural` 关闭网络与文件引用提取；未知字段、带未解决内容的生产模板拒绝（`validate_structural`、`config-candidate.md`）。
- **normalized 只补唯一缺省**：仅 `RoleBinding.cluster_scope=false`，补齐后重新过预算与 schema 再算摘要；通用 `parse()` 保留原输入值，不自动补齐。
- **计划内容不代替事实**：目标提示不裁剪新旧完整交付集合；未知目标、错配基线/候选、超应用上限拒绝。逐目标按两侧依赖计算，投影未变仍可能有静默或换代动作。每目标保留待验 RuntimeCapability/RuntimeIdentity，真实 SafeReadIndex、资格、effective 和授权属于服务端；不得增加纯值 Ready 捷径。
- **归档重读仍非授权**：EncodedPlan 拒绝未知或缺失字段、重复键、非规范字节、重复/乱序集合及未知编码版本；恢复完整语义还须重算核对。带域计划摘要与复制对象正文 SHA-256 不可混用；规则摘要是源码身份，不是进程构建证明。
- **locator 链**：generation 十进制字符串 1..2^63-1、恰好加一、前驱摘要等于旧完整摘要、root 改变且 migration ID 不复用紧邻前次；首次前驱摘要为零；清单摘要始终非零（`locator.rs::validate_successor`、`config-locator.md`）。
- 本 crate `#![forbid(unsafe_code)]`，不读文件、不访问网络、不把校验通过变成部署授权。

## 改动检查清单

1. `cargo build -p tickwire-config && cargo test -p tickwire-config` 通过，并跑 `python3 scripts/verify-config-contract.py`。
2. 值树/规范字节/摘要规则改动联动 `config-candidate.md`；locator 链改动联动 `config-locator.md`。
3. schema 变更必须同步 `specs/contracts/system-config.schema.json`、`validate_structural` 与 `verify-config-contract.py`。
4. 新增变更类别/授权规则同步 `changes.rs` 的 plan/commit 授权与 `independent_review.rs` 测试。
5. 本 crate 不得引入 I/O、进程或授权签发逻辑。
