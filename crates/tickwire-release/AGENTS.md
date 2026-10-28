# tickwire-release

## 1. 职责（架构位置：策略进程侧 —— 不可变发布承诺与共享对象校验）

不可变发布承诺库：清单/对象/能力/目标校验，rebuild（声明重建对象）与 replay（版本化合并输入、锚点/尾部、安装与回滚）共享同一套对象内容与绑定校验。本 crate 只校验内容与绑定；**只有合格 loader 才能证明进程真正加载或恢复了它**。lib 根文档：`Immutable release commitments. These types validate content and binding; only the qualified loader can attest that a process loaded or restored it.`

## 2. 公共 API 入口

- 根类型：`Hash=[u8;32]`、`Error/Result`、`Binding`（operation_id/revision/logical_strategy/members_digest）、`Object{digest,bytes}`（`Object::of`）、`Role`（Binary/HostConfig/Bootstrap/StrategyConfig/RecoveryTemplate/Model(u16)/Dependency(u16)）、`Artifact{role,object}`、`Product/PositionMode/OrderKind/Method::{Restore,Migrate,Rebuild}`、`Capabilities`、`Requirements`、`Target`、`ReleaseManifest`、`InputPrefix`、`Migration/MigrationSet`、`ProbeInput`、`Rollback/RollbackPlan`、`ReleaseBundle`、`MemberRelease<'a>`；`pub use tickwire_lifecycle::{Member, Operation}`。常量 `MAX_DOCUMENT=1MiB`、`MAX_FILES=64`、`MAX_BINARY=512MiB`。
- `closure`：`Verified`（completion/restored/rollback 三份回执 + bundle + 对象）、`verify(root, read)` 有界完成图递归校验。
- `files`：`ObjectFile`、`Location`、`LoadedFiles`、`load(required, locations)`、`read_object`（受信私有目录按摘要读取，长度+摘要双核对）。
- `receipt`：`Process{instance,generation,pid,uid,gid}`、`Kind`、`Receipt`、`Completion`（解码不是认证，Host 还要对照 Registry 进程与已认证消息）。
- `rebuild`：`Plan/Plans`、`Contract`、`Inputs`、`Frontier`、`MarketRequirement`、`ModelBinding`、`ActionBinding`、`MarketSeed`、`AccountPrefix`、`TimedInput`、`Ordering`、`MAX_ACTIONS=256`；子模块 `rollback`（`Verified`、`Computation`）、`window`。
- `replay`：`Source/ClockDomain/StreamKey/Cursor/VersionedBytes/Model/Timer/Rng/EffectCut/RuntimeState/Cut/InputCommit/Checkpoint/Effect/Input/MergedInput/VerifiedAnchor/VerifiedTail/CapturedTail/Head/ControlInput`，`canonical/runtime_object/verify_anchor/row_chain/encode_tail/verify_tail`；子模块 `history`（`CommittedHistory::verify`）、`install`（`Request/Installation/Continued`，`install/verify::Verified::verify`）、`retained`（`RecordedCut::decode`）、`rollback`（`Job/Receipt/Completion/Verified`）、`source`（`SourceJob/SourceReceipt/Completion/VerifiedSourceReplay`）、`startup`（`Verified::verify`、`verify_seal`）。常量 `MAX_BYTES=16MiB`、`MAX_ROWS=65536`、`MAX_STREAMS=64`、`MAX_TIMERS=256`、`MAX_EFFECTS=32`。

## 3. workspace 位置

上游（`Cargo.toml` 实际声明）：`tickwire-handover`、`tickwire-ipc`、`tickwire-lifecycle`、`tickwire-recovery`；第三方 `sha2/serde/serde_json/libc`。

下游（grep 命中）：`tickwire-strategy-sdk`、`tickwire-strategy-host`、`tickwire-strategy-example`、`tickwire-runtime`、`tickwire-replica`、`tickwire-app`、`tickwire-lab`。

## 4. 对应规格

- [../../specs/implementation/release-preflight.md](../../specs/implementation/release-preflight.md)（三份规范对象、Operation/member、文件长度摘要、目标与回滚能力校验；闭包最多 256 对象、来源最多 16 层）
- [../../specs/implementation/strategy-rebuild.md](../../specs/implementation/strategy-rebuild.md)（`rebuild` 声明重建对象与固定动作契约）
- [../../specs/implementation/strategy-replay.md](../../specs/implementation/strategy-replay.md)（`replay` 版本化输入、锚点/尾部、schema3 安装与回滚校验）

## 5. 构建与测试

```sh
cargo build -p tickwire-release
cargo test -p tickwire-release
```

无 bin/example。`tests/replay.rs` 为纯对象校验；`tests/retained_installation.rs` 的实际保留安装测试带 `#[ignore = "requires TICKWIRE_INSTALL_DELTA_FIXTURE"]`，需设置环境变量指向只读 fixture 目录才运行。

## 6. 关键不变量与注意事项

- **校验 ≠ 授权**：lib 根注释与 `receipt.rs` 头注释都强调“Decoding is not authentication: Host also checks this against the Registry-owned process and its authenticated message.”；内容合法不代表进程已加载。
- **文件不可变性靠 exec 全程保持**：`files.rs` 头注释“a hash alone does not protect a pathname from a later same-UID replacement”，部署 owner 负责私有目录独占。
- **rebuild 内容验证不建立活进程**：`rebuild.rs` 头注释“Content verification does not establish a live process, authorize output, or certify a rollback strategy.”
- **replay 字节连续 ≠ 回调真实执行**：`replay.rs` 头注释“Validation here proves byte continuity, not that a strategy executed the callbacks; the SDK verifies the latter.”
- **MarketTransition 记录边界**：保存非零 operation/approval 标识、旧 Cursor、新 StreamKey、TWSB0001 基线及激活前 catchup，不允许 effects；仍受类型化 16 MiB 限制。`market_lineage::after_cut` 逐条重算原始输入摘要，在 C、换代 previous、末尾核对完整 Cursor，为 schema3 安装选择完整 C 后输入；拒绝相同盘口但原始输入不同、缺失换代或跳过 C。SDK 另核对完整视图与真实回调。记录中的审批摘要只绑定运行时决定，不授予 JOIN、配置或交易权限。
- **生产者 Cursor 与本地 JOIN 证据分开**：新转换 `cursor_basis=1` 经 `market_cursor_digest` 将最后一条真实 catchup 规范为 MarketApplied；独立消费者的本地快照/游标/时间不改变相同生产者 frontier 的 Cursor，原始记录或 ingress 差异仍改变摘要。完整转换继续进入 merged row/hash 与全量重放。旧 basis0 字段省略并保持原编码/散列；未知 basis、空 catchup 或 frontier 不匹配拒绝。
- **闭包先同步后发布**：`closure.rs` 验证数据与先前回调，活进程所有权仍由 Host 在发布前检查。
- 上限常量集中在根/`replay`/`rebuild`，新增对象类型时先扩 `MAX_*` 并同步规格预算。
- schema3 启动、安装与回滚闭包的不透明依赖沿 `tickwire-recovery` 的单对象 64 MiB、总量 256 MiB 预算；`Request.source_objects` 和 `rollback::Job.objects` 可承载这些依赖。类型化重放记录、控制请求、完成根与检查点仍限制为 `replay::MAX_BYTES`（16 MiB），所有引用继续核对精确长度与 SHA-256。

## 7. 改动检查清单

1. `cargo build -p tickwire-release && cargo test -p tickwire-release` 通过。
2. 改清单/能力/目标校验联动 [release-preflight.md](../../specs/implementation/release-preflight.md)；重建联动 [strategy-rebuild.md](../../specs/implementation/strategy-rebuild.md)；重放/安装/回滚联动 [strategy-replay.md](../../specs/implementation/strategy-replay.md)。
3. 新增对象角色/预算时核对 `tickwire-recovery` 闭包字节上限与 `specs/contracts` 既有向量。
4. 本 crate 不得引入网络、授权决策或活进程资格判定。
