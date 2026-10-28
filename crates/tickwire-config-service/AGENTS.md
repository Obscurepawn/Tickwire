# AGENTS.md — tickwire-config-service

## 职责

配置面持久控制状态：在持久控制共识组（`tickwire-control-raft` 的 `Node`）上维护确定性复制状态（对象 Begin/Chunk/Seal、bootstrap 提交、mTLS 认证、角色授权、安全读），并提供有界 HTTPS server。**对象复制本身不授权配置部署**；部署资格由 evidence/账户/凭据/资源事实与 deployment owner 分别核验。

## 公共 API 入口

- `Replica<S: Store>`（`src/lib.rs`）：`new(node, config)` / `new_with_bootstrap(node, config, trust)`；`tick/campaign/step/initialize/bootstrap/propose/drive`；`request_read() -> ReadId`、`read_object(ticket, key) -> Read`、`read_head(ticket) -> HeadRead`；`is_leader()`、`scanned_through()`。
- 类型：`Config { cluster, objects: ObjectLimits }`、`ObjectCommand::{Begin, Chunk, Seal}`、`Change::{Initialized, Begun, ChunkStored{received}, Sealed, Bootstrapped{revision}, PreparedStored{id,index,term}}`、`AppliedResult { matches(proposal), index(), result() }`、`Output { messages, applied, more_ready }`、`Read::{Pending, Ready(Option<Object>)}`；重导出 `Object/ObjectKey/ObjectLimits/ObjectSpec/Purpose`。
- `ReplicaControl<'a, S>`（`peer_driver.rs`，窄门面，不暴露 Node）：`initialize/propose/bootstrap/request_read/read_head/read_object/propose_membership/membership_receipt/compact_snapshot/install_deployment_owner/replace_deployment_sources`。
- HTTP/认证：
  - `api`：`Endpoint::{Validate, Preview, Plans, Plan(id), PlanDiff(id), Commit, RollbackPlan, Head, Desired, Revision(n), Effective(Option), Operation(id), OperationByKey(id), Resume(id), Watch, Schema}`、`Request { parse/endpoint/if_match/idempotency_key/last_event_id/body/digest }`、`read_request(stream, max_body)`；`MAX_HEADER_BYTES=16KiB`、`MAX_BODY_BYTES=16MiB`。
  - `auth`：`ServerTls::new(material, peers)`、`accept/accept_until/revalidate`、`AuthenticatedPrincipal { subject(), leaf_sha256() }`、`AuthenticatedStream`。
  - `authorization::RoleBindings::from_candidate(candidate)` + `authorize(...)`。
  - `https::Server::bind(address, trust, limits)`、`poll`、`replace_trust`、`shutdown(control)`、`local_addr()`；`Limits/Status`。
- 事实与资格：`deployment_owner::{Owner, Job, Assessment}`、`deployment::{Sources, ContentValidated}`、`qualification::{qualification_basis, ReportClosure::check}`、`evidence::{Kind, Issuer, Claim, Trust, VerifiedEvidence, Catalog}`、`bootstrap::{BootstrapTrust, Bootstrap, CommittedHead, HeadRead}`、`account_facts::AccountModes`、`credential_facts::CredentialFacts`、`resource_facts::RetainedCapacity`、`source_bundle::Bundle`、`snapshot::ReplicaRestore`。
- 计划准备：`planning::{PlanInput, PendingPlan, PlanProgress, PlanDraft}`；`ReplicaControl::begin_plan/poll_plan/cancel_plan` 使用两次实际 ReadIndex、当前 mTLS/RBAC 与同 Replica 身份。返回内部完整草案；放弃时显式取消，HTTP scoped 视图与持久准入仍待接入。
- 内容归档：`plan_archive::{PendingRecord, RecordProgress}`、`ReplicaControl::begin_record_plan/poll_record_plan/cancel_record_plan`；`PendingRecord::observe` 接收实际提交回执，最后安全读取 `publication::StoredPrepared`。`read_prepared(ticket, PreparedQuery)` 同时返回提交头和首次归档记录。它们是内部内容保存接口，不授予发布资格。
- 运行实例事实：`runtime_facts::{IssuerScope, RuntimeTrust, RuntimeTargets}`、`RuntimeTargets::check(Input, Sources, now, Limits)` 核对签发范围、完整基线投影、构建与实例身份、来源期限；签名观测保留 epoch/sequence，实际 provider 与水位持久防回退另行接入。
- 常量 `CHUNK_BYTES=64KiB`、`MAX_COMMAND_BYTES=1MiB`；`Error::{Identity, Bounds, Protocol, NotReady, Conflict, NotFound, Incomplete, Digest, Capacity, Consensus, Poisoned, Authentication, Authorization, Transport, Configuration}`。

## 在 workspace 中的位置

上游 `[dependencies]`：`tickwire-control-raft`、`tickwire-journal`、`tickwire-config`；第三方 `rustls`、`serde_json`、`jsonschema` 等。

下游消费者（全仓 grep `Cargo.toml` 核对）：`tickwire-control-peer`（`ApiOwner` 持有 `Replica<GenerationStore>`）。

## 对应规格

- [../../specs/implementation/config-control-state.md](../../specs/implementation/config-control-state.md)（确定性控制状态、复制、mTLS、安全读、HTTPS 有界连接）
- [../../specs/implementation/config-credential-facts.md](../../specs/implementation/config-credential-facts.md)（凭据/用量事实核验）
- [../../specs/implementation/config-desired-read.md](../../specs/implementation/config-desired-read.md)（desired/revision 安全读与权限）
- [config-plan-archive.md](../../specs/implementation/config-plan-archive.md) 与 [config-archive-sources.md](../../specs/implementation/config-archive-sources.md)（完整历史依赖、确定性恢复与信任/资料原字节）
- 启动组合见 [../../specs/implementation/config-server.md](../../specs/implementation/config-server.md)（由 `tickwire-control-peer` 装配）

## 构建与测试

```sh
cargo build -p tickwire-config-service
cargo test -p tickwire-config-service
cargo clippy -p tickwire-config-service --all-targets -- -D warnings
cargo fmt -p tickwire-config-service
```

`tests/`：`bootstrap.rs`、`replicated_objects.rs`、`control_api.rs`、`control_tls.rs`、`peer_driver.rs`、`deployment.rs`、`deployment_owner.rs`、`evidence.rs`、`qualification.rs`、`account_facts.rs`、`credential_facts.rs`、`resource_facts.rs`、`source_bundle.rs`，fixture 在 `tests/fixtures/`、`tests/deployment_support/`、`tests/control_reads/`。

## 关键不变量与注意事项

- **复制不授权**：对象 Begin/Chunk/Seal 只是共识复制事实；部署资格必须另过 evidence 信任、账户模式、凭据用量、保留容量与 deployment owner 评估（`lib.rs` 头注释、`deployment.rs`、`deployment_owner.rs`）。
- **扫描水位单调**：`apply` 拒绝 `index <= scanned_through` 的重复回放；`read_object/read_head` 要求 `scanned_through >= ReadBarrier.index` 才返回 Ready（`lib.rs`）。
- **leader 与安全读**：非 leader 或扫描未达 barrier 返回 `NotReady/Pending`；HTTPS 层在原控制 owner 上 begin/poll/cancel，慢正文与未完成握手不阻塞 Raft（`config-control-state.md`）。
- **mTLS 身份**：`ServerTls` 钉住 client leaf SHA-256 与 subject，`revalidate` 在每次使用前复核证书/CRL；新信任在下一次 begin/poll 生效（`auth.rs`、`https.rs`）。
- **bootstrap 签名资格**：成功加载 bootstrap 不授予部署权限；签名 envelope 的当前资格在实际候选检查时由服务时钟核验（`bootstrap.rs`、`config-server.md`）。
- **poisoned**：`drive` 失败置 `poisoned`，后续所有入口返回 `Poisoned`。

## 已知边界 / TODO（partial）

- 已完成：对象复制、bootstrap 提交、mTLS 认证、角色授权、安全读（Head/Desired/Revision/Schema/Validate）。
- `request.rs` 中 `Plan/PlanDiff/Effective/Operation/OperationByKey/Watch` 返回 503 `UnsupportedEndpoint`；`Preview/Commit/RollbackPlan/Resume` 走 503 `ControlUnavailable`：**内部计划器和内容归档已接线；HTTP 计划准入、常规提交、操作状态机、watch 推送尚未实现**，改动前先确认对应规格章节是否已落地。

## 改动检查清单

1. `cargo build -p tickwire-config-service && cargo test -p tickwire-config-service` 通过。
2. 复制状态/水位/安全读联动 `config-control-state.md`；凭据事实联动 `config-credential-facts.md`；desired 读联动 `config-desired-read.md`。
3. HTTP 路由/状态码改动同步 `api.rs::Endpoint` 与 `control_api.openapi.json`（如存在）。
4. 开启 503 端点前先补 `request.rs` 处理、对应规格章节与测试，不得把复制成功当部署成功。
5. 本 crate 不直接部署配置、不启动进程、不签发交易授权。

## 配置规划新增边界

`deployment_owner::Owner::{start_plan,poll_plan,revalidate_plan}`、`PlanJob/PlanAssessment` 与普通部署核验共享最多 8 个槽位，完成未消费结果继续占额。结果同时持有 candidate content 与旧新投影，合计投影字节受原预算限制。来源替换、跨 owner、过期和时钟回退拒绝。详见 [config-planning.md](../../specs/implementation/config-planning.md) 与 [config-runtime-facts.md](../../specs/implementation/config-runtime-facts.md)。当前登记头、实际运行实例 provider、计划发布准入、普通 revision/operation、实际配置应用和双收据集群完成仍待实现；不得将这些内部类型当成 Ready 或 Succeeded。

## 计划归档新增边界

归档保留原始 Full/Patch、真实提交基线、候选、规范计划、规划规则摘要、完整旧新投影、全部资料包与信任配置。依赖先上传并封口，最后原子登记根与主体幂等键；同请求重试返回首次位置。来源替换、过期和证书撤销会终止未提交保存；已提交事实由当前权限下的安全读返回。取消不撤销已经提交的对象，没有对象垃圾回收。

历史恢复只校验固定格式、封口闭包及历史绑定，不根据当前时钟或当前业务规则改判旧事实；重新用于发布时必须重新核验。旧空归档快照保持原字节，非空归档使用显式新格式并校验完整依赖。desired 仍为引导修订，不生成 Ready、操作或运行时收据。
