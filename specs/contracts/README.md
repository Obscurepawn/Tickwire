# 机器契约说明（`contracts/`）

本目录七个 JSON 是设计事实的机器可读来源：字节布局与 golden 向量、风险数值、全局配置 schema、控制 API、复制画像与参考部署。它们不连接 Binance、不读取凭据，由 `scripts/verify-*.py` 在设计时核验；Rust 运行时另有测试证据，二者不互相冒充。全局索引见 [`../INDEX.md`](../INDEX.md)。

## 1. `ipc-wire-v1.json` — IPC ABI 线格式契约

- 身份：`format = tickwire-ipc-contract-1`，`abi = {major: 1, minor: 0}`，小端、二进制补码、16 字节不透明 ID、保留位零填充，记录头 128 字节、控制头 128 字节。
- 顶层结构：
  - `layouts`：固定记录/控制布局（字段、对齐、类型宽度、保留字节）。
  - `enums`：snapshot_section_kind、config_apply_class/config_status/config_actor_kind/config_file_kind、projection_transport/format、clock_domain、rounding_policy、ledger_adjustment_kind、relation_kind、resource_kind、architecture、endian、trust_profile、qos、venue、product、event_class、producer_lifecycle。
  - `bitsets`：config_flags、report_value_validity、features、record_flags、control_flags、permissions、intent_flags、depth_flags、trade_flags、report_flags、coverage_flags、join_flags、instrument_flags、position_modes。
  - `messages`：Quote/Trade/Depth/MarkFunding/FeedState/各类 Intent（New/Cancel/CancelReplace/AmendKeepPriority/Modify/QueryOrder/CancelList/CancelAlgo）与回报（IntentStatus/OrderState/Fill/Balance/Position）的 body_layout 与允许字段。
  - `control_opcodes`、`variable_bodies`（如 RESOURCE_OFFER）。
  - `profiles.production_reference`：页面字节、隔离字节、序列上限、最大控制帧/FD、每 topic 最大消费者、触发引用、深度档位、payload/快照预算、SPSC bank、租约与执行/授权 TTL。
  - `validation_rules`、`message_constraints`（每消息 message_id/body_layout/allowed_fields）、`report_value_fields`、`resource_kind_constraints`（各 bank/cursor/快照/目录/配置投影的容量规则与 seal_profile）、`topic_snapshot_manifest`、`control_reply_rules`、`configuration_control`（特性位、哈希语义、role_rules、投影资源、finalization_receipt）、`exact_market_events`（feature bit、DecimalWire、来源 Json/SpotSbeV1）。
- 消费：`scripts/verify-ipc-contract.py`（布局覆盖、字段对齐、保留字节、类型宽度）；`verify-design-contracts.py` 与 `verify-config-contract.py` 读取 `profiles.production_reference` 做容量一致性。

## 2. `ipc-vectors-v1.json` — IPC golden 向量与语义案例

- 身份：`format = tickwire-ipc-vectors-1`，`abi_major = 1, abi_minor = 0`，CRC32C（reflected polynomial/initial/xorout，含 ASCII 校验与十六进制校验工具开关）。
- 顶层结构：`vectors`（94 条编码 golden 向量）、`semantic_cases`（33 条语义/畸形输入案例）、`compatibility_baseline`（37 条兼容基线样本：quote、depth inline/arena、new_limit_intent、fill_report、freeze、switch_applied、join_snapshot、release_risk、freeze envelope/frame、instrument、depth_level_signed、cursor page、topic member/header/level/manifest）。
- 消费：`scripts/verify-ipc-contract.py` 重算编码哈希、帧校验并逐项比对；`tickwire-ipc` Rust 回归使用同一批向量。

## 3. `risk-vectors-v1.json` — 风险计算数值契约

- 身份：`schema_version = 1`；含 `contract`、`synthetic`、`production_parameters`、`valuation_date`、`decimal_encoding`、`default_scales`（quantity/price/money/rate 均为 8）、`rounding`（liability/asset、signed_floor/signed_ceil）、`arithmetic`、`notes`、`cases`（21 个案例、131 项预期结果）。
- 用途：用 Decimal 重算 Spot 可用余额、USD-M 保证金/未实现亏损/执行费/平仓费/资金费/缓冲与舍入尾额，逐项比对预期数值；不使用浮点。
- 消费：`scripts/verify-risk-vectors.py`；`tickwire-risk` 测试读取合成向量。

## 4. `system-config.schema.json` — 全局配置 schema（Draft 2020-12）

- 顶层 `properties`：`identity`、`hosts`、`processes`、`venues`、`ipc`、`strategies`、`accounts`、`risk_policies`、`journal`、`replication`、`control`、`local_config`、`observability`、`timeouts`、`qualification`；`$defs` 提供 Host、Process、Venue、Topic、RiskPolicy、Account、Strategy、ParameterValue、Voter、RoleBinding、Materialization、AccountView、VenueView、SecretBinding、ProjectionPayload、ObservabilityView、ProcessProjection、ProjectionObject、ConfigLocator；`allOf` 与 `x-semantic-checks` 承载跨字段约束。
- 用途：校验全量与投影配置、参考 TOML、未知字段/未解决引用/未填写生产负例。
- 消费：`scripts/verify-config-contract.py`；`tickwire-config` 的 `validate_structural()`。

## 5. `control-api.openapi.json` — 配置控制 API（OpenAPI 3.1.1，v1.0.0-design）

- 安全方案 `components.securitySchemes.mutualTLS`；公共参数 IfMatch/IdempotencyKey/PlanId/OperationId/Revision。
- 路径：`POST /v1/config/validate`、`POST /v1/config/preview`、`POST /v1/config/plans`、`GET /v1/config/plans/{plan_id}`、`GET /v1/config/plans/{plan_id}/diff`、`POST /v1/config/commits`、`POST /v1/config/rollback-plans`、`GET /v1/config/head`、`GET /v1/config/desired`、`GET /v1/config/revisions/{revision}`、`GET /v1/config/effective`、`GET /v1/config/operations/{operation_id}`、`GET /v1/config/operations`、`POST /v1/config/operations/{operation_id}/resume`、`GET /v1/config/watch`、`GET /v1/config/schema`。
- `components.schemas`：Error、Candidate、PatchSelector/PatchChange/ScopedPatch、ValidationRequest/Issue/ValidationResult、PlanRequest/DiffEntry/Target/PlannedSuccessor/Plan、AuthorizationScope/DeliveryScope、CommitRequest、RollbackPlanRequest、FileReceipt、TargetStatus、Operation、Head/Desired/Effective/WatchEvent/ResumeRequest、CommittedHead/BootstrapHead/CommittedDesired/BootstrapDesired；`x-role-semantics` 与 `x-idempotency` 标注角色与幂等语义。
- 消费：`scripts/verify-design-contracts.py --openapi`（标准验证器）；`scripts/verify-config-contract.py`（接口引用交叉核对）。

## 6. `replication-profile-v1.json` — 复制与主备画像

- 身份：`contract_name = tickwire-replication-profile`，`contract_version = 1`，`design_version = 0.3`，`profile_id = LocalDurable+AsyncReplica`（`default_profile`）。
- 顶层结构：`topology`（执行主机/副本/control voters/quorum/故障域）、`profiles`（LocalDurable+AsyncReplica 与 SyncReplica 的数据共识、本地持久、远程 ACK、缺尾部动作、自动提升条件、固定 RTO）、`groups`（control Raft 组、风险数据组、WAL 规则、leader 共处）、`control_raft`（tick/心跳/选举超时/check_quorum/pre_vote/read_only）、`engine`（名称/crate/版本/raft 0.7.0/git 与 sha256/存储适配器）、`batching`（意图批/输入 blob/pending 上限）、`qualification_targets`（完整 tick-to-socket p99/p999）、`wal`（格式版本/帧头尾/对齐/段与帧上限/帧类型/CRC/哈希）、`async_replication`（协议/消息/批次/乱序缓冲/ACK/滞后阈值）、`dependencies`（清单/收据/恢复模式/固定引脚/GC）、`snapshot`（触发/分块/保留代次/原子发布）、`membership`（voter 变更/Joint/替换）、`execution_authority`、`fencing`、`guardian`、`evidence_encoding`、`evidence_contracts`、`deterministic_scenarios`、`implementation_evidence`。
- 消费：`scripts/verify-design-contracts.py`（与部署、延迟合同对照）。

## 7. `deployment-reference-v1.json` — 参考部署容量与 SLO

- 身份：`format = tickwire-deployment-reference-1`，`design_version = 0.3`，`profile_id = TW-LD-ASYNC-16x2-v1`。
- 顶层结构：`purpose`、`default_storage_profile`、`data_plane_consensus`、`control_plane`（voters/quorum/实现/单订单往返）、`hosts`、`isolation`（同区/不同主机电源域/cgroup v2/禁 swap/锁内存/fork 不继承）、`cpu_reference`（os控制/irq/md/exec/guardian/日志复制/recorder/活动与影子策略/余量）、`process_capacity`（每主机 md、exec 风险域、策略实例、topic、消费者、事件类别/键）、`shm`（bank/SPSC/快照/记录/arena/游标预算）、`memory_unique_budget_bytes`（共享内存/各私有/日志缓存/控制监督/OS 余量）、`load_contract`（事件率/突发/意图速率/磁盘预算/checkpoint）、`storage_qualification`（掉电保护介质/持续写/组提交/本地提交延迟/阈值）、`timing`（租约/心跳/选举/fencing/接管/配置期限）、`slo`（normal/sync_replica_optional/recovery 的 p99/p999 与 RTO，tail_evidence 采样）、`qualification_only`（合成资本，非真实账户）、`runtime_binding_required`、`contract_references`（ipc/replication/risk_vectors/runtime_schema）、`not_guaranteed`。
- 用途：模拟环境结构与容量参考；`REQUIRED` 生产输入必须替换并通过运行时准入，文件解析成功不代表可上线。
- 消费：`scripts/verify-design-contracts.py`（内存/CPU/存储/延迟跨文件一致）；`scripts/verify-config-contract.py`（dep 对照）。
