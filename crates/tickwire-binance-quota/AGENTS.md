# AGENTS.md — tickwire-binance-quota

## 职责
把 Binance 的时间查询、私有控制、公共行情、完整交易命令和只读对账查询映射到同一个核心 `ExecLimiter`（`tickwire-quota`）：精确匹配 Source 与 Action 的费用表、完整桶列表、费用类别与网络报告权重，负责策略安装/续租/冷却/冻结、完整尝试身份留存与精确重试复检。它是运行期所有出站请求的共享本地额度闸门，自身不发网络请求。

## 公共 API 入口
- `Endpoint`（`lib.rs`）：`new(limiter: ExecLimiter, producer: [u8;16], domain, policy: Policy)`、`prepare_policy/commit_policy`、`consume_time/consume_private/consume_public/consume_read/consume_command/consume_command_session/consume_guardian_connect`、`time/private/public/read(binding) -> 对应 Permit`、`remaining(key, class)`、`consumer() -> &ExecLimiter`、`producer_id()`、`install/renew/cooldown/freeze/is_frozen/stop/last_error`。
- 绑定结构：`TimeBinding`、`PrivateBinding`（含 `AccountScope`）、`PublicBinding`、`CommandBinding { owner_id, risk_domain_id, account, product, environment, domain }`、`ReadBinding`。
- 动作枚举：`Action`（`TimeConnect/TimeQuery/PrivateStreamConnect/PrivateManagementConnect/PrivateUpgrade/PrivatePong/PrivateClose/SpotLogon/SpotSubscribe/.../UsdmCreate/UsdmKeepalive/UsdmClose/Read(ReadAction)/Public{lane,kind}/Command{transport,action}`）、`CommandAction::{New, Cancel, CancelReplace, Amend, Modify, Query, CancelList, Algo, CancelAlgo, SpotList{..}, PeggedNew, AdvancedCancelReplace, Sor}`、`CommandSessionAction::{Connect=1, Upgrade=2, Ping=3, Pong=4, Close=5}`、`ReadAction::{Connect, TradePage, AccountSnapshot, OpenOrders, ConditionalOrders, OrderQuery, Income, StructureQuery, AmendmentHistory}`、`ReadQuery`。
- 策略：`Policy { generation, starts_ns, ends_ns, rules: Vec<Rule> }`、`Rule { source, action, class, api_weight, websocket_messages, costs }`、`PreparedPolicy`；常量 `MAX_RULES = 256`、`MAX_IDENTITIES = 4096`。
- Permit：`TimePermit`、`ControlPermit`、`PublicPermit`、`ReadPermit`（同时实现 `tickwire-binance-account::reconciliation` 的 `ReadPermit`/`SnapshotPermit` 等只读闸门）。
- `command_action(body: &venue::CommandBody) -> CommandAction`；再导出 `quota::{Class, Consumed, Cost}`、`Transport`、`ClockDomain/MonoTime/Source`。
- `pub mod bootstrap`：部署配置 `Config { supervisor_generation, consumer_instance, consumer_generation, consumer_lease_until_ns, producer_id, policy_generation, policy_starts_ns, policy_ends_ns, windows, grants, rules }`、`Built<S> { coordinator, endpoint }`、`Config::resolve_costs/policy/build`。

无 binary、无 example。

## 在 workspace 中的位置
- 上游依赖（`[dependencies]` 中的 tickwire-*）：`tickwire-quota`、`tickwire-clock`、`tickwire-binance-clock`、`tickwire-binance-private`、`tickwire-binance-public`、`tickwire-binance-account`、`tickwire-binance`、`tickwire-venue`；外部 `sha2`、`serde`。
- 下游（已在各 `Cargo.toml` 核对）：`tickwire-app`、`tickwire-binance-execution`、`tickwire-guardian`、`tickwire-lab`、`tickwire-md`、`tickwire-runtime`。

## 对应规格
- ../../specs/implementation/binance-quota.md

## 构建与测试
```sh
cargo test -p tickwire-binance-quota
cargo build -p tickwire-binance-quota
cargo clippy -p tickwire-binance-quota --all-targets -- -D warnings
cargo fmt -p tickwire-binance-quota
```
`tests/permits.rs` 覆盖各类绑定的允许/拒绝、策略世代推进、精确重试复检、冻结与身份留存；无 OS 条件编译。

## 关键不变量与注意事项
- **费用表精确匹配**：未配置的 (source, action) 或报告权重不符即拒绝；规则按 (source, action) 唯一，窗口严格递增，costs 非空且 units>0（出处：`validate_policy`、`binance-quota.md`）。
- **策略世代单调**：`prepare_policy` 要求新 generation > 当前、ends_ns 未过、domain 一致；`commit_policy` 复检 previous_generation/producer/domain，防止并发替换（出处：`prepare_policy/commit_policy`）。
- **完整尝试身份最多 4096 条不淘汰**：私有请求绑定账户/凭据代次/连接代次/内容摘要；精确重试重新检查原授权期限，冲突即冻结；`consumer()` 只给 supervisor 发放与续租所需的只读引用（出处：`binance-quota.md`、`Endpoint`）。
- **公共 429/418 冻结整个共享消费者**：`freeze()` 后所有消费拒绝；本层不区分单用户（出处：规格与 `freeze/is_frozen`）。
- **命令消费不带网络报告权重**：`consume_command` 的 api_weight/messages 显式为 0，命令费用来自规则显式 costs（出处：`consume_command` 注释）。
- **身份/域校验**：`binding()` 检查 owner 非零、domain 与 Endpoint 及 now 域一致，不符直接 freeze 并返回 Identity。
- 本 crate 不实现桶算法本身，桶/lease/cooldown 在 `tickwire-quota::ExecLimiter`。

## 改动检查清单
```sh
cargo test -p tickwire-binance-quota
cargo clippy -p tickwire-binance-quota --all-targets -- -D warnings
cargo fmt -p tickwire-binance-quota
```
改费用表/动作映射后跑 `tests/permits.rs`；新增 Action 必须同时更新 `bootstrap` 的序列化枚举与 `validate_policy` 唯一性检查。配额契约见 `binance-quota.md`。
