# AGENTS.md — tickwire-guardian

## 职责

监督与紧急执行面：独立、受审计的 Binance 查询/撤单紧急 owner。与主执行进程隔离，只接受 Query/普通 Cancel/CancelList/CancelAlgo，在隔离（fencing）或 emergency 签名授权下经真实 REST/TLS 执行，全部命令与结果先写审计 WAL。自带 CLI（`--config`），不使用执行进程 RPC。

## 公共 API 入口

- `Guardian<S: Store, C: MonotonicClock>`（`src/lib.rs`）：
  - `Guardian::create(store, config, verifier, clock, quota, network)` / `Guardian::recover(store, config, verifier, clock, quota, network, required)`。
  - 校时：`start_calibration() -> u64`、`poll_calibration(budget) -> PollReport`、`calibration_in_flight()`、`register_clock(registry, token)`。
  - 提交：`submit(command, authorization, credentials) -> Hash`；`poll(budget) -> Option<PollResult>`；`register(registry, token)`、`busy()`。
  - 观察：`outcome(digest) -> Option<Outcome>`、`contains(digest)`、`unresolved()`、`audit_position()`、`local_time()`。
  - `start_quota_run(producer, consumer, generation)`（任何校时/连接前持久记录配额消费者身份）。
- 类型：`Config { guardian_id, scope, old_identity, audit, writer_epoch, allowed_instruments, max_requests, recv_window_micros, delivery_budget_ns, request_timeout_ns, max_response_bytes, audit_degraded }`；`Authorization::{Emergency(SignedDocument), Fenced(SignedDocument)}`；`Network { address, tls, limits }`；`ResultKind::{Unknown, NoBytes, Response, Rejected, NotFound, RateLimited, Authentication}`；`Outcome { kind, status, accepted_bytes, request_bytes, raw_response, raw_digest, received_ns, audit_degraded }`；`PollResult { command_digest, outcome }`。
- `audit`（`src/audit.rs`）：`Audit::create/recover/append/position/events/start_quota_run`，事件 `Start/Complete/QuotaRun/Config` 分块写 WAL。
- CLI（`src/cli.rs`）：`Deployment` 严格 JSON、`Calibration`、`Algorithm::{Hmac, RsaPkcs8, Ed25519Pkcs8}`、`main_entry()`、`run(deployment)`。
- `Error::{Invalid, Identity, Denied, Busy, Capacity, AlreadyUsed, Audit, Clock, Transport, Encoding, Poisoned}`。

### 可执行入口

- `src/main.rs` → bin `tickwire-guardian`：`cargo run -p tickwire-guardian -- --config /absolute/guardian.json`（`cli::main_entry`）。

## 在 workspace 中的位置

上游 `[dependencies]`：`tickwire-fencing`、`tickwire-binance`、`tickwire-binance-clock`、`tickwire-binance-quota`、`tickwire-clock`、`tickwire-journal`、`tickwire-quota`、`tickwire-transport`、`tickwire-venue`；第三方 `serde`、`sha2`、`rustls`、`mio`。

下游消费者：无（独立紧急二进制，不被其他 crate 引用）。

## 对应规格

- [../../specs/implementation/guardian.md](../../specs/implementation/guardian.md)（独立身份、授权白名单、审计 WAL、配额 Recovery 池、结果分类、audit_degraded 固定为真）

## 构建与测试

```sh
cargo build -p tickwire-guardian
cargo test -p tickwire-guardian
cargo clippy -p tickwire-guardian --all-targets -- -D warnings
cargo fmt -p tickwire-guardian
```

`src/tests.rs` 纯单元测试（增险命令拒绝、跨账户拒绝、签名/期限、撤销存量连接证据、请求计数、分块审计、同步失败禁续、配额身份不可重用、连接重试计费、时间回退）；fixture 复用 `tickwire-binance/tests/common/mod.rs`。真实独立进程/协议服务器/外部隔离设备回读属后续统一验证。

## 关键不变量与注意事项

- **动作白名单**：只准 Query、普通撤单、列表撤销、algo 撤销；其余 `CommandBody` 返回 `Denied`（`action`）。软件白名单不等于交易所提供 cancel-only API key。
- **授权双重路径**：`Emergency` 用 `Verifier::verify_emergency`（精确命令 scope、次数、期限）；`Fenced` 用 `verify_fence` 核对旧 exec 完整身份；隔离证明必须由可信外部控制器提供，本模块不执行交换机/电源/密钥撤销，也不把签名输入里的设备摘要当成本机已执行动作（`permission`、`guardian.md`）。
- **先审计后上网**：每条命令先保存规范字节、原始签名授权、请求摘要、连接代次、本地时间域，再允许应用数据进入 TLS（`submit` 的 `Event::Start` 在 connect 前落盘）。
- **结果边界**：`NoBytes`=TLS/socket 尚未接受请求字节；`Unknown`=部分接纳无完整回复或恢复后只有请求记录；`Response`=完整成功；`Rejected/NotFound/RateLimited/Authentication` 复用 Binance 分类并保存原响应。这些结果不减少账户预留、不授予新 exec、不声明账户安全。
- **配额只花 Recovery 池**：CLI 配置只接受 Recovery 类规则与授权；`start_quota_run` 在任何校时或连接前持久记录消费者身份，重启不能重新加载同一份额度（`AlreadyUsed`）。
- **时钟单调**：`ordered` 拒绝跨域/回退；时钟失败禁用新 IO，但已接收字节仍排空记录后停机；`audit_degraded` 固定为真（当前审计无复制确认）。
- 已有命令键恢复后只返回保存结果或 Unknown，不自动重发。

## 改动检查清单

1. `cargo build -p tickwire-guardian && cargo test -p tickwire-guardian` 通过。
2. 动作白名单/授权校验/结果分类联动 `guardian.md`；隔离验签联动 `tickwire-fencing`。
3. 审计事件格式改动同步 `audit.rs` 分块写入与 `recover` 重放校验。
4. 新增命令类型必须先过 `action` 白名单与 `Config::command` scope 校验。
5. 本 crate 不替代主执行路径，不做正常下单，不执行物理隔离动作。
