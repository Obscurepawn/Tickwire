# tickwire-quota 接手说明

## 1. 职责（架构位置：共享限流）

supervisor 分配与单所有者本地请求配额消耗：窗口（WindowConfig）、消费者租约（ConsumerId/ExecLimiter）、发放（Grant/PreparedGrant）、本地消耗（consume/AttemptId 去重）、停止归还（StoppedReceipt/reclaim）与持久 CAS 配额存储（`files::FileQuotaStore`）。协调器与限流器都是单 owner 串行 reducer；持久状态经 `QuotaStore::compare_exchange` 比较交换提交。

架构位置：限额/限流决策层，处在 supervisor 分配与实际请求发送之间；`tickwire-binance-quota` 在其上做类型化请求分类，`tickwire-md`/guardian/执行适配器消费本地额度。

## 2. 公共 API 入口

- 标识：`ScopeKind`（Ip/Uid/Account/Connection）、`BucketId`（namespace/scope/subject/pool）、`WindowKey`（bucket+window）、`Class`（Normal/Recovery）、`Cost`（window+units）、`WindowConfig`（key/revision/starts_ns/ends_ns/limit/recovery_reserve）、`ConsumerId`（instance+generation）、`AttemptId`（producer+sequence）、`Consumed`（New/Duplicate）。
- 协调器 `Coordinator<S: QuotaStore>`：`open`、`prepare_windows`/`commit_windows`/`add_window`、`register_consumer`、`prepare_renew_consumer`/`commit_renew_consumer`/`renew_consumer`、`prepare_issue`/`commit_issue`/`issue`、`reclaim`、`accounted`、`into_store`。
- 限流器 `ExecLimiter`：`install`、`renew`、`consume`、`stop`（产 `StoppedReceipt`）、`remaining`、`cooldown`、`freeze`/`is_frozen`、`identity`、`clock_domain`。
- 凭证：`Grant`（`id()`/`costs()`，无 Clone/不可反序列化）、`PreparedGrant`/`PreparedWindows`/`PreparedRenewal`（验证过的计划，本身不产生消费能力）。
- 存储：trait `QuotaStore`（`load`/`compare_exchange`）、`MockQuotaStore`（`fail_next(MockFailure::BeforeCommit|AfterCommit)`）、`files::FileQuotaStore`。
- 上限常量：`MAX_WINDOWS=256`、`MAX_CONSUMERS=256`、`MAX_GRANTS=4096`、`MAX_COSTS=16`、`MAX_ATTEMPTS=4096`。

## 3. workspace 位置

- 上游依赖：`tickwire-clock`（复用 `ClockDomain`/`MonoTime`）、serde、serde_json、sha2、libc。
- 下游（grep `Cargo.toml` 核对）：`tickwire-app`、`tickwire-binance-execution`、`tickwire-binance-quota`、`tickwire-guardian`、`tickwire-md`。

## 4. 对应规格

- [../../specs/implementation/quota.md](../../specs/implementation/quota.md) — 共享请求额度规格（桶/窗口/发放/比较交换/回执丢失语义）。

## 5. 构建与测试

```sh
cargo test -p tickwire-quota
```

- `tests/quota.rs`：10 个测试（多桶共享分配与本地消耗原子、精确重试复查过期且不退款、恢复容量与停止收据不重发、重开保留未确认分配且不重铸 owner、提交失败 poison 与恢复保留已发容量、CAS 防陈旧协调器且活 owner 共享上限等）。
- 模块内 `preparation_tests`：2 个测试（准备窗口原子/陈旧 ticket 不可提交；分配与续约只从已提交 ticket 发布）。

## 6. 关键不变量与注意事项

- **两段式 prepare/commit**：`prepare_*` 只在内存构造候选 `Prepared*`，`accounted()`/`remaining()` 在 commit 前看不到占用；只有 `commit_*` 成功持久写入才产生 Grant 或改变状态。陈旧 ticket（revision/supervisor_generation/domain 任一不符）commit 返回 `Conflict`。
- **CAS 失败即 poison**：`commit` 中 `compare_exchange` 失败后 `poisoned=true`，该协调器此后所有写操作返回 `Poisoned`，不得重试自愈；调用方应重新 `open`。
- **open 准入**：要求新 supervisor_generation 严格大于已存、`ClockDomain` 一致、`now.nanos()` 不回退；否则 `Identity`/`Domain`/`ClockRegression`。
- **窗口不可变、不重叠**：同一 `BucketId` 的后续 window 编号必须更大且区间不与已有窗口重叠；已存在且完全相同的窗口可跳过，不同则 `Conflict`；`recovery_reserve <= limit`。
- **Normal/Recovery 分账**：Normal 类发放受 `(limit - recovery_reserve)` 扣减已占 Normal 后约束；Recovery 类只扣总上限。余额不足 `Exhausted`。
- **request_id 唯一**：同一 request_id 不得再次产生 Grant（`AlreadyIssued`）；grant 过期时间必须落在窗口内且不超过 owner 租约。
- **consume 幂等去重**：同 `AttemptId` 重复且 digest/class/costs 完全一致返回 `Duplicate` 且不重复扣减；任一字段冲突立即 `freeze` 并 `Conflict`；cooldown 期内拒绝；窗口或 grant 已过期返回 `Expired`。
- **停止归还不补退**：`stop()` 汇总各 grant 已花/剩余，`reclaim` 按收据归还未用额度并关闭 owner；已花单位不退、不重铸。
- **回执丢失不重复发放**：commit 已落盘但应答丢失时，重新 `open` 仍能读到全额已占容量（`MockFailure::AfterCommit` 模拟）。
- **文件存储安全**：`FileQuotaStore` 要求目录 0700、LOCK/STATE 0600、`O_NOFOLLOW|O_CLOEXEC`、独占 try_lock，STATE 超过 16MiB 拒绝。

## 7. 改动检查清单

1. `cargo test -p tickwire-quota` 全绿。
2. 改 prepare/commit 拆分或 CAS 语义时，对照 quota.md 的"只有成功返回的持久写入才能产生 Grant"逐条核对。
3. 改窗口/租约/发放结构（serde）时注意 `files::FileQuotaStore` 的 STATE 解码兼容与容量上限。
4. 新增 Class 或桶维度时，同步 `accounted()` 的过滤口径与 `tickwire-binance-quota` 的请求分类。
5. 时钟相关改动联动 `tickwire-clock`；domain 变化/回退必须走 poison/冻结路径，不得静默换域。
