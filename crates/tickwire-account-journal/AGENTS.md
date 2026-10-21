# tickwire-account-journal

## 1. 职责（架构位置：账户风险域 —— 有序账户持久化）

按原顺序保存八类（及后续扩展的）账户恢复记录的有序持久化层；收据只是持久化证据，不是交易授权。固定物理风险域/stream/分段配置、账户 Scope、初始 writer 与完整基线摘要。lib 根文档：`Ordered account persistence; receipts are durability evidence, not trading authority.`

## 2. 公共 API 入口

- `AccountJournal<S: Backend>`：`create(backend, config, baseline)`、`recover(...)`、`append(record: RecoveryRecord) -> AppendReceipt`、`append_reactivation(binding)`、`position()`、`writer_epoch()`、`poisoned()`、`recovered()`、`records()`、`ingress_event(stream,seq)`、`ingress_events()`、`ingress_record(stream,seq)`、`ingress_account_inputs()`、`baseline()`、`config()`、`admission_origin(key)`、`history_frontier()`。
- `FileBackend`（`backend.rs`，受信私有目录，物理 store 单写锁覆盖全部文件）、`HistoryFrontier`（`history.rs`：`scope/stream/baseline_digest/record_count/chain_hash/bytes/digest`）、`Config{physical: tickwire_journal::Config, baseline_digest, initial_writer_epoch, scope}`、`AppendReceipt{baseline_digest,sequence,record_digest,physical_frontier}`、`AdmissionOrigin{receipt,history}`、`Recovered<S>`、`RecoveryVerifier` trait（`verify_admission` 必选；`verify_uncreated_algo/income_association/account_inputs_closure/income_baseline/verify_handover/verify_reactivation` 默认 `Invalid`）。
- `bootstrap_bytes(config) -> Vec<u8>`（日志创建前发布的不可变引导字节）；常量 `MAX_RECORDS = MAX_FACTS + MAX_ORDERS`。
- 私有模块：`backend`、`history`、`identity`。无 bin/example。

## 3. workspace 位置

上游（`Cargo.toml` 实际声明）：`tickwire-account`、`tickwire-account-codec`、`tickwire-journal`、`tickwire-venue`；dev：`tickwire-risk`、`tickwire-recovery`。

下游（grep 命中）：`tickwire-account-gateway`、`tickwire-binance-account`、`tickwire-binance-execution`、`tickwire-runtime`、`tickwire-replica`、`tickwire-app`、`tickwire-lab`。

## 4. 对应规格

- [../../specs/implementation/account-journal.md](../../specs/implementation/account-journal.md)（固定绑定、有序追加、先同步基线/bootstrap/物理日志、同步成功后才更新公开列表、物理错误 poisoned）

## 5. 构建与测试

```sh
cargo build -p tickwire-account-journal
cargo test -p tickwire-account-journal
```

无 bin/example。`tests/`：`persistence.rs`（创建/恢复/追加/重复/冲突）、`ingress.rs`（输入流序号去重）、`income_closure.rs`（收入闭合记录）；公共 fixture 在 `tests/common/`。

## 6. 关键不变量与注意事项

- **收据不授权**：`AppendReceipt` 只证明记录被某个同步前缀覆盖，不授予发送或交易权限（lib 根注释）。
- **创建顺序**：先同步基线，再同步确定性编码的 bootstrap，最后建立物理日志；既有或部分账户文件不覆盖（`create`）。
- **序号连续**：成功记录的 `CanonicalRecord.sequence` 从 1 连续递增，等于物理记录序号，**不等于账户状态变更次数**；Admit 精确重试返回原收据，同键不同输入封闭日志，其它完整输入保留原顺序。
- **写入前校验**：基线摘要、账户身份、目录身份、代次在写入前核对；新 Admit 只属于当前 writer，事实/中止可引用旧意图代次。
- **同步成功才发布**：同步成功后才更新公开记录列表、成功序号与历史前缀；物理追加错误使封装 poisoned（含可能已完整写入的批次），必须重开日志核对介质，不能沿未确认结果继续追加。
- 记录总量受 `MAX_FACTS + MAX_ORDERS` 限制；当前只用本地文件同步，镜像提交证明与新交易授权不由本模块产生。

## 7. 改动检查清单

1. `cargo build -p tickwire-account-journal && cargo test -p tickwire-account-journal` 通过。
2. 持久化规则改动联动 [account-journal.md](../../specs/implementation/account-journal.md)；记录编码联动 `tickwire-account-codec`，物理帧联动 `tickwire-journal`。
3. 恢复核对逻辑核对 `tla/execution_models/account_journal/L0_ordered_recovery`。
4. 本 crate 不做授权、网络或账户经济计算。
