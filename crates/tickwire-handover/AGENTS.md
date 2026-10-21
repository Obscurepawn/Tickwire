# tickwire-handover

## 1. 职责（架构位置：策略进程侧 → 账户风险域的桥接校验）

统一校验策略交接中保留的账户回调（TWINP 输入对象、账户基线、封口摘要），只核对恢复数据本身，不校验策略授权、不执行回调。lib 根文档：`Shared verification of the account callbacks retained in a strategy handover. This checks recovery data, not strategy authorization or callback execution.`

## 2. 公共 API 入口

- `verify(seal: &HandoverSeal, checkpoint: &[u8], input_bytes: &[u8], account_input_bytes: &[u8]) -> Result<VerifiedHandoverInputs, Error>`（`src/lib.rs`）。
- `VerifiedHandoverInputs`：`baseline()`、`records()`、`matches_history(baseline, prior)`（把已校验回调与独立读到的实际 WAL 逐字节比对，不符返回 `Error::History`）。
- `Error`：`Bounds/Checkpoint/Inputs/Callback/Baseline/Sequence/AccountSeal/Replay/State/History`。
- 常量 `MAX_ACCOUNT_RECORDS = tickwire_account::MAX_FACTS + tickwire_account::MAX_ORDERS`。
- 无 bin、无 examples、无独立 tests/ 目录（测试在 `src/tests.rs`）。

## 3. workspace 位置

上游（`Cargo.toml` 实际声明）：`tickwire-account`、`tickwire-account-codec`、`tickwire-recovery`；dev：`tickwire-risk`、`tickwire-venue`。

下游（grep 命中）：`tickwire-release`、`tickwire-runtime`、`tickwire-replica`、`tickwire-strategy-sdk`、`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/handover-inputs.md](../../specs/implementation/handover-inputs.md)（TWINP001 保存真实 callback 原文；checkpoint/输入/账户封口长度与摘要核对；kind-2 行严格 `TWAI0001`；基线恰好一次且在记录前；恢复出的 ledger 与策略订单摘要必须等于交接声明）

## 5. 构建与测试

```sh
cargo build -p tickwire-handover
cargo test -p tickwire-handover
```

无 bin/example。`src/tests.rs` 构造 seal/checkpoint/inputs/account_input 捆绑，覆盖摘要不符、缺基线、序号缺口、封口原像不符、恢复状态（ledger/订单摘要）不符等拒绝路径。

## 6. 关键不变量与注意事项

- **先核 seal 长度/摘要**：`verify` 先校验 `seal.validate()`、`report_position <= MAX_ACCOUNT_RECORDS`、两段字节长度等于 seal 声明且 SHA-256 等于 `input_digest/account_input_digest`，再 `seal.verify_checkpoint(checkpoint)`。
- **输入链从 1 开始、零前置摘要**：`EncodedInputs::decode` 后必须 `boundary.first_sequence == 1` 且 `previous_hash == [0;32]`。
- **kind 2 严格解析**：wrapper 以 `TWAI0001` 开头，tag 0=基线（position 0、仅一次）、tag 1=记录（`position == records.len()+1 == record.sequence()`），其余 tag/位置返回 `Callback`。
- **封口逐步累计**：`InputSeal::new(baseline.digest())` 后逐条 `apply(record)`，要求记录绑定同一基线且序号连续；最终 `account_seal.digest() == seal.account_input_digest` 且 `object_bytes() == account_input_bytes`。
- **重放核对状态**：用 `Account::recover(baseline.snapshot, baseline.catalog, records)` 重建账户，核对 `ledger_sequence()` 与 `strategy_orders_digest(seal.logical_strategy)` 等于 seal 声明，不符返回 `State`。
- **构造私有**：`VerifiedHandoverInputs` 只能由 `verify` 成功构造；它只携带数据，不授予交易权限。

## 7. 改动检查清单

1. `cargo build -p tickwire-handover && cargo test -p tickwire-handover` 通过。
2. 校验规则改动联动 [handover-inputs.md](../../specs/implementation/handover-inputs.md) 与 `tickwire-recovery` 的 `HandoverSeal`/`EncodedInputs` 编码。
3. 新增账户记录类型时同步 `tickwire-account` 的 `RecoveryRecord` 与 `tickwire-account-codec` 规范解码。
4. 本 crate 不得做授权决策、网络访问或回调执行。
