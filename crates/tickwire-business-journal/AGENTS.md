# tickwire-business-journal

## 1. 职责（架构位置：持久化恢复链 —— 业务 WAL + 默认冻结的恢复运行时）

把 `tickwire-journal-codec` 的确定性业务事件 CBOR 接到真实物理 WAL（`tickwire-journal`），并提供默认冻结的恢复运行时 `Runtime`，把授权、WAL 同步、执行内核 reducer 与受信 `Exchange` 串成一条提交链。lib 根文档：`Ordered, synchronized business WAL and a frozen-by-default recovery runtime.`

## 2. 公共 API 入口

- `BusinessJournal<S: Backend>`：`create(backend, config, writer_exec_epoch)`、`recover(backend, expected, writer_exec_epoch, required)`、`commit_intent(record: DurableRecord, closure: &VerifiedClosure) -> WalAppendReceipt`、`append_fact(event: codec::Event)`、`events()`、`position()`、`logical_hash()`、`config()`。
- 自由函数：`intent_record(config, writer_exec_epoch, seq, record)`（供受信授权方取闭包存储）、`closure_binding(record) -> ClosureBinding`。
- `BusinessConfig{physical: tickwire_journal::Config, domain: Domain, initial_capital}`；`WalAppendReceipt{journal_seq,logical_hash,physical_frontier}`；`AuthorizationError::{Unavailable,Denied,Recovery}`；`AuthorizationRequest{binding,record,intent_payload}`；`CommitAuthorizer` trait（`current_gate/authorize/verify_recovery`）。
- `Runtime<S, A: CommitAuthorizer, E: Exchange, N, T>`：`create(backend, config, gate, authorizer, exchange)`、`recover(...)`、`submit(request)`、`resume_submit(intent_id)`、`record_fact(identity, fact)`、`record_fill(identity, fill)`、`order(id)`、`view() -> RuntimeView`、`exchange()`、`update_gate(gate)`；`RuntimeView{frozen,held,spent,available,journal_seq}`。
- `Backend` trait（`read_bootstrap/publish_bootstrap`，扩展 `journal::store::Store`）与 `FileBackend`。
- 私有模块：`backend`、`runtime`。无 bin/example。

## 3. workspace 位置

上游（`Cargo.toml` 实际声明）：`tickwire-execution`、`tickwire-journal`、`tickwire-journal-codec`、`tickwire-recovery`；第三方 `sha2/serde/serde_json`。

下游（grep 命中）：`tickwire-gateway`。

## 4. 对应规格

- [../../specs/implementation/business-journal.md](../../specs/implementation/business-journal.md)（完成点、意图与事实、NoBytesAccepted 先同步 AbortedBeforeSend 再释放）
- [../../specs/implementation/command-binding.md](../../specs/implementation/command-binding.md)（命令绑定 schema2 与信封身份）
- 背景：[../../specs/implementation/mock-abort.md](../../specs/implementation/mock-abort.md)（确定性替身与中止恢复）

## 5. 构建与测试

```sh
cargo build -p tickwire-business-journal
cargo test -p tickwire-business-journal
```

无 bin/example。`tests/runtime.rs` 覆盖提交授权、重试幂等、事实先同步后应用、NoBytesAccepted 中止、冲突冻结与恢复重放。测试入口通过 `FORK_BARRIER` 同步：普通文件用例持共享 guard，创建子进程的用例持独占 guard，避免 fork 到 exec 之间继承其他并行测试的 WAL 锁描述符；生产文件锁行为不变。新增文件或子进程测试须沿用该约束。

## 6. 关键不变量与注意事项

- **完成点才出收据**：`commit_intent` 只有在真实物理批 `append_batch` 同步成功后返回 `WalAppendReceipt`；收据不代表策略授权、复制确认或完整 LocalCommitProof。
- **先授权/恢复依赖，再同步意图**：`Runtime::resume_after_admission` 顺序为 `authorize` → `journal.commit_intent`（WAL sync）→ `refresh_gate` → `accept_commit` → `begin_send` → `exchange.submit`；`verify_recovery` 缺恢复依赖必须失败，绝不重新创建依赖。
- **发送前时效刷新**：`submit/resume_submit` 先 `refresh_gate()`，提交成功后发送前再 `refresh_gate()`，使用新鲜受信时间/fencing。
- **重试幂等**：相同完整 `DurableRecord` 返回原业务位置与逻辑哈希（即使重开后 writer 代次已提高，物理前缀可更晚）；同一 intent_id/intent_seq 绑定不同内容 → 拒绝并 poison。
- **未发送中止先保存后释放**：`SubmitOutcome::NoBytesAccepted` 时先 `journal.append_fact(AbortedBeforeSend)` 同步，再 `apply_fact` 让 reducer 释放预留；写入/同步/应用失败都冻结，失败前不提前释放；外部 `record_fact` 提交 AbortedBeforeSend 会被拒绝。
- **事实先同步再交 reducer**：Fill/Fee/Terminal 先 `append_fact` 同步，即使冲突/算术失败也保留已接收事实并冻结；未知/错配身份阻止后续恢复完成。
- **恢复隔离**：`recover` 核对 bootstrap 字节 == 预期规范编码、物理身份/配置、writer 代次不回滚；物理/business 记录身份（journal_seq、writer_exec_epoch）逐一核对，重复 Prepared 身份拒绝；仅接受 `LocalDurable`。
- 逻辑链从 genesis 全量重算，无公共 reset/跳前缀接口。

## 7. 改动检查清单

1. `cargo build -p tickwire-business-journal && cargo test -p tickwire-business-journal` 通过。
2. 提交/中止/恢复联动 [business-journal.md](../../specs/implementation/business-journal.md)；信封身份联动 [command-binding.md](../../specs/implementation/command-binding.md)。
3. 形式模型：`tla/execution_models/business_journal/`（L0_commit_abort_recovery、L1_command_binding）。
4. 事件格式改动同步 `tickwire-journal-codec`；物理批/分段改动同步 `tickwire-journal`。
5. 本 crate 不自行授权，授权永远来自注入的 `CommitAuthorizer`。
