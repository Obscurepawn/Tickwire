# 多域发布协调器接口

`tickwire-lifecycle` 提供独立协调器、Ed25519 签名控制请求与成员实际回执。每个执行域先关闭新增风险，再持久移交管理权；协调器收齐所有成员的 `Committed` 回执后，才签发 `GroupRelease`。成员应用自己的门禁后返回 `Released`，全部成员确认后进入 `ActiveAll`。

执行端仍负责核对 Host 的实际 READY、检查点、输入封印、账户账本与订单摘要，以及当前 `risk_gate_revision`。协调器发出文件或执行端收到文件，都不构成成员已应用。

## 公共接口

`Operation` 固定操作 ID、revision、策略 ID、发布文件摘要、检查点摘要、回滚政策摘要及有序成员清单。`Member` 固定风险域、exec 身份与代次、新旧策略实例与代次，并各自保存 `clock_boot_id / clock_generation / valid_until_ns`。

`ControlRequest` 绑定完整 operation、目标 member、唯一 request ID、动作与可选证明：

| 动作 | 输入 | 实际回执 |
|---|---|---|
| Prepare | `Readiness` | 执行端核验实际状态后的 Prepared |
| Commit | 当前 Readiness | 账户 WAL 同步后产生的 Committed |
| Release | `SignedGroupRelease` | 执行端比较门禁版本并应用后的 Released |
| QueryApplied | 当前请求挑战 | 执行端当前实际阶段 |
| Halt | 精确操作和成员身份 | 关闭并持久化后的 Halted |

`Readiness` 包含 `checkpoint_digest`、`exec_report_sequence`、`orders_digest`、`ledger_sequence` 和 `input_seal_digest`。这些字段描述受信输入；实际核验在执行端完成。

```rust,ignore
Coordinator::create(store, config, writer_epoch, signer, now)
Coordinator::recover(store, config, writer_epoch, signer, required_frontier, now)
owner.pending_requests()
owner.ingest(signed_receipt, expected_version, now)
owner.begin_commit(expected_version, now)
owner.release(expected_version, now)
owner.halt(expected_version, now)
owner.query_applied(expected_version, now)
owner.resume_after_queries(expected_version, now)
owner.update_readiness(member, new_proof, expected_version, now)
```

所有状态更新先检查版本，生成候选状态，完成日志同步后才对外公布。精确重复回执返回首次保存的结果，不再次推进版本；调用者查看 `stage()` 获取当前阶段。同步结果不确定时，owner 进入 Frozen，隐藏待发请求和组凭据。

readiness 过时的成员只要尚未 Committed，就可以更新证明并重新 Prepare。其他成员已经提交的管理权保留；协调器重新收齐所需 Prepared 后，只向尚未提交的成员发 Commit。同一操作中的非 Halted 回执若改变了风险门禁版本，协调器发出 Halt，不能用刷新后的门禁替旧移交重新授权。

## 时钟与签名

协调器的 `Operation` 时钟只约束本地操作期限。远端成员按自己的 `Member` 时钟核验请求及 Release；collector 不比较远端 `issued_ns` 与本地时间，而是校验固定 exec 公钥、完整成员身份和当前请求 ID。

签名覆盖协议域前缀与确定 JSON 字节。接收端同时拒绝未知字段和重新编码后不同的 payload。`verify_control_request` 与 `verify_group_release_for_member` 是多主机接收入口。`verify_group_release` 供协调器时钟域内使用。

Halt 只关闭精确身份对应的操作，因此期限到达后仍可认证并应用；Halted 回执可在期限之后收集。该例外不适用于 Prepare、Commit 或 Release。

## 持久化与恢复

`EventJournal<S>` 复用物理 `Journal`：逻辑记录按 8192 字节分块，每块同步，最后同步长度及 SHA-256 提交记录。恢复只返回具有完整提交记录的逻辑事件；完整物理尾部中尚未提交的逻辑分块不会变成事件。单事件最多 4 MiB，累计帧 payload 最多 128 MiB、最多 4096 个事件。`frontier()` 可供外层保存要求恢复包含的物理前缀。

协调器首先保存完整配置和固定公钥。恢复必须匹配该配置，并重新验证所有状态转换和签名记录。恢复完成后立即持久发出新的 QueryApplied；历史 Released 不自动成为当前授权。收齐本轮实际回复后仍需调用 `resume_after_queries`。任何成员 Halted 都拒绝恢复放行；后续发布采用新的 operation revision 和有效身份。

部分成员已经 Released 时，显式继续后只向尚未 Released 的成员发 Release。组凭据仍包含全部成员，并保留已放行成员原来的 Committed 签名回执摘要；Released 查询回执只证明当前阶段，不能充当 Committed 回执。缺少任一成员的本轮查询、门禁版本改变或出现 Halted，均不能继续 Release。

## 可运行入口

```sh
cargo run -p tickwire-lifecycle -- --config /absolute/coordinator.json
cargo run -p tickwire-lifecycle -- halt --config /absolute/coordinator.json
```

配置使用 `cli::Deployment`：协调器配置、独立签名 PKCS#8 文件、writer 代次、恢复开关、是否在完整查询后继续、WAL 目录、回执目录、请求目录、可选 readiness 更新目录及有界等待参数。目录必须为受信绝对路径，已有目录拒绝组和其他用户访问；签名文件也执行权限限制。

每个执行域依次消费：

```text
requests_directory/<risk_domain_hex>/<dispatch_version:020>-<request_id_hex>.json
```

请求先写临时文件并同步，再通过不覆盖已有文件的方式发布。已有同名同字节文件可重试；同名不同内容拒绝。执行端将 `SignedReceipt` JSON 放入回执目录。非 Applied 拒绝结果不会被计作成功，也不会阻挡后面的回执。

readiness 更新文件使用 `cli::ReadinessUpdate`，绑定 operation ID、revision、预期协调器版本、成员与新证明。版本已过去的更新不再应用。等待超时会先持久发 Halt，再报告尚未收齐回执；没有收到全部 Halted 时，状态保持 Halting。

本模块实现 Prepared、Committed、Released、Halted 的协调。旧进程退出后的 Finalized、资金补偿及反向管理权移交仍由对应业务操作负责，不能从 GroupRelease 推导这些结果。
