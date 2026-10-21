# tickwire-fencing 接手说明

## 1. 职责（架构位置：执行权隔离 / fencing 协议公共验签）

来自独立可信隔离控制器的认证证明（attestation）的公共验签库。**验签只信任控制器的设备回读（device readback），绝不从超时、进程号或执行租约推导隔离**（lib.rs 首段）。两类文档：fence（旧执行实例被阻断/吊销的完整证明，`verify_fence`）与 emergency（监护人签名的窄范围应急取消/查询能力，`verify_emergency`）。纯验签，不持有私钥、不做隔离动作本身。

架构位置：`tickwire-guardian` 的公共验签底座；app、binance-execution、replica、lab 消费 `VerifiedFence`/`VerifiedEmergency` 作为执行权切换的证据。

## 2. 公共 API 入口

- 错误：`Error`（Invalid/Identity/Signature/Expired/Capacity）、`Result<T>`；`Hash=[u8;32]`。
- 身份与范围：`ExecutionIdentity`（host_id/boot_id/exec_instance_id/exec_epoch/network_identity/credential_access_identity）、`Scope`（risk_domain/venue/account/product/environment，`validate()`：risk_domain 非全零、venue=1、account≠0、product∈1..=3、environment∈1..=2）、`Validity`（clock_boot_id/clock_generation/issued_ns/expires_ns，`check(now)` 校验域与排他区间 `issued <= now < expires`）。
- 证明：`FenceClaims`（schema=1、scope、old_identity、controller_id、operation_id、control_commit、all_egress_hash、device_action_id、device_revision、deny_tombstone_hash、readback_hash、probe_hash、all_egress_blocked、existing_sessions_revoked、credential_access_revoked、validity）；`EmergencyClaims`（schema=1、scope、guardian_id、controller_id、grant_id、control_commit、precommitted_ns、manual_activation_allowed、instruments、actions、command_digests、max_requests、validity）；`Action`（Query/Cancel/CancelList/CancelAlgo）。
- 传输：`SignedDocument{payload, signature}`（`digest()` = SHA-256(`tickwire.signed-control.v1` + payload 长度 + payload + signature)）。
- `Verifier::new(controller_id, fence_key, emergency_key)`（两键必须不同且非全零）；`verify_fence(document, scope, old, now) -> VerifiedFence`；`verify_emergency(document, scope, guardian, now) -> VerifiedEmergency`。
- 结果：`VerifiedFence`（`evidence_digest/old_identity/risk_domain/scope/controller_id/validity/claims/check`）；`VerifiedEmergency`（`evidence_digest/claims/allows(instrument, action, command, used, now)`）。

## 3. workspace 位置

- 上游依赖：`tickwire-clock`（`MonoTime`/`ClockDomain`）、ring（ED25519）、serde、serde_json、sha2。
- 下游（grep `Cargo.toml` 核对）：`tickwire-app`、`tickwire-binance-execution`、`tickwire-guardian`、`tickwire-lab`、`tickwire-replica`。

## 4. 对应规格

**无专门实现文档。** 隔离证明的公共验签能力在 [../../specs/implementation/guardian.md](../../specs/implementation/guardian.md) 中声明位于本 crate（实现主体在 `tickwire-guardian`）；fence 与执行权切换的上下文另见 [../../specs/implementation/execution-core.md](../../specs/implementation/execution-core.md)。

## 5. 构建与测试

```sh
cargo test -p tickwire-fencing
```

- 模块内 2 个测试：`verifier_requires_distinct_keys_and_exact_authenticated_claims`（两键不同、签名精确、emergency allows 计数/动作边界、篡改 payload → Signature）；`fence_requires_complete_old_identity_and_revoked_existing_sessions`（旧身份逐字段绑定、过期 Expired、吊销标志缺失 Invalid）。

## 6. 关键不变量与注意事项

- **先认证后解析**：`authenticated()` 先用 ED25519 验签，通过后才 `serde_json::from_slice`；payload 空或 >65536、signature ≠64 字节返回 `Capacity`；篡改任一 payload 字节返回 `Signature`。
- **fencing token / 代次语义**：`verify_fence` 要求 claims.scope 与传入 scope 全等、`old_identity` 与传入旧实例全等（含 exec_epoch 代次）、controller_id 匹配；旧身份任一字段（host_id/boot_id/exec_instance_id/exec_epoch/network_identity/credential_access_identity）为零即 `Invalid`。
- **三项吊销标志必须全真**：`all_egress_blocked`、`existing_sessions_revoked`、`credential_access_revoked` 任一为 false 拒绝；`all_egress_hash`/`deny_tombstone_hash`/`readback_hash`/`probe_hash` 非全零，control_commit/device_revision 非零。
- **Validity 域绑定与排他区间**：`check(now)` 要求 `now.domain()` 的 boot_id/generation 与证明一致，且 `issued_ns <= now.nanos() < expires_ns`（过期排他）；`issued >= expires` 本身 `Invalid`。
- **emergency 窄授权**：`manual_activation_allowed=true`（显式操作员触发是允许的触发，不是 fence 证明）；max_requests ∈1..=4096；instruments 非空 ≤4096、不含 0、区间严格递增；actions 非空 ≤4 且无重复；command_digests ≤1024 且不含全零（空列表表示允许签名动作/工具范围内任意命令）。
- **allows 运行时复核**：`VerifiedEmergency::allows` 每次重新 `validity.check(now)`，校验 used < max_requests、instrument 在列、action 在列、（非空时）command digest 在列，任一不符 `Identity`。
- **schema 固定 1**：claims.schema != 1 拒绝；`#[serde(deny_unknown_fields)]` 拒绝未知字段。
- 本 crate 不签发证明、不执行隔离；隔离动作与私钥在 guardian/控制器侧。

## 7. 改动检查清单

1. `cargo test -p tickwire-fencing` 全绿。
2. 改 claims 字段时保持 `deny_unknown_fields` 与 schema=1 校验，同步 guardian.md 与 `tickwire-guardian` 的签发侧。
3. 新增 Action/范围维度必须在 `allows` 加逐项成员校验并补测试。
4. 不要在此引入超时/进程号/租约作为隔离依据；那会违反 lib.rs 首段原则。
5. 验签算法固定 ED25519；换算法需同时改 Verifier 构造与签发侧密钥格式。
