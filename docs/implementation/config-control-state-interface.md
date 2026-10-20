# 配置控制状态与复制对象接口

状态：待实现。本文细化[统一配置接口](configuration-interface.md)中的控制业务层；候选解析、投影与变更分类已有生产代码，计划器、控制服务和物化 agent 继续分别实现。

## 控制日志保存什么

三个 voter 保存相同的配置内容、计划、操作、授权版本、审计和幂等映射。`tickwire-control-raft::Applied` 只证明命令已持久提交；业务 reducer 再按提交顺序确定命令的结果。无效业务命令产生可重放的拒绝结果，保持原 desired 和操作状态。

完整候选和现有 Raft 命令的上限均为 16 MiB；候选达到上限时，额外元数据会使命令超限，因此采用有界复制对象：

1. `ObjectBegin` 固定对象身份、用途、发起主体、总长度、SHA-256、分片数和预算。
2. `ObjectChunk` 携带连续 offset 和最多 64 KiB 的原始字节。精确重复返回原结果；相同位置不同内容拒绝；跳片拒绝。
3. `ObjectSeal` 在全长和摘要相符后发布不可变对象。计划、提交和收据只引用已封口对象。

每条命令包含固定版本和严格长度，其完整编码至多 1 MiB；集合元数据超过该限额时也保存为独立对象。候选对象最多 16 MiB，投影 payload 最多 4 MiB，其他对象按用途分别限额。所有分片经实际 Raft 提交进入三个 voter 的完整日志，只有本机临时文件而没有复制内容的哈希引用不满足此接口。

对象表固定对象数、未完成对象数及累计字节上限。达到限额时返回 Capacity，保持已有内容及幂等历史。第一阶段保留完整历史；后续压缩按已提交引用图保留 desired、全部未决操作、历史回执和仍有效的幂等结果。对象重试不改变用途、身份、长度或内容。

## 计划与准入

拟新增 `tickwire-config-service`，先实现纯业务 reducer 和 Raft 驱动，再接 HTTPS：

```rust,ignore
struct ControlState;
struct AuthenticatedPrincipal; // 仅由实际认证入口产生
struct VerifiedDeployment;     // 实际部署资料和固定对象共同验证
struct PreparedPlan;           // 服务端规划结果，无提交权限
struct CommittedOperation;     // 仅由已应用控制记录产生

Planner::prepare(head, candidate, principal, deployment) -> Result<PreparedPlan>;
Service::create_plan(request, principal) -> PendingRequest;
Service::commit(request, principal) -> PendingRequest;
ControlState::apply(applied: &tickwire_control_raft::Applied) -> ApplyResult;
Service::read(barrier: ReadBarrier, principal, query) -> Result<View>;
```

`Planner` 固定 base ETag、完整候选和政策对象、服务端重新计算的 diff、mutation/dependency scope、全部 delivery targets、实际实例身份、旧 effective、依赖证据、动作和期限。参数 schema、发布对象、设备资源或当前部署资料缺失时，结果保留 pending checks，拒绝进入 Ready。`requested_target_ids` 约束请求意图；服务端计算的完整集合仍包括 NoOpRevisionAdvance、共享政策依赖和旧操作残留目标。

认证入口验证实际 mTLS 身份，将主体及证书身份交给服务；调用者不能在 JSON 中选择自己的角色。角色和账户域从当前已提交配置取得。计划准入、首次 commit、resume，以及幂等查询均使用当前授权。历史重放使用当时的角色版本和原准入事实，不让今天的撤权抹去已经发生的提交。

首次 commit 还要检查计划未过期、请求 candidate hash 与不可变计划一致、accepted actions 完整匹配，以及当前目标身份和资格仍满足计划。计划创建时的账户余额、磁盘空间和运行代次只是当时事实；发生变化时重新验证或拒绝，不只比较旧摘要。

写请求的处理顺序固定为：

1. 认证并按当前角色检查本次端点及已有结果的读取作用域。
2. 按 `(principal, endpoint, Idempotency-Key)` 查持久映射。规范请求摘要和 base If-Match 都相同时返回原结果，否则返回冲突。
3. 首次请求才检查当前 ETag、计划期限和实时资格。
4. 将完整准入绑定作为命令提案；提案尚未提交时只处于 Pending。
5. reducer 在实际提交顺序中重新检查当前 ETag、角色版本、计划和未决操作，再原子保存结果与幂等映射。

两个请求基于同一个 ETag 并发提交时，只有先应用且满足条件的请求推进 revision。后一请求产生持久冲突结果；丢失响应后的同键重试返回第一次确定结果。容量不足时先拒绝准入，不通过删除旧键或复用 plan/operation ID 腾出空间。

准入时间由受信时钟入口取得，并写入命令。期限判断在首次准入检查当前时间；确定性重放使用原准入时间。安全读等待 Raft 的 ReadIndex 和业务 reducer 的 applied 水位都达到屏障；业务层落后时继续等待，避免读取尚未重建的状态。

## desired 与操作完成

`ConfigCommitted` 在同一个业务转换中推进 desired、分配严格递增的新 revision、创建固定目标操作和幂等结果。它返回 202 所需的准入事实，操作初始状态为 Committed。进程尚未应用时，不产生 runtime/effective 收据。

同一时间只允许一个正在应用的操作。Stalled 的 resume 沿原 revision 继续，重新核对原计划身份和残留目标。新计划接管 Stalled 或仍有残留差异的 Failed 操作时，显式绑定 supersedes，并对所有实际 effective 差异重新规划；合法替代后旧操作保存 Failed 和后继 ID，历史收据保留。

逐目标收据由实际配置 agent 与运行时入口产生。共同验证器核对完整签名或认证来源、原 journal 事实、固定目标、operation、全局 revision/hash、projection hash/policy revision、boot、generation、slot、locator 和 applied event。公开 DTO 或非零摘要均不能构造 `VerifiedTargetReceipt`。

desired 文件持久化、运行时 ConfigApply、effective 文件持久化分别记录完成。运行时已经替换内存但 effective 写盘失败时，保存真实 runtime 版本并进入 Stalled。RollingRestart 另外保留旧实例退休、新实例绑定和 owner cutover；Retire 保存完整退休事实，退休进程仍保留在固定 target set 中。

只有固定集合全部目标的真实完成收据通过验证后，汇总器才能产生待提交的 `OperationSucceeded`。reducer 再核对集合及全部绑定并持久应用该命令，API 才返回 Succeeded。历史成功不随后来的进程崩溃改写，当前健康状态由 effective 视图报告。

## 分阶段测试

先以真实三节点文件 Raft 驱动验证对象分片、缺片/冲突/未封口拒绝、重启重建、预算耗尽，以及业务 applied 落后时安全读被阻止。随后验证同 ETag 并发、提交后丢响应、同键异内容、撤权后的重试与作用域过滤。

计划器测试使用实际固定候选、政策和部署对象，分别制造缺依赖、旧 boot、共享政策扩散与残留 effective 差异。操作测试从真实 agent/运行时收据进入共同验证器，覆盖漏目标、跨 operation 拼接、错投影、运行时已应用但文件失败，以及完成记录尚未多数提交的窗口。最后接 HTTPS 和实际文件物化流程；接口有结构定义但没有实际生产者的证据保持 Unsupported。
