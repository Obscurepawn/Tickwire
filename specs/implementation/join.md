# JOIN owner 实现规格

实现入口为 `tickwire-join`；接口及所有权边界见 JOIN 接口，源码为 [owner](../../crates/tickwire-join/src/lib.rs) 与 [consumer](../../crates/tickwire-join/src/mapped.rs)。

| 操作 | 成功条件与结果 | 失败后的状态 |
| --- | --- | --- |
| prepare | 已认证 Supervisor、完整可信订阅绑定、有效期限；先实际 Publisher.join 保留 S，再构造、封印并保存响应 | 构造/封印/结果存储失败保留 pending gate；重复已登记请求不重新构造 |
| deliver | 独立 subscriber 会话匹配完整登记身份；bank 与 snapshot 分别交付 0x17/0x0f 的只读能力 | 已发生的登记保留，发送失败进入 Failed |
| receive_cursor | 唯一可信 SDK 写者、完整 descriptor 与订阅身份匹配 | 不登记替代映射，不提前采纳 released |
| read_ready | 两个独立资源的 offer/header/length/seals/access/generation 和原 membership V 全部匹配 | 对应确认位保持原值 |
| join_ready | 双确认和实际 cursor 已登记；快照完整且具备必需状态；applied 已追平同次调用读取的 published | 保持 pending；错误正文不覆盖原 JoinBody |
| ready retry | 原正文与原 membership V，认证会话有效 | 返回首次激活水位，不更新其他订阅 |
| expire | JOIN 到期或 owner 观察到时间倒退 | pending 进入 Expired，gate 保留 |
| quiesced | 实际登记 cursor 生命周期 5、quiesced_seq 与最终水位精确一致，且无活借用 | 无法伪造控制正文移除门禁；成功后完整 ACK 幂等 |
| all_detached | 无登记或所有历史登记均为 Detached 返回 true；只读查询 | Reserved/Prepared/Delivered/Active/Failed/Expired/Quiescing 均返回 false，不因超时或断线推断退出 |

累计历史登记数不超过 M，已 detach 条目不自动回收。错误身份与期限不通过重试变成新授权。所有者使用同步 `&mut self` 串行域，消费者可在独立进程推进 cursor；unsafe 仅位于私有 mapped 模块的已有 SDK 合同接缝。

真实 Red 先取得可编译的失败测试，再实现对应行为。SDK 6 项失败测试、owner 2 项失败测试、flow 2 项失败测试均有原始日志；前两类还包括在 stub 阶段已通过的拒绝测试，具体数量见 验证记录。实际 Linux 测试包含三进程认证、SCM_RIGHTS、immutable snapshot、pending 满门禁、追平激活及持有 guard 时无法退出。TLA 只证明列出的有限状态范围，见 [模型计划](../../tla/execution_models/join_owner/PLAN.md)。

保持待实现：持久 owner/RequestStore 恢复、代次重用、Supervisor 常驻分发、真实业务状态源、死亡回收和最终 exec 增险授权。超时与 socket EOF 都没有 detach 权限。

## 消费者非阻塞接入

`tickwire-market-source::PendingJoin::new(config, now)` 沿用 `Client::connect` 的可信 qualified MD writer 安全契约，固定批准 bank/catalog/instrument 集合、OS peer 身份、时钟域与总启动期限。`poll(now, budget)` 分步完成建连、HELLO/WELCOME、JOIN 响应、bank/snapshot offers、cursor offer、两个 READ_READY、追平与 JOIN_READY/STATUS；每次最多执行 min(budget,64) 个协议步骤或行情记录，预算为零拒绝。它仅使用非阻塞 connect/send/recv，不等待、不 sleep；EINTR 作为本次未推进返回，保留原状态与待发整包。

`JoinProgress` 返回实际 work/records、是否仍可推进、ready 与下一期限。私有 pending 状态不提供可运行 Client；只有认证 Active STATUS 绑定完整 JOIN 且实际 applied 不小于其 published 水位，才可通过 `take_client()` 取出一次完成态。CatchUp STATUS 目标不可倒退，追到目标后重发 JOIN_READY；等待响应时继续按预算追平。每条 catchup 输入先保留完整 record/payload 和本次 now，再应用副本、推进 cursor；累计最多 bank.capacity 条、64 MiB。发送受背压时保留原编码与 FD，成功后才推进阶段。

所有 poll 校验同一时钟域、时间不倒退、总启动期限及会话租约。错误、显式 cancel 或 Drop 关闭本地资源并停止接入，不能返回半完成 Client，也不产生 DETACH/QUIESCED 证明；owner 已安装的 pending gate 继续按原故障规则保留。`Client::connect` 保持原签名，作为兼容阻塞入口驱动同一状态机，只有该 wrapper 可 sleep，并保留绝对 Instant 截止点。

预算约束的是有限协议步骤和记录数，不是硬实时耗时：完整不可变快照的映射、验证及副本装载仍是一个冷步骤，受既有 64 MiB、4096 成员和 maximum_levels 上限约束。真实回归使用 authenticated SEQPACKET、JoinOwner、memfd、实际 cursor 与快照，验证逐步暂停、追平/激活、身份变异、EOF、超时和取消；无需引入 MD dev 依赖回边。
