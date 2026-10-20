# JOIN owner 实现规格

实现入口为 `tickwire-join`；接口及所有权边界见 [JOIN 接口](../../docs/implementation/join-interface.md)，源码为 [owner](../../crates/tickwire-join/src/lib.rs) 与 [consumer](../../crates/tickwire-join/src/mapped.rs)。

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

累计历史登记数不超过 M，已 detach 条目不自动回收。错误身份与期限不通过重试变成新授权。所有者使用同步 `&mut self` 串行域，消费者可在独立进程推进 cursor；unsafe 仅位于私有 mapped 模块的已有 SDK 合同接缝。

真实 Red 先取得可编译的失败测试，再实现对应行为。SDK 6 项失败测试、owner 2 项失败测试、flow 2 项失败测试均有原始日志；前两类还包括在 stub 阶段已通过的拒绝测试，具体数量见 [验证记录](../../docs/validation/join/README.md)。实际 Linux 测试包含三进程认证、SCM_RIGHTS、immutable snapshot、pending 满门禁、追平激活及持有 guard 时无法退出。TLA 只证明列出的有限状态范围，见 [模型计划](../../tla/execution_models/join_owner/PLAN.md)。

保持待实现：持久 owner/RequestStore 恢复、代次重用、Supervisor 常驻分发、真实业务状态源、死亡回收和最终 exec 增险授权。超时与 socket EOF 都没有 detach 权限。
