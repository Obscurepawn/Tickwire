# TopicSnapshotManifest 接收规格

Status: implemented receiver validation; builder and JOIN coordinator pending

`tickwire-snapshot` 接收完整的 NormalizedMarket TopicSnapshotManifest，不修改文件，也不推进 ring/cursor。资源完整性检查与订阅上下文校验全部成功后，返回借用原始不可变字节的 `ValidatedTopic`。接口及 owner 后续接线边界见[接口设计](../../docs/implementation/topic-snapshot-interface.md)。

## 已实现行为

1. 输入总量最多 64 MiB，登记成员数为 1–4096。复用 IPC 校验器验证 4096 字节资源头、完整清单/成员 hash、严格升序完整标的集合、共同截面 S、J=S+1、section 连续范围和零页尾。
2. 完整 ResourceHeader 必须与来自认证 registry 的预期值相等；顶层、资源头与 JOIN 的 channel/feed/ring/topic/catalog/membership/qos 必须一致。目录 hash 来自外部预期；资源消费者可为广播零值，否则必须与订阅者一致。
3. 各成员必须为 NormalizedMarketSnapshot。成员 snapshot ID 非零，在整个文件中唯一且与顶层不同。状态行额外匹配 topic 和 producer generation，FeedState 行匹配清单中的该标的状态。
4. 资源创建不晚于顶层创建；顶层和全部成员均满足 created ≤ now < expires。JOIN 截止时间也必须尚未到达。已标为有效的 ingress/publish 单调时间不得晚于成员创建时间，UTC 来源时间不与本机单调时钟混算。
5. 成员集合始终完整。缺少登记要求的 Quote/Trade/MarkFunding 或存在非 Live 成员时仍可读取已验证内容，但 `check_readiness(now)` 返回 NotReady。每个必需状态自身也必须为 Live。该方法每次比较验证时刻及所有期限的最小值。
6. `member(index)` 对索引和字节范围做边界检查，返回状态/深度只读切片及拥有的头部值。调用方修改返回的头部副本不改变已验证对象。

该模块是 JOIN 冷路径，会分配最多 4096 项的身份列表和 snapshot ID 集合。它不承担热路径零分配要求。接口不检查 Linux FD；调用者先用 `ImmutableMapping` 验证 seals，再以认证的 expected/hash 构造 `ImmutableResource`，随后调用本校验器。

## 验证与后续

接收校验使用现有完整黄金文件、重新计算合法摘要后的身份/时间冲突、多标的共同截面、缺少状态、重复 snapshot ID、全部文件截断边界测试。Red/Green 和独立审核记录位于[验证目录](../../docs/validation/topic-snapshot/README.md)。

当前只处理不可变输入上的有限检查，没有新增会发生交错的 JOIN 状态机。源码对应 TLA+ 验证将在 owner 的 pending gate、快照交付、cursor 初始 S、JOIN_READY 和失败回收接通后建立；现有 ring 模型继续覆盖 pending gate 的防覆盖行为。生产快照构造、控制资源登记与完整多进程行情链路尚未完成。
