# 真实 JOIN owner 与消费者接口

`tickwire-join` 将已认证控制会话、实际共享 ring、完整快照构造器和 Linux 文件描述符交付接成一条 JOIN 流程。owner 先在发布边界保留 pending reader 的容量门禁，再构造并交付快照；消费者完整装载基线、追平并提交 JOIN_READY 后，owner 才允许实际 cursor 的释放水位推进门禁。

## owner 与发布边界

`JoinOwner<M>` 独占一个 `tickwire_shm::Publisher<M>`，保存该 topic 的目录 hash、完整且有序的 `InstrumentSpec` 集合、集合版本以及已登记订阅。所有发布都经过 owner；调用方不能同时从另一句柄推进该 Publisher。每个 Owner 累计保存最多 M 次登记，包括失败、过期和已 detach 的历史条目。达到上限后返回 Capacity，不自动复用历史槽位；持久代次高水位和 owner 重建协议另行实现。

```rust
pub struct TopicRegistry {
    pub catalog_hash: [u8; 32],
    pub instrument_set_version: u64,
    pub instruments: Vec<tickwire_snapshot::InstrumentSpec>,
}

pub struct SubscriberRegistration {
    pub identity: tickwire_control::Identity,
    pub subscription: tickwire_shm::SubscriptionKey,
    pub qos: u16,
}

pub struct JoinPlan {
    pub resource: tickwire_ipc::ResourceHeader,
    pub join: tickwire_ipc::JoinBody,
    pub sequence: u64,
}
```

准备入口接收 Supervisor 的 `Session`、`RequestStore`、原始 JOIN 帧、可信订阅登记、此次 snapshot ID、单调时间，以及 `FnOnce(&JoinPlan) -> tickwire_ipc::Result<BuiltTopic>` 构造闭包。入口先调用 `Session::admit`，使相同请求重试先得到既有结果，再检查状态版本。新请求核对请求方角色、subscriber 身份/代次、bank/cursor、topic/channel/feed/ring、QoS、catalog、完整集合和期限；请求的基线与快照字段必须为零。

核验通过后调用真实 `Publisher::join`，取得 `J`，令 `S=J-1`，记录已增加的 membership。随后在同一次 `&mut JoinOwner` 调用中把计划交给 `tickwire_snapshot::build_topic`。应用须在同一 owner 串行域中提供 S 下的状态；构造器检查成员声明的共同截面，串行持有状态缓存负责使该声明对应实际数据。`ResourceHeader.first_seq` 始终保留 bank 起点 F。

构造成功后，以 `ImmutableMapping::create` 封印完整文件，并执行资源包装校验。最终 JOIN 响应保存构造器产生的 snapshot ID/hash/S/J 和原始认证绑定。构造、封印或结果存储失败后，pending 成员仍存在；重试不会创建另一个成员或解除它的 gate。

## FD 交付、资源确认与 JOIN_READY

owner 对 subscriber 的独立认证会话交付 bank 和 snapshot。两者分别发送 RESOURCE_OFFER：bank 使用 SingleWriter、`0x17`，snapshot 使用 ImmutableObject、`0x0f`，避免在同一 offer 的 required_seals 中混合两种封印模式。每个 offer 固定 ID、资源 ID、长度、header hash、FD index、访问方式和注册代次，SCM_RIGHTS 的接收 FD 全部以 RAII 持有。

消费者首先取得可信最终 JOIN 和预期资源信息，再核验对应 offer。它映射 bank、完整不可变快照和独立 cursor，调用资源/快照验证器，并通过基线装载回调安装完整业务视图。快照校验或装载失败保持未 Ready。缺少必需状态的成员保留在视图中，由 readiness 检查拒绝。

消费者回送实际 cursor 的只读能力，以及 bank/snapshot 各自的 READ_READY。owner 比对认证 subscriber、offer ID、资源 ID、header hash、mapped_bytes、observed_seals、access、ack_kind 和 registration_generation。收到 cursor 时只登记真实映射，pending gate 仍固定在 S；即使该 cursor 已经释放后续记录，也不推进这个门禁。

消费者可从 J 连续借用并应用后续记录。发送 JOIN_READY 时，它再次检查快照与 JOIN 期限，正文必须保留原 JoinBody 全部绑定。owner 核对两项 READ_READY、cursor 登记、完整原正文及会话期限，然后读取实际 cursor，要求它已应用至当前 published。落后时保持 pending，消费者追平后重试。首次激活保存实际水位；相同正文的后续 READY 返回该历史结果，不因新发布再次激活。其他 JOIN 会增加全局 membership，但每个订阅始终核对自己登记时的版本。成功只表示该订阅已完成 JOIN，不授予 exec 增险资格。

## 共享映射 SDK 的必要接缝

新流程使用以下 `Publisher` 方法：

```rust
pub fn membership_version(&self) -> u64;
pub unsafe fn register_pending_cursor(
    &mut self, sub: SubscriptionKey, fd: OwnedFd,
) -> Result<()>;
pub fn activate_join(
    &mut self, sub: SubscriptionKey, baseline: u64, minimum_applied: u64,
) -> Result<Watermarks>;
pub fn detach_registered(
    &mut self, sub: SubscriptionKey, final_watermarks: Watermarks,
) -> Result<()>;
```

`register_pending_cursor` 继承现有 register_cursor 的完整 Safety 合同：真实 peer 使用合格 SDK，header 初始化后不可变，所有 cursor 字段只由唯一写者按同宽原子规则访问，released 绝不越过活借用。身份认证本身不能证明这些内存行为。`Subscriber::attach` 的 publisher/gating 合同也保留；新 crate 在明确的窄映射边界调用这些 unsafe 接口，不对任意已认证 FD 承诺内存安全。

`activate_join` 比对准确 baseline、实际 lifecycle 和水位。同一 owner 调用期间 published 固定，先采样 applied/released，最后采样 read；单调性保证合法消费者不会因较晚推进 read 而被误判。它检查 `S <= applied/released <= read <= published` 以及 `applied >= minimum_applied` 后，才激活原 pending 成员。发布时只刷新已激活成员的真实 released。

旧 `register_cursor` 保留给已经完成全部资源与快照授权的低层调用者；新 JOIN 协调器始终使用 pending 登记及显式激活。Producer/Reader 的原状态迁移语义不变。

## 超时、quiesce 与回收

构造失败、交付失败、会话过期和 JOIN 超时均保留 gate。超时只使订阅转入等待 quiesce 的流程。消费者停止新借用，全部 guard 结束后发布真实 `Cursor.lifecycle=Quiesced`；owner 对已登记的同一代 cursor 核对 lifecycle、quiesced_seq 和最终水位，再执行 `detach_registered`。普通控制正文不能伪造这项证据。回收后保存完整 ACK，只有相同证据才能幂等成功。

当前实现不提供“进程已经死亡”的布尔入口，也不根据心跳或 socket EOF 自动 detach。尚无 cursor 的失败订阅继续占 gate；后续死亡回收须由 pidfd 和完整 holder 登记证明所有借用进程退出，并提供不可复用的退出证据 ID。

Linux 验收覆盖真实进程、HELLO/WELCOME、builder、SCM_RIGHTS、封印资源和实际 cursor。容量使用代码允许的 64：pending 消费者已经应用并释放 64 条时，第 65 条仍须 Full；完成 JOIN_READY 后才允许复用。还验证构造/交付/期限失败、缺 READ_READY、错误身份/hash/generation、未追平、活借用与伪造 quiesce 的拒绝行为。生产路径和测试均不以平行 Mock ring 替代实际共享内存。


## Linux 进程接线

`tests/process_linux.rs` 的主进程持有 owner，两个独立 exec 子进程分别执行 Supervisor 和 consumer。每条 pathname SOCK_SEQPACKET 连接都核验实际 SO_PEERCRED，并完成 HELLO/WELCOME。父进程持有未 reap 的 Child，锁定登记的 PID；全部时间来自同一 Linux 时间命名空间的 CLOCK_MONOTONIC。

Supervisor 发出真实 JOIN 并检查响应和精确重试。consumer 的可信最终 JoinBody、bank/snapshot 预期 header 由测试 launcher 传入；它们独立于收到的文件。consumer 再核对已认证 RESOURCE_OFFER、映射封印资源、安装完整状态和盘口，回送实际 cursor FD 与两项 READ_READY。Supervisor 向 consumer 分发这份可信登记的常驻服务仍未实现，测试没有把 launcher 交接称作完整生产控制面。

receiver 在调用 `JoinConsumer::attach` 前负责验证 offer 的 envelope、完整 descriptor、封印要求、期限与 FD 数量；预期 header/hash 来自可信登记。该低层 attach API 接收 OwnedFd，失败时自动关闭；正常生产路径不会从收到的文件反向构造预期值。`load_baseline` 成功只在应用回调完成后设置 loaded；业务状态缓存归调用者持有。

本阶段的 RequestStore 测试使用显式 MockRequestStore，实际 ring、cursor、封印文件和文件描述符传输均为 Linux 实现。持久控制请求恢复、常驻 launcher、pidfd holder 回收、业务适配器和 exec 增险准入仍是后续集成工作。
