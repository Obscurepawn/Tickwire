# TopicSnapshot 构造接口

`build_topic` 把 owner 在同一发布边界 S 固定的完整标的状态，编码为一个可交付的 NormalizedMarket TopicSnapshotManifest。owner 在调用前已建立 pending gate；构造失败返回错误，不修改 Publisher、cursor、JOIN 模板或输入缓存。布局与状态含义沿用 [IPC 快照契约](../design/ipc-abi.md#33-快照与-instrument-目录)和[接收接口](topic-snapshot-interface.md)。

```rust
pub const MAX_TOPIC_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
pub struct BuildRequest<'a> {
    pub resource: &'a ResourceHeader,
    pub join: &'a JoinBody,
    pub catalog_hash: [u8; 32],
    pub instrument_set_version: u64,
    pub sequence: u64,
    pub created_mono_ns: u64,
    pub expires_mono_ns: u64,
    pub now_mono_ns: u64,
    pub logical_strategy_id: u64,
    pub instruments: &'a [InstrumentSpec],
    pub members: &'a [InstrumentSnapshot<'a>],
    pub byte_limit: usize,
}
pub struct InstrumentSnapshot<'a> {
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub snapshot_id: [u8; 16],
    pub at_sequence: u64,
    pub last_event_sequence: u64,
    pub feed_generation: u64,
    pub catalog_version: u64,
    pub catalog_hash: [u8; 32],
    pub created_mono_ns: u64,
    pub expires_mono_ns: u64,
    pub state_version: u64,
    pub coverage_depth: u32,
    pub coverage_flags: u32,
    pub states: &'a [MarketRecord],
    pub bids: &'a [DepthLevel],
    pub asks: &'a [DepthLevel],
}
pub fn build_topic(request: &BuildRequest<'_>) -> tickwire_ipc::Result<BuiltTopic>;
```

`BuiltTopic` 拥有完整 `Vec<u8>`，提供 `bytes()`、`resource()`、`header()`、`join()`、`file_hash()` 和消费对象的 `into_bytes()`。getters 只提供共享借用或摘要副本；错误时没有可交付的半成品。构造属于 JOIN 冷路径，可分配内存；`byte_limit` 是该对象的剩余预算，必须不超过 64 MiB。完整成员计数、section 长度、对齐及累积总长先用 checked arithmetic 计算，再分配完整输出。

`resource` 是已认证 owner 提供的身份模板。构造器统一设置格式、类型、record/count/size、slot offset、零 arena/cursor 布局及 descriptor hash；保留并检查资源、channel、producer/consumer、host/namespace、generation、目录、membership、时钟与信任模式。`join` 提供真实订阅、bank/cursor 和 deadline，构造器只填写 snapshot ID/hash/S、J=S+1、集合 hash/version。这些派生字段允许零或与结果相同，非零冲突拒绝；其他身份字段不被覆盖。输入模板来自 owner 的认证上下文，不是接收文件中的自报身份。

登记标的和成员必须一一对应、严格升序且唯一，数量为 1–4096。每个成员显式声明 `at_sequence=S`；最后实际事件序号允许小于 S。feed generation、目录 hash/version、instrument version 必须匹配本次截面。所有 snapshot ID 非零且互异。创建、截止和 now 来自同一单调时钟域；所有成员在构造时都必须处于有效窗口。

每个成员提供 1–4 个状态记录，类型限 Quote、Trade、MarkFunding、FeedState 且不重复；FeedState 必需，其枚举决定清单 feed_state。构造器按类型排序，将 sequence 写为 1 起行号，重新计算启用的记录 CRC；保留原时间、flags 和业务状态。输入记录的原 sequence 不得超过成员最后实际事件序号。缺少登记要求的 Quote/Trade/MarkFunding 或非 Live 状态仍完整保留，由 `check_readiness` 返回 NotReady。

买侧严格降序、卖侧严格升序，规范快照中的已存层价格和数量均为正；Live 成员拒绝交叉簿。coverage_depth/flags 原样保留并检查有效位，不把未知外侧补成零，也不提升 FeedState。子对象统一 kind=6，section 为 StateRecords 后接 DepthLevels；每个 offset 是文件绝对位置，零页尾覆盖进子 hash。顺序为 section 数据 hash、section 描述符 hash、子对象 hash、清单行 hash、资源头 hash、完整文件 hash。

最终产物使用现有 `validate` 与完整认证上下文再次检查。返回对象只证明字节及上下文有效；实际 memfd 封印、SCM_RIGHTS 交付、ReadReady 和 JOIN_READY 仍由 JOIN owner 协调器完成。
