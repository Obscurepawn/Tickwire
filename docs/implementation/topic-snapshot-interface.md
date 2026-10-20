# Topic 快照与 JOIN 基线接口

真实行情进程和策略进程之间的 JOIN 使用不可变 TopicSnapshotManifest。owner 在单一发布边界 S 固定完整标的集合及全部状态，先将待加入读者的 gating 设为 S，再构造清单。消费者完整验证并装载后，才从 J=S+1 消费。现有 Publisher 的 pending gating 和 IPC 快照解码器作为底层原语，业务协调器负责连接两者。

## 构造与验证边界

`tickwire-snapshot` 已实现完整字节对象的规范构造和上下文校验；构造 API 见[TopicSnapshot 构造接口](topic-snapshot-builder-interface.md)。`tickwire-shm-sys::ImmutableMapping` 负责 0x0f seals 与只读映射。两层都不自行签发 JOIN_READY 或交易授权。

构造输入包含可信资源身份、同一目录版本/hash、完整且有序的 instrument/version 集合、集合版本、共同 feed generation、S、创建/截止时间及各成员状态。每个 NormalizedMarket 成员具有自身 snapshot ID、最后实际事件序号、FeedState、最新 Quote/Trade/MarkFunding（按登记能力要求存在）以及买卖深度和覆盖信息。最后实际事件序号可以小于 S，但所有子快照的截面都必须是 S。

子对象采用现有 snapshot_kind=6：256 字节 SnapshotHeader，两个 64 字节 section 描述符，连续 StateRecords 和 DepthLevels，最后补零到 4096 边界。状态记录按 message_type 唯一升序，内部 sequence 重排为 1 起的行号，保留原观测时间。深度沿用整数 tick/lot；覆盖不足继续表示 Unknown。清单使用 snapshot_kind=4/resource_kind=8，先生成子对象 hash，再生成 manifest 行 hash、资源头 hash 和最终文件 hash；避免内容摘要自引用。

构造器统一计算 count、offset、size、hash，拒绝重复/缺失标的、未知必需能力、超出 S 的成员事件、混合版本或 generation、越界/过期时间、非法深度和超过 64 MiB 的完整对象。一个清单及全部子对象共用大小上限。无有效状态的标的仍写入清单，并保留 Bootstrapping/Invalid 状态，后续 readiness 拒绝它们。

接收方从已认证控制会话与 registry 取得 expected ResourceHeader，从 JOIN 取得最终文件 hash；核对 OS seals、完整头和文件 hash 后，再验证清单/全部子对象的身份、范围、目录、集合、S 与内部 hash。现有 `validate_topic_bundle` 提供结构与成员一致性检查，业务包装还要核对 ResourceHeader、顶层 SnapshotHeader、JoinBody 三者的 channel/feed/ring/catalog 身份、有效期限和登记能力。禁止用收到的文件自行填写 expected identity/hash。

## JOIN 状态协调

业务 owner 串行持有 Publisher 和标的状态缓存。收到已认证 JOIN 后核对 bank/cursor、subscription generation、集合和目录，在无未提交批次的发布边界保留 pending gate，再构造/封印/交付快照。构造或交付失败仍保留 gate，后续走明确的取消能力或 quiesce/退出回收；超时本身不是释放仍可能存活读者的证据。

消费者验证整个清单后，以 S 初始化 cursor 的 read/applied/released，完成资源 ReadReady 和业务视图装载，再回传完整绑定的 JOIN_READY。owner 比较原快照、membership、generation、期限及实际 cursor；通过后转 Active。策略增险还须独立经过 exec 的业务准入。

## 接收校验 API

纯安全 Rust 的接收校验器接口如下，构造器使用同一校验器复核最终输出；JOIN owner 负责连接发布边界和 cursor：

```rust
pub struct InstrumentSpec {
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub required_states: u8, // Quote=1, Trade=2, MarkFunding=4; FeedState 总是必需
}
pub struct JoinContext<'a> {
    pub expected_resource: &'a ResourceHeader,
    pub catalog_hash: [u8; 32],
    pub instruments: &'a [InstrumentSpec],
    pub join: &'a JoinBody,
    pub now_mono_ns: u64,
}
pub fn validate<'a>(bytes: &'a [u8], context: &JoinContext<'_>)
    -> Result<ValidatedTopic<'a>>;
```

`ValidatedTopic` 只借用不可变字节，暴露 header、成员数量及经过边界检查的成员读取。它不保留 context 引用；构造时核验完整资源身份、JOIN 的 channel/feed/ring/catalog/membership、顶层与全部成员期限，以及状态记录的 topic/producer generation。输入字节可来自受封印保护的映射，也可来自本进程拥有的只读缓冲区；OS seals 仍由 `ImmutableMapping` 验证。

创建时间、截止时间和 `now` 必须来自同一 host boot、同一 Linux 时间命名空间的单调时钟。启动器和认证资源会话负责维持这个前提，文件中的 namespace 标识不能单独证明实际 OS 时间命名空间。

`check_readiness(now)` 在每次调用时检查时间没有早于验证时刻，且未达到任何成员、清单或 JOIN 的最早截止时间；还要求所有 FeedState 为 Live，具备登记要求的状态，且这些必需状态本身也为 Live。缺状态和非 Live 成员保持在对象内，该方法返回 `NotReady`，不能通过删除成员伪造 Ready。该检查只描述快照基线，逐状态行情新鲜度、实际 cursor、资源会话及 exec 增险授权仍由对应所有者检查。该值是调用时的判断，不是可以长期缓存的交易许可。

TDD 先以现有完整基准样本确认兼容，再通过重新计算合法 hash 的身份/时间/状态冲突输入，验证新增业务约束；单纯破坏摘要不能代替这些测试。多标的构造、快照交付与待加入读者的满环背压将在 owner 协调模块的集成测试中验收，并以真实状态迁移建立有限 TLA+ 模型。
