# Binance 私有回报入账接口

`tickwire-binance-account` 将私有会话交付的事实接到账户所有者。一个实例固定账户风险域、产品、账户基线和输入日志代次，持有有界订单关联表及真实输入 WAL。它与 `AccountGateway` 在同一线程串行运行。

处理顺序为：会话交付原始事件 → 校验来源并关联订单 → 保存原文及确定的处理决定 → 同步输入 WAL → 通过持久输入键幂等写入账户 WAL → 发布账户变化。无法关联的事实保留原文并冻结增险，后续已知订单的成交仍继续入账。

## 订单登记与调用接口

```rust,ignore
pub struct AccountIngress<S: tickwire_journal::store::Store> { /* 单 owner */ }
pub struct InputKey {
    pub stream: [u8; 16],
    pub sequence: u64,
}
pub enum Outcome {
    Applied { input: InputKey, applied: tickwire_account::Applied },
    Quarantined { input: InputKey, reason: QuarantineReason },
}

impl<S: Store> AccountIngress<S> {
    pub fn create(store: S, config: Config) -> Result<Self>;
    pub fn recover(store: S, config: Config,
                   required: Option<&PhysicalFrontier>) -> Result<Self>;
    pub fn submit(
        &mut self, gateway: &mut AccountGateway<impl Backend, impl Authority, impl Encoder>,
        command: CanonicalCommand, order_ids: &[[u8; 16]],
        evaluated_at: u64, deadline: Instant,
    ) -> Result<Status>;
    pub fn ingest(
        &mut self, gateway: &mut AccountGateway<impl Backend, impl Authority, impl Encoder>,
        event: &tickwire_binance_private::PrivateEvent<'_>,
    ) -> Result<Outcome>;
    pub fn source_gap(
        &mut self, gateway: &mut AccountGateway<impl Backend, impl Authority, impl Encoder>,
        gap: &tickwire_binance_private::GapNotice,
    ) -> Result<InputKey>;
    pub fn replay_pending(
        &mut self, gateway: &mut AccountGateway<impl Backend, impl Authority, impl Encoder>,
        maximum: usize,
    ) -> Result<usize>;
}
```

`submit` 先检查完整规范命令和内部订单 ID，保存登记，再调用账户准入与发送。登记只允许与同一原意图关联的完整命令；相同命令及 ID 重试复用原登记，冲突使本实例停止。订单 ID、同标的 client ID 和本地订单列表 ID 不复用，登记表容量耗尽即拒绝新登记。账户准入失败留下的登记不代表已经提交；只有网关保存了相同命令，回报才可按该登记入账。

本阶段为 `New`、`CancelReplace` 和 Spot 订单列表按创建顺序登记内部订单 ID。撤单及查询不创建订单登记。Algo 子单需要先由协议响应确定父子及原生 ID 关系，不能根据普通成交回报自行猜测；这类尚无可靠关联的事实进入隔离区。

关联优先使用已经确定的交易所订单 ID；首次回报可以通过标的和原 client ID 找到唯一登记。匹配后由现有 `bind_order` 校验产品、方向、数量单位、原意图及列表身份，生成规范事件。一个原生 ID 不能绑定两个本地订单。相同 ID 的冲突或多个候选不会按“第一个匹配”处理。

## 输入日志与账户日志

输入 WAL 使用 `tickwire-journal` 的真实批同步和恢复机制。第一条记录固定完整配置摘要及账户基线；恢复时逐项核对，不能把另一个账户的输入日志接到当前网关。记录包括：

| 记录 | 保存的内容 |
| --- | --- |
| 配置 | 风险域、venue、账户、环境、产品、基线摘要、输入流代次和容量 |
| 登记 | 完整规范命令、按顺序分配的内部订单 ID |
| 已关联事件 | 原始事件 JSON、来源上下文、原接收时间、完整规范事件 |
| 隔离事件 | 同样的原文与来源、明确失败原因 |
| 来源缺口 | 原连接代次、缺口身份及原因 |

Spot 保存事件体的原始 JSON 字节和已验证的订阅上下文；USD-M 保存原事件。日志不保存 API key、签名密钥或 listenKey。生产事件通过现有解码器到达本模块；恢复已关联事件使用保存的规范字节，避免新版解析器悄然改变历史入账含义。

`InputKey` 使用输入 WAL 的流代次和实际记录序号。账户增加 `RecoveryRecord::IngressEvent { input_stream, input_sequence, event }`，将此键与规范事件一起写入账户 WAL。`AccountJournal` 对同一输入键的相同事件返回原收据，内容冲突拒绝；物理日志中重复出现同一输入键视为非法历史。`AccountGateway::record_ingress_event` 在已处理键上直接返回重复结果，不再次调用账户 reducer。

首次处理先在候选账户中验证完整事件，成功后同步账户记录，再发布候选账户。业务校验失败保留原账户并冻结，由输入日志记录隔离决定；它不会向账户 WAL 写入已知无法重放的事件。追加或同步结果不确定时停止该输入 owner，账户保持冻结。

两份日志之间没有跨文件事务：

- 输入已同步、账户尚未写入：恢复后补交保存的规范事件。
- 账户已同步、调用方未收到结果：恢复后同一输入键命中原记录，不重复入账。
- 两边都已完成：重复恢复继续返回已处理。

恢复不会重新发送订单，也不会开放账户增险。`replay_pending` 按输入日志顺序、在给定数量预算内补交；已完成条目可以按幂等键跳过。有限登记表及日志容量不通过淘汰旧身份腾空间，后续分段归档必须保留完整恢复关系。

## 缺口、隔离与后续事实

收到来源缺口、未知订单、未支持的账户变更或身份冲突时，先调用 `gateway.freeze_account()`，再保存输入。冻结立即阻止后续增险门禁，撤单、查询和已知订单事实仍可处理。输入同步失败时回调返回错误，私有会话停止交付；不能把未保存的事实确认成功。

已保存的隔离事件返回 `Outcome::Quarantined`，调用者可以继续接收后续事实。余额及持仓消息是稀疏更新，第一版保留原文并要求后续对账；它们不能直接替代完整账户基线。缺口及隔离记录只有经过后续对账与激活协议才能解除，模块不提供单独的 `unfreeze`。

`AccountGateway` 增加只读命令、基线及已处理输入查询，保持账户和日志的可变状态私有。私有事件包装增加委托 `bind_order` 的方法，不暴露可以伪造网络来源的构造器。配置显式限制原文字节、单条编码大小、登记数和输入记录数；超限冻结并返回错误，不截断事实。

必要回归覆盖真实私有事件到文件 WAL、订单登记先于准入、同键重试与冲突、输入同步后进程重开、账户已同步但结果丢失、未知回报后的继续入账、Gap 后增险拒绝，以及不同账户/基线/流代次拒绝。形式模型随后转录双日志完成点与幂等键，不把同步回执视为订单执行确认。
