# 账户发送协调接口

`tickwire-account-gateway` 由单个所有者串行持有 `Account`、`AccountJournal`、协议编码器、授权服务和网络连接。账户统一维护资源预留；该协调器不再调用旧的标量 `Engine::reserve`，也不双写旧业务日志。

## 创建与提交

配置绑定账户日志、writer 代次、交易所路由及意图容量。一个实例对应一个产品风险域，基线目录中的产品必须与路由一致；Spot 和 USD-M 分别创建实例。`create` 从完整基线构造账户与日志，连接实际 TLS 端点。连接建立和 TLS 握手不发送订单应用字节。

```rust,ignore
AccountGateway::create(backend, config, baseline, authority, encoder, connection)
gateway.submit(command, evaluated_at, transport_deadline)
gateway.poll(now: Instant, budget: IoBudget)
gateway.cancel_pending()
gateway.record_event(event)
AccountGateway::recover(backend, config, authority, encoder, required_frontier)
```

提交先检查身份、精确重试、容量和协议编码，再克隆账户并计算准入。以当前日志位置的下一序号构造完整 `CanonicalRecord::Admit`。授权服务同步恢复依赖后，协调器核对完整准入记录、命令、账户基线与此前日志前缀的摘要，再追加并同步 Admit。成功后发布候选账户、创建私有的 `CommittedAdmission`，最后进入网络队列。

同一 `(intent_exec_epoch, intent_id)` 的相同命令返回现有状态，不重复预留、追加或排队；内容冲突冻结协调器。准入失败不增加资源，但保留账户计算产生的冻结状态。依赖发布失败不排队并冻结协调器；可能留下的孤立依赖不能授权发送。WAL 追加或同步失败时保留候选预留并冻结，因为完整尾记录可能已经存在。重开后按实际日志恢复。

`CommittedAdmission` 只由上述成功流程构造，包含活跃 `Admission`、准入记录摘要和物理同步收据。公开的 `AdmissionImage` 或恢复时解码的记录不能自行生成此对象。

## 最终门禁与发送结果

复用 `tickwire-transport::Connection` 作为无账户和无 WAL 的发送内核。`drive` 只处理连接及已有缓冲；`try_send` 在首次接受订单应用字节前，同步读取当前授权并执行 `Account::check_admission`。授权服务返回新鲜的时间、owner 隔离证据对应状态、执行代次、策略代次、配置版本、动作许可和租约。增险动作另核行情有效性；撤单和查询仍核身份与请求期限，不因账户基线或行情过期被一律阻断。

门禁结果不能缓存到下一轮调用。传输层在门禁返回后再次检查 `Instant` 期限。协议编码与服务器时钟由编码器负责，发送协调器不会用本机墙上时间替代校准结果。

发送状态只描述传输：Queued、PartiallyAccepted、Flushed、Unknown、Aborted。`Flushed` 表示本机缓冲已冲刷；订单业务状态由账户中的交易所观察维护。任何正数接受都保留订单责任。只有精确匹配当前 send token 且接受字节为零，才进入中止流程。协调器先在候选账户调用 `Account::apply_abort`，确认已有交易所事实不与中止矛盾，再追加 `Aborted`；日志同步后发布候选账户与 Aborted 状态，释放对应预留。预校验失败时保留原账户、停止发送，不写入无法重放的本地中止决定。

网络或日志出现不可继续的错误后停止使用该连接，未确定中止的意图保留 Unknown。账户因事实冲突冻结时仍保存后续真实输入；协调器不通过网络错误或本地取消推导交易所已撤单。

## 成交与管理记录

交易所事件整体写入并同步 WAL 后，才调用 `Account::apply_event`。不拆分成交中的费用和订单观察，不在日志层按 trade ID 丢弃重复输入。费用闭合、外部现金流、外部仓位及两类归属转移也走同一顺序入口。公共事实入口不接受调用方构造的 Admit 或 Aborted。

账户拒绝已保存的输入时保留原记录并冻结，恢复不能跳过该输入。账户查询接口只返回不可变引用或视图，调用者不能绕过协调器修改账户。

## 恢复与验证

恢复委托 `AccountJournal` 校验 bootstrap、完整基线、物理链、连续序号和 writer 代次。每个 Admit 的历史依赖使用其之前的精确日志前缀重新核验。账户随后按原顺序重算准入、应用事实并保持冻结。返回对象没有网络连接或发送队列；持久 Aborted 保留，其余已提交意图不自动重发。

测试覆盖 Spot 与 USD-M 的实际 TLS 签名请求、先同步后发送、零接受中止、部分接受、依赖和 WAL 故障、完整事实顺序、重复命令、恢复后的资源与无重发。TLA+ 组合模型在实现完成后按上述公开 API 和传输首次接受边界建立。
