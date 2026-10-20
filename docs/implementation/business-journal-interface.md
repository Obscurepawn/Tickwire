# 业务 WAL 与冻结恢复接口

`tickwire-business-journal` 将现有物理 WAL、业务 codec 和 execution reducer 接到同一条持久事件流。严格 runtime 私有持有 engine 与 journal，只提供订单、资金和日志位置的只读视图；发送只能经过受信授权服务、完整闭包验证和真实 WAL 同步。旧 execution receipt 暂作封装内部的状态迁移参数，不作为新接口的外部授权入口。

## 完成边界与服务接口

新接口使用 `WalAppendReceipt` 表示某条业务事件已包含在完成同步的物理前缀中，字段私有，提供 `journal_seq/logical_hash/physical_frontier` 只读访问。它没有 LocalCommitProof 或镜像证明的名称与转换接口。`BusinessJournal::commit_intent` 按完整请求和原 intent_seq 幂等：重复调用沿用原 journal_seq 和逻辑哈希，同 ID 或 intent_seq 对应不同内容则拒绝并冻结；重开后可以返回覆盖该位置的较新物理前缀。

授权与闭包在 WAL 追加前核验，端口为：

```rust
pub trait CommitAuthorizer {
    fn current_gate(&mut self) -> Result<Gate, AuthorizationError>;
    fn authorize(&mut self, request: AuthorizationRequest<'_>)
        -> Result<VerifiedClosure, AuthorizationError>;
    fn verify_recovery(&mut self, request: AuthorizationRequest<'_>)
        -> Result<VerifiedClosure, AuthorizationError>;
}
```

`AuthorizationRequest` 包含完整 `DurableRecord`、期望 `ClosureBinding` 和摘要对应的规范业务原像。`authorize` 可以首次持久化闭包；`verify_recovery` 只核验已有对象，并重新完成相同内容的同步。缺失对象必须失败，恢复不能通过重新发布补造决策依赖。

`current_gate` 负责读取可信时间、执行代次、策略与配置版本、行情和账户有效性。runtime 在预留前与全部授权/WAL I/O 完成后各调用一次；`resume_submit` 也重新读取。最后一次门不允许发送时，已同步意图和资金预留保留，runtime 冻结。提供者不能返回缓存的历史时间；该职责以后可拆成独立 GateProvider，语义保持一致。

`CommitAuthorizer` 是受信服务边界，负责当前策略、配置及执行授权。默认不提供无条件通过的生产实现。测试使用显式 MockAuthorizer，但仍通过 `tickwire-recovery::Persistence` 校验并持久化依赖后取得私有构造的 VerifiedClosure。该 capability 只证明所选存储后端的闭包；外部授权、签名、防回滚和镜像完成不能从它推导。

runtime 将 capability 的 risk_domain_id、intent_id、intent_exec_epoch、business_digest、strategy_generation、config_revision、recovery_object 与当前意图逐项比较；不匹配时不写 PreparedIntent、不调用 Exchange。闭包服务持久化不可变的 binding→manifest_root 映射及 pin，恢复按同一绑定重验，避免把可变 u64 对象标识当成内容根。当前实现只开放 LocalDurable；SyncReplica 在指定镜像证明接口完成前拒绝。

## 身份、顺序与恢复

物理 DataRecord.exec_epoch 对应 codec.writer_exec_epoch；原订单 request.epoch 对应 intent_exec_epoch。PreparedIntent 两者相等；迟到事实保留原订单代次，写入时使用当前 writer 代次。按配置核对 risk_domain_id 与 venue/account/product，journal_seq 来自连续物理记录序号，intent_seq 保留 reducer 的意图编号。Engine 当前按 intent_id 查找，因此本阶段仍要求同一风险域内 intent_id 不跨代次重用。

全流顺序为任意交错的 PreparedIntent、Fill、Fee、Terminal、AbortedBeforeSend，不再分别保存“全部意图”和“全部事实”数组。提交前先构建候选 codec 记录、逻辑链和索引，物理 `append_batch` 同步成功后才发布内存状态。写入或同步失败关闭增险入口；已写但未返回的完整批在重开后按原内容恢复，不据回执丢失重发订单。

`NoBytesAccepted` 的唯一释放顺序是：可信 Exchange 返回零字节结果 → 记录该订单身份的 AbortedBeforeSend → 完整批同步 → 调用 reducer 的 `transport_rejected_without_bytes`。若同步失败，保留全部预留并冻结；若事实与已有成交或费用矛盾，保留已写审计事实并冻结。重启依序重放 AbortedBeforeSend 后才能释放对应预留。

新进程 `recover` 先核对业务 bootstrap，再检查每条物理记录与 codec 的域、代次、序号，重算逻辑链并重验每个意图的闭包。随后在新建且冻结的 engine 上按原顺序重放。未知意图、原订单身份不匹配、冲突交易结果记录、算术溢出或闭包丢失会阻止恢复完成。本接口通过静态 `recover` 返回新 runtime，没有原地替换或解冻入口；空日志恢复也冻结。恢复过程不调用 Exchange，当前不提供解除恢复冻结的入口。

## Bootstrap 与实际存储

`FileBackend` 包装真实 `FileStore`，复用其单写锁与段文件；一次性 `BUSINESS.json` 固定业务 schema、codec schema、risk_domain_id、journal_epoch、venue/account/product 和初始 capital。创建时同步文件及目录，已有 bootstrap 不覆盖；恢复逐字节核对调用方提供的预期配置，再以该配置重建资金基线。该文件的校验与可信目录假设防止无意配置错配，不宣称签名或外部可信启动、防介质回滚。

测试后端实现同一 `Backend` 协议，可在 bootstrap、追加、同步阶段注入失败。正常批次仍只用物理 WAL 的一次文件同步，不增加逐批 bootstrap 或 manifest 同步。物理 `Journal` 仅补只读 `config()`，使空日志恢复也能核对真实 domain/journal_epoch；既有 WAL 格式不变。

## 验证与后续范围

当前测试覆盖真实文件提交、统一事件顺序、跨 writer 幂等、迟到事实、身份冲突、同步等待期间过期、NoBytes 写入/同步失败、闭包缺失、bootstrap 错配，以及实际子进程终止后的文件 WAL/闭包恢复。外部 `record_fact` 拒绝 AbortedBeforeSend；它只能由本 runtime 的 Exchange 零字节回调生成。事实写入失败时冻结增险入口，调用方仍负责保留外部事实来源并完成账户对账。

BusinessJournal 是存储原语；公开的 append_fact 本身不提供交易权限。Runtime 私有持有它，不开放可变引用。只有 runtime 的受信授权服务与新鲜门可以启动发送。测试 MockAuthorizer 使用任意示例授权对象，闭包格式和持久化是真实实现，策略授权语义仍是显式 Mock。

新建 FileBackend 复用物理目录的排他锁，固定 canonical 路径，进程切换工作目录不改变后续 I/O 目标。目录建立和重试都同步父目录；正常批次不因此增加一次同步。已有物理格式和 codec 原像保持不变。

业务 WAL L0 检查新增的提交、中止和冻结恢复顺序。完整签名 LocalCommitProof、MirrorCommitProof、真实策略权限提供者、可信资本来源、防回滚、恢复后重新授权及快照压缩仍未实现。进程终止测试覆盖进程崩溃，不覆盖实际掉电。
