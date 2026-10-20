# 业务日志编码与逻辑链

`tickwire-journal-codec` 把执行内核的有界支出意图、成交、费用、终态与发送前中止事实编码为确定性 CBOR。它独立于 execution、Mock 和物理 WAL；输出可直接作为现有 WAL v1 的 `DataRecord.payload`。本切片提供内容编解码和逻辑链计算，不签发提交、依赖闭包或交易授权证明。

## Schema 1

每条记录恰好包含以下七个整数键，按表中顺序编码：

| 键 | 字段 | 类型与含义 |
| --- | --- | --- |
| 0 | schema_version | 固定整数 1 |
| 1 | risk_domain_id | 16 字节 byte string，与物理帧 domain 一致 |
| 2 | journal_epoch | 16 字节 byte string，与物理帧 stream_epoch 一致 |
| 3 | journal_seq | u64，全事件日志序号 |
| 4 | writer_exec_epoch | u64，本次写入者的执行代次，与物理 DataRecord.exec_epoch 一致 |
| 5 | event_type | 1 PreparedIntent、2 Fill、3 Fee、4 Terminal、5 AbortedBeforeSend |
| 6 | payload | 下述确定长度 map |

payload 公共键为 `0 venue:u32`、`1 account:u64`、`2 product`、`3 intent_id:u64`、`4 intent_seq:u64`、`5 intent_exec_epoch:u64`。product 固定为 1 Spot、2 UsdM。`intent_seq` 保存原 `DurableRecord.sequence`，不拿它替代 `journal_seq`：一个意图之后的成交、费用和终态各占新的日志位置。`intent_exec_epoch` 保存原订单执行代次，对应原 request.epoch；接管后的写入者可以记录属于旧订单代次的迟到事实。

| 事件 | 额外键及类型 |
| --- | --- |
| PreparedIntent | 6 quantity:u64；7 unit_budget:i128；8 fee_budget:i128；9 strategy_generation:u64；10 config_revision:u64；11 deadline:u64；12 recovery_object:u64；13 commit_profile |
| Fill | 6 trade_id:u64；7 quantity:u64；8 cost:i128 |
| Fee | 6 fee_id:u64；7 amount:i128 |
| Terminal | 6 status；7 cumulative:u64 |
| AbortedBeforeSend | 无 |

commit_profile 为 1 LocalDurable、2 SyncReplica；status 为 1 Filled、2 Canceled、3 Rejected、4 Expired。字段与现有 [OrderRequest/Fill](../../crates/tickwire-execution/src/lib.rs)、[ExecutionFact](../../crates/tickwire-execution/src/ports.rs) 对应，只有独立日志序号和发送前中止事件补足按发生顺序记录的需要。`recovery_object` 仍是当前内核的对象标识，不充当未来 RecoveryManifest 根哈希。

codec 保留 u64 与 i128 的全部数值，包括零数量和负成本等需要业务 reducer 拒绝的事实。解码只验证文法和表示；AbortedBeforeSend 也只是一条审计内容。后续业务适配器负责检查 PreparedIntent 的两种执行代次相等、事实引用的原订单有效、域标识与 venue/account/product 配置映射一致；真实零字节结果经过核对并持久化中止事件后，才释放保留资金。

## 编码与 API

map 长度固定，键升序，整数和长度使用最短表示。未知、缺失、重复、乱序键、尾随内容、不定长项、浮点数和 schema 以外的 CBOR 类型均拒绝。i128 优先使用 major type 0/1；需要更大幅度时使用 tag 2/3，后接 9–16 字节、无前导零的大端 byte string。负整数的幅度是 `-1-value`，因此无需对 `i128::MIN` 求负；正负幅度都不得超过 `i128::MAX`。这采用 [RFC 8949 §3.4.3](https://www.rfc-editor.org/rfc/rfc8949.html#section-3.4.3) 和 [§4.2.1–4.2.2](https://www.rfc-editor.org/rfc/rfc8949.html#section-4.2.1) 的确定性规则。

Schema 1 最长记录为 201 字节：信封 62 字节，加所有整数取最大编码长度时的 PreparedIntent payload 139 字节。解码先检查总长度，随后按固定文法读取，不分配输入声明长度对应的缓冲区。

`encode(&Record) -> Vec<u8>` 产生唯一字节表示；`decode(&[u8]) -> Result<Record, DecodeError>` 要求完整消耗输入。`business_digest(&Record)` 返回稳定业务摘要，`logical_payload_hash(&Record)` 再绑定写入者代次。`LogicalChain::new(domain, journal_epoch)` 从原始流 genesis 开始，`append` 检查身份、连续序号与写入者代次后前进；`verify_and_append` 额外比对调用者提供的期望逻辑哈希。任何错误保持原链状态不变。

## 摘要边界

以下 `CBOR(...)` 均使用上述确定性编码，字符串为 UTF-8 text string，domain/epoch/hash 为 byte string，payload 为同一事件 map。

```text
business_digest = SHA256(CBOR([
  "tickwire.business-event", 1, domain, type, payload
]))
logical_payload_hash = SHA256(CBOR([
  "tickwire.logical-payload", 1, writer_exec_epoch, business_digest
]))
genesis = SHA256(CBOR([
  "tickwire.logical-genesis", 1, domain, journal_epoch
]))
H_i = SHA256(H_previous || CBOR([
  "tickwire.logical-record", 1, domain, journal_epoch,
  journal_seq, type, logical_payload_hash
]))
```

业务摘要绑定事件类型、全部业务字段、风险域和原订单执行代次，排除写入者代次、日志代次与日志位置。同 intent_id 的重试若改变原订单代次、config_revision、deadline 或其他业务字段，也会得到不同摘要；接管后重新记录同一旧订单事实则保持摘要。

逻辑链采用 [replication profile 的逻辑数据链](../../specs/contracts/replication-profile-v1.json)，其中公式的 `payload_hash` 明确取 `logical_payload_hash`，从而绑定写入者代次；再绑定日志代次和位置。物理 LSN、批次与分段不参与这些哈希。未来证明接口需分别引用业务内容摘要和逻辑载荷摘要，不能将二者当作同一字段互换。

原始流从 journal_seq 1 开始；codec 本身能保留任意 u64，连续性由链检查。重复回报可占新的 journal_seq，经济去重由业务内核处理；重复或跳过已有日志位置则拒绝。当前链仅支持同一 journal_epoch 从 genesis 重放。主设计要求正常 epoch 提升保留已验证前驱并继续序号，该转换留给后续证明层，不在此接口中重置序号模拟提升。

物理 WAL 和此 codec 各自校验自己的字段；未来适配器还需比对 domain、journal_epoch、journal_seq、writer_exec_epoch，验证依赖闭包及恢复冻结，再形成业务提交证明。这里没有 `durable=true` 一类可由调用者构造的授权标志。
