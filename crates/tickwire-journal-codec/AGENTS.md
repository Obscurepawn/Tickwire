# tickwire-journal-codec

## 1. 职责（架构位置：持久化恢复链 —— 确定性业务事件 CBOR 与逻辑哈希链）

独立、无持久化授权的编解码 crate：把 5 类业务事件（PreparedIntent/Fill/Fee/Terminal/AbortedBeforeSend）编码为 schema2 确定性 CBOR，计算稳定业务摘要与逻辑数据链。不含网络、经济校验或持久化权威。lib 根文档：`Deterministic business-event CBOR and logical hashing, without durability authority.`

## 2. 公共 API 入口

- `encode(record: &Record) -> Vec<u8>`、`decode(bytes) -> Result<Record, DecodeError>`；`SCHEMA_VERSION=2`、`MAX_ENCODED_EVENT_BYTES=236`。
- 类型：`Product::{Spot,UsdM}`、`CommitProfile::{LocalDurable,SyncReplica}`、`TerminalStatus::{Filled,Canceled,Rejected,Expired}`、`OrderIdentity{venue,account,product,intent_id,intent_seq,intent_exec_epoch}`、`PreparedIntent{order,command_digest,quantity,unit_budget,fee_budget,strategy_generation,config_revision,deadline,recovery_object,commit_profile}`、`Event::{PreparedIntent,Fill{trade_id,quantity,cost},Fee{fee_id,amount},Terminal{status,cumulative},AbortedBeforeSend}`、`Record{risk_domain_id,journal_epoch,journal_seq,writer_exec_epoch,event}`。
- 摘要：`business_digest(record)`（绑定域/类型/业务字段含原订单代次，不含 writer 代次与日志位置）、`business_payload(record)`（business_digest 的规范原像，可作不可变意图载荷依赖对象）、`logical_payload_hash(record)`（再绑定 writer_exec_epoch，逻辑链 payload_hash 输入）。
- 链：`LogicalChain::new(domain, journal_epoch)`、`append(record) -> Hash`、`verify_and_append(record, expected)`、`last_seq()`、`hash()`；`DecodeError::{TooLong,Truncated,NonCanonical,UnexpectedType,Schema,Field,Enum,IntegerOverflow,TrailingData}`、`ChainError::{WrongDomain,WrongJournalEpoch,Sequence,SequenceExhausted,ExecutionEpochRollback,HashMismatch}`。
- 私有模块：`cbor`、`chain`。无 bin/example。

## 3. workspace 位置

上游：无 `tickwire-*` 依赖（仅 `sha2`）；dev：`serde/serde_json`。

下游（grep 命中）：`tickwire-business-journal`、`tickwire-gateway`。

## 4. 对应规格

- [../../specs/implementation/journal-codec.md](../../specs/implementation/journal-codec.md)（schema2 信封、原订单代次与 writer 代次分离、业务摘要/逻辑载荷摘要、LogicalChain 规则；旧 schema1 201 字节上限与历史测试记录）
- [../../specs/implementation/command-binding.md](../../specs/implementation/command-binding.md)（命令绑定 schema2 信封）

## 5. 构建与测试

```sh
cargo build -p tickwire-journal-codec
cargo test -p tickwire-journal-codec
```

无 bin/example。`tests/codec.rs` 使用签入 golden：`golden-v2.json`（6 个事件向量、17 个 RFC 整数边界向量、maximum 边界）与 `golden.json`（旧 schema1，仅用于拒绝旧格式）。测试包含：全前缀截断拒绝、尾随数据/超长拒绝、基于 golden 字节（而非 Rust 编码器）的单字节变异全拒绝、整数边界往返、golden 接管与逻辑前缀、链序号耗尽不回绕且不改状态。

## 6. 关键不变量与注意事项

- **原订单代次与 writer 代次分离**：`Record.writer_exec_epoch`（信封，当前写入者）与 `OrderIdentity.intent_exec_epoch`（原订单代次）分开保存；`business_digest` 不含 writer 代次，移动日志位置/更换写入者业务摘要不变；`logical_payload_hash` 才绑定 writer_exec_epoch。
- **稳定业务摘要**：`business_digest` 绑定风险域、事件类型与全部业务字段（含原 intent_exec_epoch）；同意图重试改 deadline/config_revision/保留资金会改变摘要，换 writer/移位置不改变。
- **逻辑链**：仅从原始流 genesis（`tickwire.logical-genesis`）开始；域/日志代次变化、序号重复/缺口/耗尽、writer 代次回退、期望哈希不符都报错且保留旧状态；`verify_and_append` 先在候选副本计算，哈希相等才发布。
- **规范编码**：显式 CBOR major type、网络字节序整数、固定 enum 值，无 Rust 内存布局/Debug 依赖；解码器逐字段核对，不跳过未知内容，不按不可信长度分配内存。
- **i128 保真**：所有 i128（含最小值）可保真保存，业务非法数值留给 reducer 判断；负整数幅度 `!value`，大整数仅接受 tag2/3、9–16 字节非零开头、幅度 ≤ i128::MAX 的唯一表示。
- 重复交易结果可占新日志位置，结果去重由执行内核负责，本链不去重。

## 7. 改动检查清单

1. `cargo build -p tickwire-journal-codec && cargo test -p tickwire-journal-codec` 通过。
2. 信封/事件格式改动联动 [journal-codec.md](../../specs/implementation/journal-codec.md) 与 [command-binding.md](../../specs/implementation/command-binding.md)，并更新 golden 向量（6 事件/17 整数）。
3. 链规则改动核对 `tla/execution_models/business_journal/L1_command_binding`。
4. 本 crate 不做授权、持久化或经济校验。
