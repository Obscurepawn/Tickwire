# 业务日志与恢复运行时契约

## 完成点

`BusinessJournal::commit_intent` 只有在真实物理批完成同步后返回 `WalAppendReceipt`。收据构造私有，表示该业务记录被某个物理同步前缀覆盖，不代表策略授权、复制确认或完整 LocalCommitProof。

每条业务事件占一个物理 DataRecord，采用既有 codec v1 的确定性 CBOR。业务 `journal_seq` 等于物理记录序号；`journal_epoch/risk_domain_id/writer_exec_epoch` 分别匹配物理流、域和写入代次。逻辑链从 genesis 全量重算，无公共 reset 或跳过中间前缀接口。

业务 bootstrap 在创建时固定 codec/schema、物理配置、venue/account/product 和初始资本。恢复要求文件内容等于受信调用方预期配置的规范编码。校验摘要不能取代外部可信资本或防回滚来源。

## 意图与事实

新 PreparedIntent 的原订单代次等于当前 writer 代次。相同完整 `DurableRecord` 重试返回原业务位置及逻辑哈希，即使重开时 writer 代次已提高；返回的物理前缀可以更晚。同一 intent_id 或 intent_seq 绑定不同内容时拒绝，Journal 进入 poisoned 状态。

事实保留原订单 intent_id/intent_seq/intent_exec_epoch，信封绑定当前 writer。runtime 在应用前与原始 DurableRecord 比较完整身份。Fill/Fee/Terminal 先同步再交给 reducer；即使内容冲突或算术失败，也保留已接收事实并冻结。未知或错配身份会阻止后续恢复完成。

NoBytesAccepted 必须来自本次受信 Exchange 调用。runtime 先同步 AbortedBeforeSend，再调用 reducer 释放预留。写入、同步或应用失败都冻结，失败前不提前释放。外部 `record_fact` 提交 AbortedBeforeSend 会被拒绝。

## 发送权限

Runtime 私有拥有 engine/journal，不提供可变引用。`CommitAuthorizer` 是受信授权服务，只提供 trait，没有无条件通过的生产实现。

1. 预留前重新读取 gate。
2. 授权服务返回私有构造的 VerifiedClosure；runtime 逐项比较完整 ClosureBinding。
3. 同步 PreparedIntent。
4. 重新读取 gate，再由 reducer 检查 writer 代次、策略代次、配置版本、期限、租约和有效性。
5. `begin_send` 先记录 MayHaveSent，再调用 Exchange。

任何授权、WAL 或最终门失败都保留现有预留并冻结。PendingResponse 保留未知发送占用；MayHaveSent 还冻结后续增险。当前实现只接受 LocalDurable，SyncReplica 不降级。

## 恢复

静态 `Runtime::recover` 创建新 engine，并在读取日志前冻结。它按统一业务流原顺序重放，每个 PreparedIntent 通过 `verify_recovery` 重验已有闭包；恢复不发布缺失依赖、不调用 Exchange、不开放解除冻结。

空日志也恢复为冻结。闭包缺失、身份错配、非法事实和算术错误返回失败，不发布部分恢复的 runtime。成功恢复的资金状态反映按序应用的持久前缀。

物理层允许丢弃最后未封口批，完整已同步前缀必须保存。外部 `PhysicalFrontier` 可以要求一个精确前缀；它仍需来自独立可信来源。没有该来源时，单文件自描述不能识别存储介质无声删除整个已确认尾部。

## 验收

历史验证覆盖接口桩行为 Red、接管幂等 Red、等待期间过期 Red、native/Linux Green、格式/Clippy 和源码摘要（原始日志曾存于已删除的 `docs/validation/business-journal`）。真实文件测试包括 Prepared/Aborted 同步失败、部分 Aborted 写入、两个子进程 SIGKILL 边界以及相对目录打开后 CWD 改变。

TLA 的 business_journal L0 检查新增完成顺序；交易结果记录组合与跨 writer 身份模型留待后续层。签名证明、镜像确认、真实策略授权、可信资本、防回滚和掉电实验不在本契约已完成范围。
