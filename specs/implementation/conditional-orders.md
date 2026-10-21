# 条件订单实现约束

接口见 条件订单的持久结构与责任。

## 第一阶段

1. 原始 SpotList/Algo Admit 唯一决定父身份和最多三个潜在腿，父单不重复占额；每条腿保留原订单风险预留。
2. 规范 Structure 事件使用事件 schema 2/tag 5，原 schema 1/tag 1–4 字节不变。codec 验证原 intent/epoch、产品、父身份、唯一子单和有界数量。
3. 父确认/触发/终态/未知单调保留。父终态不修改子单终态、成交量、费用完成或预留。晚到子单只绑定已准入腿。
4. 父 ACK 禁止后续 Abort；已 Abort 后出现父 ACK 会重建各腿责任并冻结。native 父/子冲突保留原状态并冻结，原事件可供解释。
5. CancelList/CancelAlgo 校验完整作用域、标的、原父身份、所有腿当前 manager/revision；发送前重新检查，不复用移交前管理资格。
6. 精确重复父状态不推进账本。恢复重放相同事实，合成冻结/Unknown 不改变持久摘要。

## 状态摘要

未出现结构事件时，既有账户输入状态摘要和策略订单摘要保持 v1。第一次接受结构观察后单调升至 v2：账户输入摘要增加区段 11 的 32 字节结构摘要；策略订单摘要增加该策略的结构摘要。

结构摘要为 SHA-256，原像以 `tickwire.conditional-state.v1` 开头，随后是 u64LE 结构数量。父结构按内部 16 字节 ID 排序；每个结构依次写内部 ID、原 intent epoch/id、instrument ID、kind（OCO/OTO/OTOCO/Algo=1/2/3/4）、命令摘要、父 client ID、可选父 native ID、confirmed/triggered/terminal/unknown 四个单字节、腿数及原命令顺序的腿 key、子单数及按原 client ID 排序的子单绑定。整数都是 u64LE，文本为 u64LE 字节长加 UTF-8，可选文本先写 0/1。腿 key 为 intent epoch/id + client 文本；子单为原 client 文本、内部子 ID、可选 native ID、可选交易所 child client ID。结构数受 MAX_ORDERS 限制，腿和子单最多 3。原命令身份和子单完整父引用已由事件校验固定。

首次保存未创建闭合后，账户输入与策略订单摘要升至 v3，内部结构摘要改用 `tickwire.conditional-state.v2`。每条腿 key 前增加 uncreated 单字节；腿列表后增加 closure-present 单字节，存在时写 coverage/history/state 三个 32 字节摘要及原 ledger u64LE。其他字段顺序保持，旧 v1/v2 原像不变；闭合后迟到子单重建责任时仍保留闭合历史和 v3。

证据见 第一阶段记录。

## 第二阶段

私有流父结构和实际 REST/WS 父响应经过原命令、实际会话作用域与注册表绑定，输入先同步，再发布账户记录。Spot members 不形成各腿状态；只有真实 orderReports 形成订单观察。USD-M native child 仅由已知父实例绑定，先到未知 child 隔离。

空 actualOrderId 同时带非零触发字段或 actualType 时为 Unknown。native ID 在保留历史中不可重新归属。输入先同步而账户撕裂/丢同步回执时恢复补写相同事件一次，父终态始终不释放腿。

迟到 ACK/成交/费用否定此前 Abort 时，从原持久 OrderSpec 的明确减仓属性和 `original-filled` 恢复独占票据；实际新成交继续减掉本次数量。账户保持冻结，重新激活仍需完整外部对账。不得按回报里的数量、方向或模式授予新的额度。

## 第三阶段

`ReadOnlyStructureReader` 按实际原命令查询 Spot `/api/v3/orderList` 或 USD-M `/fapi/v1/algoOrder`，共享额度使用独立 `StructureQuery` 规则。输入 tag 21 保存实际 parent query、ClockDomain 和原响应；重开重新解析原始 HTTP 内容并比对规范事件。旧 REST 输入标签不变。

已创建的腿使用各自 native ID 查询普通订单和从 0 开始的连续逐笔成交页。只有精确累计成交与全部页一致才写 FeesReconciled。父 ALL_DONE/FINISHED、撤父成功和成员列表均不替代这些记录。Algo 的 trade query 从原潜在腿取得 side/position，严格核对已绑定的 native parent/child；不按父 actualQty 构造成交。

账户 tag 16 `UncreatedAlgoClosed` 为固定 13 项：scope、account_epoch、ledger_sequence、baseline_digest、history_digest、input_stream、through_sequence、state_digest、coverage_digest、structure_id、intent、command_digest、venue_parent_id。普通 `record_fact` 拒绝提交；专用 gateway 入口核实际 verifier 后采用 candidate → WAL sync → publish。精确历史重试先查询原记录，冲突冻结。

closure object 的确定性 CBOR 固定 25 项，按以下次序编码：purpose bytes `tickwire.binance.uncreated-algo-closure.v1`、schema 1、scope 四元组、account_epoch、ledger_sequence、baseline_digest、history_digest、input_stream、through_sequence、state_digest、structure_id、intent 二元组、command_digest、native parent UTF-8 bytes、account_stream、input-prefix digest、writer_epoch、private_epoch、gap、clock boot、clock generation、evaluated、expires、parent reference、三 snapshot references。每个 reference 为 `[sequence, byte_length, sha256]`；最大 4096 字节，只接受最短规范 CBOR 并消费完整输入。

原查询须明确 CANCELED/EXPIRED/REJECTED，actualOrderId 空字符串，actualType 缺失或空，actualQty 缺失或零，actualPrice 和 triggerTime 均存在且为零。所有保留历史均无子单、触发或成交。核对 scope/baseline/真实 HistoryFrontier/ledger/state/实际输入前缀和原物理 writer，且查询早于三份快照并在同一有效 cut 内。多个未创建父单各凭自己的查询构建临时快照投影；实际 tag 16 只关闭一个父单，下一父单要求新三快照。

关闭不改变现金、仓位或 venue shadow，只撤下已证明从未创建的潜在腿责任；费用关闭、uncreated 和原 closure 进入持久状态，账户保持冻结。迟到子单撤销 uncreated，按原持久 OrderSpec 恢复剩余责任和独占 reduce 票据，保留原闭合证据并冻结。

文件恢复通过真实闭包对象及完整输入 WAL 重验 tag 16，缺对象拒绝。复制附带该对象与完整输入前缀，核四个引用的实际长度/摘要和 prefix digest；handover 重放相同 CanonicalRecord。离线报告将 uncreated 单列，不再将该潜在腿报告为开放订单。Controller 在 tag 16 后清空旧三快照，重新查询并持久 Reactivated。
