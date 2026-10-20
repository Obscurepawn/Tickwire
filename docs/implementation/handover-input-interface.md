# 交接输入闭包校验

`tickwire-handover::verify(seal, checkpoint, inputs, account_input)` 读取已有的 `HandoverSeal`、`TWHS0001`、`TWINP001` 和账户 `InputSeal` 格式，不增加第二套持久记录。成功后返回私有构造的 `VerifiedHandoverInputs`，只提供基线、规范账户记录及 `matches_history` 只读接口。

校验要求输入从序号 1 和零前置摘要开始。每条 kind 2 输入都必须是完整 `TWAI0001`：先有且仅有一条基线，随后账户记录从 1 连续到交接切点，wrapper 序号与规范记录序号一致。校验器从这些实际字节重算 `InputSeal`，再调用 `Account::recover` 核对账本序号和策略订单摘要。缺少 callback 不能由单独保留的摘要对象替代。

runtime、SDK 和 replica 共用此校验器。各调用者继续检查执行者身份、交接目标、对象来源和依赖总预算；runtime 还通过 `matches_history` 将基线及每条规范记录与实际账户 WAL 字节逐一比对。镜像将规范记录摘要与已验证物理前缀比对。其他输入类型保持原顺序，仍由各自回放器解释。

对象按现有 64 MiB 上限读取；账户记录数不超过账户日志既有容量。返回值证明交接数据可重放，不提供策略授权，也不证明策略代码已经执行这些 callback。
