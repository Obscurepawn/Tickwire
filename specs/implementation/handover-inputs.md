# 交接账户输入完整性

交接闭包中的 `TWINP001` 必须保存真实账户 callback 原文。单独匹配的账户 `InputSeal` 只能核对累计摘要，不能代替缺失的基线或记录。

`tickwire-handover` 统一执行以下规则：

1. 检查 `HandoverSeal` 与实际 checkpoint、输入、账户封口的长度和摘要；checkpoint 绑定目标实例、generation、逻辑策略、报告切点、ledger 和订单摘要。
2. 输入从序号 1、零前置摘要开始；每条 kind 2 严格解析为 `TWAI0001`。基线恰好一次，并位于所有账户记录之前。wrapper 长度、标签和序号均核对。
3. 每条记录必须通过规范账户解码，与基线绑定，且从 1 连续到报告切点。重新计算的账户封口原像必须等于闭包保存的原像。
4. 从实际基线与全部记录恢复账户，恢复出的 ledger 和策略订单摘要必须等于交接声明。
5. runtime 另外逐字节核对实际基线和 prior WAL；SDK 继续核对认证 READY 的目标身份与报告切点；replica 继续核对源流、交接记录和此前已验证记录摘要。三处保留原有对象及依赖总预算，runtime 预算也计入完整输入对象。

记录容量使用账户日志既有 `MAX_FACTS + MAX_ORDERS` 上限。其他输入类型保留原顺序，由各自回放器解释。共享返回类型仅允许校验成功后构造，不授予交易权限。

两项实际 Red 已确认旧 runtime 会接受重新计算哈希后的缺基线、缺账户记录输入；修复后均拒绝。测试与范围见 `docs/validation/handover-input-closure/README.md`。
