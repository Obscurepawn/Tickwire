# 持久输入幂等验证

codec-red.log 保存编译成功后的tag9行为失败；gateway-red.log 保存6项可编译接口失败。实现后 native-targeted.log 的10项测试通过：codec2项、Journal2项、Gateway6项。

真实 FileBackend 故障测试分别注入半条追加与同步完成但回执丢失。两者均不提前发布候选账户或输入索引；重开后，半条追加回到旧前缀再补交，完整尾部则命中原输入返回 Duplicate。重复请求不推进账户序号、账户日志位置或历史链。另一项直接通过底层 Journal 写入相同输入键的第二条完整记录，要求 AccountJournal 恢复拒绝。

冲突输入停止发送；无有效订单的事件只冻结账户且不写账户 WAL。显式缺口冻结后，已知订单事实仍可处理；恢复后的账户继续冻结且没有发送连接。

native-codec-compat.log 保存原编码器测试及新tag9共22项通过。legacy-codec.log 实际运行提交 `d792e8058a52151056155b68f01460442c9ddaef` 的 account/account-codec 源码，导出全部8类记录；legacy-records.hex 固定完整字节，native-legacy.log 与当前实现逐字节比较。旧标签没有换schema或重新计算历史字节。

native-clippy-initial.log 的失败来自测试中不必要地替换 Box 分配，改为修改 Box 内容；生产实现未因此改变。最终Clippy见native-clippy.log。源码与测试版本见source-hashes.json。

risk_core 独立只读审阅四crate增量，确认输入键纳入规范编码、首次同步后建索引、恢复拒绝物理重复键、候选账户提交顺序及事实在停止发送后继续处理，无开放阻断；未重复运行测试。输入日志连续前缀和原文持久化由上游输入所有者负责。旧账户、账户日志和网关模型只作锚点同步，保留原验证范围，未声称证明新tag9组合。
