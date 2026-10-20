# 反例与审阅结果

本级没有 TLC 属性反例。第一次四配置运行中，精确回收重试的量词只从当前 applied 取值；独立审阅指出，Reader 在 Quiesced/Detached 后仍允许应用已读记录，因此历史 ACK 的 applied 可以落后当前值。最终模型增加独立历史重试入口并重跑四配置。这是模型输入覆盖修正，不是 Rust 缺陷。

实现期间两项 Rust Red/Green 来自源码审阅：重复 JOIN_READY 在发布新记录后错误地重新检查追平；已回收订阅的冲突重复 Quiesce ACK 未与历史完整正文比较。现在分别返回首次激活水位、比较完整历史 ACK。原始失败/修复日志位于 [JOIN 验证目录](../../../../docs/validation/join/README.md)。
