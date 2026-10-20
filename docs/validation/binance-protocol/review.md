# Binance 协议独立审阅

审阅者为并行维护传输层的 `ipc_codec`，只读核对 `auth.rs`、`params.rs`、`request.rs`、`response.rs` 及协议服务器测试。审阅不修改被审源码。

核对内容包括：REST 签名原文与实际 query 字节、WS 参数排序与 JSON 字符串值、账户/产品/环境绑定、完整命令摘要、响应分类、首次接受应用字节前的门禁和后续分段写出。未把缺少会话协调器的实现计为完整连接器。

审阅发现 `fix_logon_preimage` 接受 `BAD!`、`A=B`、`A:B`。审阅者用临时 Rust 程序链接当前库实际复现；固定官方 `fix-api.md` 的 Header 49 限定 SenderCompID 为字母、数字、连字符、下划线，长度 1–8。作者随后加入[实际失败断言](fix-sender-red.log)，收紧字符集并验证错误时保持输出原值。[回归日志](review-green.log) 记录 15 项原语测试及两项真实网络集成通过。审阅者复读修复后确认对应官方限制。

HTTP 408 与未知错误码默认值由作者核对官方文档发现，审阅者也独立确认原默认 `Rejected` 不够保守。修复采用显式确定性拒绝码表，其余保持 `Unknown`；[失败记录](unknown-code-red.log) 和最终 `green.log` 保留前后证据。审阅者复读最终分类逻辑后未发现剩余阻断项。

本次同时审查了传输 WS 分片预算的调用契约：32 帧预算耗尽时需要继续调度，不能把它当成等待新网络输入。传输层增加 `poll_message` 的 `Message/NeedInput/Yielded` 三态，本模块集成测试已使用新接口；该修复的源码和测试证据归传输模块管理。
