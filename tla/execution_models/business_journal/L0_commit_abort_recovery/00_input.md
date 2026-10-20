# L0 输入与范围

验证新 `Runtime` 将物理批同步接到业务发送与中止释放的完成顺序。

源码锚点：

- `crates/tickwire-business-journal/src/runtime.rs:25-169`：初始化、恢复、预留、闭包、提交、刷新门、发送与零字节分支。
- `crates/tickwire-business-journal/src/runtime.rs:171-178`：外部中止事件被拒绝。
- `crates/tickwire-business-journal/src/lib.rs:218-326`：幂等提交与同步后发布。
- `crates/tickwire-execution/src/lib.rs:276-345`：资金预留与发送前门检查。

持久化故障采用既有物理 WAL 的契约：完整同步前缀不丢失，正在写入的最后一批可以丢失或完整存活；恢复先验证并重新同步完整前缀。未封口尾部丢弃对应模型中的“不保留最后事件”。同步成功后媒体无声删去整尾、任意篡改、丢失已经验证且永久 pin 的闭包，不属于该故障集合。

本层只允许 PreparedIntent/AbortedBeforeSend 两种事件。资金预留单位固定为 1，每个意图至多一次传输调用。Fill/Fee/Terminal 的日志顺序由 Rust 回归覆盖，经济迁移的组合验证留到 L1。
