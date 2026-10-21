# 控制会话与请求登记实现规格

实现位于 `crates/tickwire-control`，禁止 unsafe。接口设计见 control-session-interface.md。

## 会话身份与协商

`Policy` 由可信 launcher 提供：本端与对端的 instance、generation、角色、logical strategy、host boot、namespace、架构、trust profile 和发布文件哈希；`ProcessIdentity` 是来自内核的对端 PID/UID/GID。launcher 持有活进程句柄，防止注册生命周期内 PID 被重用，并为每次新会话分配未使用的 epoch/nonce。本库不自行从 HELLO 建立注册表。

服务端先比较内核身份，再验证 HELLO 的完整登记身份、ABI 1.0、页/隔离/原子宽度及必需特性。特性位 2/3/4 是当前协议配置的必需能力，配置控制角色还需位 5；未知必需位拒绝，已知共同能力写入 WELCOME。客户端核对内核对端、request ID、双方 instance、epoch、host boot、角色、trust、nonce、能力和期限。

帧使用现有 128 字节 envelope 和 CRC，长度最多 65536 字节。会话帧逐个检查身份、epoch、请求 ID、协商特性与长度；HELLO/WELCOME 不能重复作为已建立会话内操作。控制命令业务权限、报文 body 与资源绑定由其 owner 在此后继续检查。

时间由同一主机、同一 Linux time namespace 的 CLOCK_MONOTONIC 提供。实际 `now >= deadline` 或时间倒退会永久关闭实例；后续调用不能续租恢复。续租 nonce 必须匹配，revision 必须更高且小于 2^63。新的期限算术越界拒绝，保留旧期限。`encode` 只产生字节，接收方仍在处理时核验期限。

## 请求登记

`Session.admit` 验证会话后，以认证主体/epoch/request ID 查存储，SHA-256 摘要绑定 opcode、operation ID/revision、expected version 与 body。已有同摘要请求先返回 Pending 或准确的完整终态响应，不受后来的 owner version 影响。摘要不同拒绝；仅首次请求比较 expected version。

`RequestStore` 提供 lookup、原子 insert-if-absent 和不可覆盖 completion。存储确认首次 Received 后才交付不可克隆的 ExecutionTicket；Pending 重试不再交付票据。`complete` 校验双方身份、请求/operation/revision、预期版本、对应响应 opcode；普通 StatusBody 中的 state_version 必须等于保存的版本。响应只写入一次，重复相同内容成功，不同内容冲突。

MockRequestStore 总容量可配置，未完成记录最多 256 条，不为腾出空间清理历史结果；fail_next 在下一存储调用进入时返回错误。它可跨 Session 值重建复用，不能跨进程重启持久保存。生产实现应先持久提交再成功返回；含副作用的 owner 还需独立事务/恢复协议与经济 operation 去重。丢失未完成票据时需要 owner 查询实际状态，不能重新执行。

## 验证证据

control-session 验证目录 保存初始编译 Red、完成响应语义 Red 和后续 Green。Rust 覆盖身份、协商、期限、重复/冲突、容量、存储失败和响应关联。

TLA+ 阶梯位于 [control_requests](../../tla/execution_models/control_requests/PLAN.md)：L0 转录串行请求登记与 Mock 故障，L1 再组合期限、续租和永久关闭。每级结论以对应输出文件和实际 TLC 日志为准；未实现的持久 owner 恢复保留为 L2。
