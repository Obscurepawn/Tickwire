# 网络传输实现规格

实现位于 `crates/tickwire-transport/`，接口见 [传输接口](../../docs/implementation/transport-interface.md)。mio 1.2.4、rustls 0.23.46 固定于 manifest；生产路径没有阻塞 connect、read、write 或等待 future。地址解析在连接之前完成，外部 reactor 管理就绪和再次调度。

## 可验证行为

| 行为 | 实现要求 | 回归证据 |
|---|---|---|
| 排队 | 唯一发送槽保存不可变 bytes，不进入 TLS | queued_drive、unready_queue |
| 首次输出 | Ready 且未过期，紧接 writer.write 前同步检查门禁；闭包返回后复查截止 | queued_deadline、gate denial、TLS 接受测试 |
| 不确定发送 | 接受任意应用字节后，失败只能保留 accepted 并报告 Failed；取消关闭 TLS/socket | partial_tcp、TLS cancel/deadline、实际 backpressure |
| 本地完成 | 全报文已接受且 TLS 待写缓冲清空才 Flushed；同次 EOF 不覆盖 | HTTPS/FIX 真实 TLS 集成 |
| 有界推进 | 网络调用/字节、解密交付量有每次上限；接收容量满后停止读取 | bounded_receive、decrypted_plaintext |
| TLS 验证 | 根证书和主机名校验正常启用，拒绝 early data 配置 | 私有测试 CA、错误主机名 |
| HTTP | CR/LF 注入、重复长度、TE/CL 歧义、超长 chunk、截断拒绝；HEAD 长度不分配 body | framing.rs、真实 HTTPS chunked |
| WebSocket | 校验 Upgrade/Accept，客户端随机 mask，服务端帧标志/最短长度/控制帧/片段/UTF-8 校验；显式区分消息、缺输入和处理预算耗尽 | framing.rs、33 帧与半帧对照、真实 WSS 升级与数据 |
| FIX 文本 | BodyLength 定位 CheckSum，错误校验和、残帧和超限拒绝 | framing.rs、真实 FIX over TLS |

准备和解析可以返回有界分配的数据，TLS 握手也可能发送协议字节；这两者均不等于排队订单已被接受。业务 ACK、venue 错误、重连、FIX 登录/序号/重放、WS heartbeat/Close 回复由会话层完成。NoBytes 只有当前发送槽零接受时才成立，报文 ID 在连接内递增；新建连接时由上层提供跨连接业务身份。

[实际测试记录](../../docs/validation/transport/README.md) 保留首轮 Red、实现过程中的真实边界失败、最后的 native/Linux Green 和源码摘要。[L0 模型](../../tla/execution_models/transport/L0_first_output/05_output.md) 检查首次接受、过期和历史 NoBytes 不复活；它不验证 TLS 密码学或硬件发送截止。
