# 非阻塞网络传输接口

`tickwire-transport` 用 mio 非阻塞 TCP 和 rustls TLS 连接承载 HTTP/1.1、WebSocket 与 FIX 字节流。地址解析、交易所鉴权、请求去重和订单状态由调用者负责。连接由一个事件循环持有，所有输出通过同一个有界发送槽。

## 发送与重新准入

```rust,ignore
Connection::connect(address, Security::Tls { server_name, config }, limits, deadline)
PreparedSend::new(bytes, deadline, max_bytes)
connection.queue(prepared) -> Result<u64>
connection.drive(now, IoBudget { max_syscalls, max_bytes }) -> Result<DriveReport>
connection.try_send(now, || fresh_gate()) -> Result<SendReport>
connection.send_report() -> Option<SendReport>
connection.take_finished() -> Option<SendReport>
```

`PreparedSend::new` 和 `queue` 只构造并保存报文。`drive` 负责 TCP 建连、TLS 握手、接收以及已有 TLS 密文的冲刷，业务明文入口集中在 `try_send`。`try_send` 每次只尝试一次有界明文写入。首次接受前同步调用授权闭包，若暂不可发送，下一次尝试仍需重新授权。闭包保持短小、同步，许可只在本次调用中使用。TLS 接受任意明文，或普通 TCP 接受任意字节，即记录 `accepted_bytes > 0`；后续失败保留这个事实。

`Queued` 表示仍有确切零接受证据。门禁拒绝、取消或期限到达会将该项永久变成 `NoBytes`；后续 `drive` 不能再发送它。接受后状态为 `MayHaveSent`，失败为 `Failed`。`Flushed` 的完成点是完整报文已交给本机 socket；订单结果由协议和执行层关联业务 ACK 后确定。终态可通过 `take_finished` 取走，连接错误则永久关闭当前连接。

`Limits` 限制单项报文、接收缓冲、TLS 发送缓冲和每次接受字节数。`IoBudget` 限制一次 `drive` 的读写/连接查询次数、网络字节以及交付到接收缓冲的解密明文字节；错误关闭只执行一次有界清理。连接尚未确认时需要至少 2 次查询预算。所有连接操作非阻塞。接收缓冲满时停止读 socket，读取已缓存数据后继续推进。调用者通过 mio 注册和更新兴趣，按就绪事件驱动；预算耗尽后仍有工作时，事件循环应再次调度，以免等待新的就绪边沿造成停滞。

`now` 和 deadline 使用同一进程的 `Instant`。方法会取 `now.max(Instant::now())`，授权闭包返回后再检查一次；截止保证针对检查点，不承诺网卡输出时刻。发送状态与接收结果独立，`drive` 报错或 `peer_closed` 后仍可调用 `read` 取出已缓存数据。完整冲刷在同一次调用后续 EOF 之前记录。

## 协议边界

- HTTP 请求编码拒绝 CR/LF 注入，自动添加准确 Content-Length；响应解析限制总字节、头部数量与消息大小，支持 Content-Length、chunked 和 EOF 定界；拒绝重复长度、TE/CL 歧义和不支持的 transfer coding。Host 由调用者提供。HEAD 响应通过 `next_head_response` 解析，Content-Length 只作元数据。每个响应最多 1024 个 chunk；头部/尾部各最多 256 项。
- WebSocket 握手校验状态码和 Upgrade/Connection/Accept，未协商压缩或扩展。客户端输出使用独立随机掩码。接收侧验证服务端无掩码、长度最短编码、控制帧、片段顺序与文本 UTF-8；Ping/Close 作为显式事件，由会话层准备回复，解析器本身不写网络。`poll_message` 每次最多处理 32 帧，返回 `Message`、`NeedInput` 或 `Yielded`。`NeedInput` 才表示等待新字节；`Yielded` 表示本轮预算用完且缓冲仍有数据，事件循环下轮继续调度。半帧也会让 `buffered_len > 0`，所以该长度不能代替三态。兼容方法 `next_message` 将后两种状态都映射为 `None`。`finish` 检查 EOF 前是否收到完整 Close。
- FIX 文本分帧检查 `8=...<SOH>9=...<SOH>`、BodyLength 和三位 CheckSum。会话序号、登录、重放和 FIX SBE 模板属于交易所协议层；`finish` 拒绝残缺文本帧。

TLS 使用调用者提供的根证书和正常主机名校验。首次应用发送只在握手完成后允许，禁用 early data。TCP 建连结果经 `take_error` 和 `peer_addr` 核实。握手流量不计作业务请求接受；业务 TLS 明文一旦接受，密文尚未写出也必须保留 MayHaveSent。

准备报文和解析消息会分配有界 Vec；`drive` 处理已经准备好的业务字节。连接的四个 Limits 各不超过 16 MiB；协议编解码单项上限为 64 MiB，进入连接前仍受较小的发送上限约束。`tls_buffer_bytes` 控制库的发送缓冲；完整连接内存还包括库管理的接收、握手和记录开销。

## 依据

- [mio TcpStream](https://docs.rs/mio/1.2.4/mio/net/struct.TcpStream.html)：非阻塞建连、就绪与连接错误检查。
- [rustls ClientConnection](https://docs.rs/rustls/0.23.46/rustls/client/struct.ClientConnection.html)：writer 明文接受、read_tls/write_tls 与缓冲上限。
- [RFC 9112](https://www.rfc-editor.org/rfc/rfc9112)：HTTP 消息长度和 chunked 分帧。
- [RFC 6455](https://www.rfc-editor.org/rfc/rfc6455)：WebSocket 握手、掩码、片段与控制帧。
- [FIX Transport 1.1](https://www.fixtrading.org/wp-content/uploads/download-manager-files/FIX_Transport_1.1.pdf)：BodyLength 与 CheckSum 的计算范围。
