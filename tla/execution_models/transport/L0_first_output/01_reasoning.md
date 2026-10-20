# 状态和抽象

一个 `Connection` 同时至多持有一个发送槽，所有方法通过独占可变借用串行调用。模型允许门禁在每次调用之间撤销；首次授权和接受在一个动作内，未保存跨调用的许可。授权闭包内部耗时越过期限另列 `ExpireInGate`，对应源码再次读取 Instant 后拒绝。

`accepted[i]` 是第 i 个发送槽已交给 TLS writer 或普通 socket 的应用字节数。`wire[i]` 是已冲刷至本机 socket 的相应前缀见证，不表示对端接收、业务 ACK 或持久性。`buffered` 抽象 TLS 尚未冲刷的应用前缀；`tlsBlocked` 表示尚未排空的握手/其他 TLS 记录占满发送缓冲。在 TLS 配置中可独立触达零接受、接受后仍未输出、部分冲刷和完整冲刷。普通 TCP 接受同步增加 wire。

容量以可接受的应用字节为有限单位，TLS 记录头和加密开销不逐字节建模。真实 rustls 的缓冲上限以内部记录计，故本级只检查接受证据和输出顺序，不推导精确内存使用量或吞吐。`TryZero` 把 writer 返回 0 / WouldBlock / Interrupted 合并成无状态变化，保留再次尝试时重新检查门禁的调用边界。

时间只取有限单调数值；每个报文独立选择截止时间。`ExpireQueued`、`ExpireAccepted` 对应下一次 drive/try_send 观察到截止，而 `ExpireInGate` 对应闭包返回后的再次截止检查。连接关闭是永久状态；取走 NoBytes 后仍可复用未关闭连接，取走 Flushed 后可排队下一项。取走失败项不会重开连接。历史 report 和 wire 是模型见证，用于检查先前 NoBytes 不会在后续槽位中重新输出。

建连细节抽象为 TCP Connecting、TLS Handshake、Ready。证书失败和网络错误归约为 `FailConnection`，不把失败伪装成新的输出。数据接收、HTTP/WS/FIX 分帧和内核队列不影响本级发送槽的准入 guard，因此本级省略这些内部状态；它不证明接收完整性、密码协议或无条件活性。

Fresh 表示方法检查时刻尚未到期。一次 drive 的预算循环和实际 write 调度可以消耗墙钟时间；本级不证明网卡输出时刻早于截止。握手占位冲刷与零接受过期可以交换顺序，因为二者都不接受应用报文；这项合并只适用于本级的应用接受证据。
