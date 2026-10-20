# Binance 私有流会话规格

实现：[tickwire-binance-private](../../crates/tickwire-binance-private/src/lib.rs)。公共签名及官方协议依据见[接口文档](../../docs/implementation/binance-private-session-interface.md)。

## 身份和状态

一个 `PrivateClient` 固定产品、环境、账户、risk domain 和凭据代次，独占数据连接与 USD-M 管理连接。请求关联键为连接代次、请求序号和操作；请求摘要同时绑定完整账户 scope。成功创建新连接会消耗新代次，失败尝试也不复用。计数或截止计算溢出使 owner 进入 `Exhausted`，清理连接和待请求并撤销接收资格。

Spot 只有精确关联的订阅成功回复才能建立 `Receiving`。登录模式先检查 logon 的 API key，再发送 subscribe；签名订阅直接认证并建立订阅。subscriptionId 0 合法，事件 ID 与当前订阅精确匹配。状态查询仅核对已确认身份，不替代订阅确认。

USD-M POST 取得 key 后完成 `/private/ws/<key>` Upgrade，才建立 `Receiving`。POST/PUT 成功将本地期限设为请求开始时刻加 60 分钟；PUT 同时要求旧 key 尚有效、请求未超时、返回 key 未变化。迟到结果不能重新激活已过期流。重新 POST 即使返回相同 key，也使用新连接代次。

任何流缺口先撤销接收资格，再交付终止事件或旧连接已完整收到的事实。EOF 后旧事实带原 SessionContext 排空，期间不开始新连接。`Receiving` 只表示当前流可接收，账户准入仍由持久化和对账协调器管理。

## 请求输出和时钟

`prepare` 生成私有、不可修改的请求字节。HMAC、RSA、Ed25519 签名订阅与 Ed25519 logon 使用同一参数表示签名及输出；recvWindow 的小数尾零在签名前规范化，防止 JSON 数值重编码改变签名原文。USD-M key 管理请求仅有固定路径、方法和 API key 头，无 query/body 参数。

首次接受应用字节前的同步顺序为：

1. 采样时钟，核对固定 ClockDomain、时间未回退、原请求 Source/签名时间和请求截止。
2. 调用必填 `Clock::check_signed`，适配器以原始 `SignedTime` 检查校准区间和剩余交付预算。
3. 调用 `ControlPermit`。
4. 再采样并重复时钟及签名检查。
5. 最后一次 `check_signed` 返回后再次采样，核对域、顺序、原请求 fresh 和本地 deadline；其后只有纯检查，直接由 transport 尝试写入。

未接受字节的 WouldBlock 重试会重新执行上述检查。`drive` 不会首次接纳排队应用请求。原始 stamp 保存在 PreparedControlRequest 中，旧请求不能用新的时间戳或新的 ClockDomain 重新绑定。`sample` 或 `check_signed` 返回 ClockDomain 错误，与返回新域的样本一样永久停止当前 owner。

`NoBytes` 只用于应用字节从未被 TLS 接受的请求。部分接受、完整本机输出后断连、响应不确定或匹配回复过早到达均保留 `MayHaveApplied`；匹配回复先核 `Pending.complete` 再消费请求，避免丢失部分接受证据。`Confirmed` 由完整协议回复建立；本机 `Flushed` 不直接推进认证或续期。

## 边界、调度与关闭

默认请求上限 16 KiB、单消息 1 MiB、提前事件 32 条/1 MiB、协议控制队列 8 帧。所有配置先校验正数及最大容量。每次 poll 分别限制网络 syscall/bytes、消息和控制请求工作量；底层失败时未知的 I/O 消耗标记为预算耗尽，本轮不继续其他连接 I/O。

`poll_message` 的 Yielded 与 NeedInput 分开调度。已缓存的 33 片以上消息可在下一轮继续解析，不等待新的网络输入。Pong 复制 Ping 载荷并优先于尚未输出的应用帧；已部分输出的帧连续完成。Close 帧有明确回送与本地完成边界。

ControlPermit 覆盖连接尝试、Upgrade、API 请求权重、Pong/Close 帧。相同尝试在零接受重试时使用同一 charge ID；共享 IP/账户限流由外部实现。TLS 证书与主机名正常校验，TLS 早数据配置被拒绝。

`Disconnect` 只关闭本地连接。显式远端关闭立即撤销接收资格，Spot 对精确订阅 unsubscribe 后按认证模式决定是否 logout；USD-M 发 DELETE 并终止 owner。DELETE 已接受但结果未知时保留 `remote_close_unknown`，不释放管理所有权，也不自动 POST 新 key。Drop 不发 DELETE。

错误、状态和自定义 Debug 不输出 API secrets、签名或 listenKey。显式 `payload`/`raw` getter 保留发送及事实持久化所需字节，调用方自行控制这些字节的存储和日志用途。

## 验证对应

[验证目录](../../docs/validation/binance-private-session/README.md)保存可编译接口行为 Red、实现 Green、独立审阅发现的三个网络边界修复、typed clock 迁移前源码及迁移后的实际日志。协议服务器使用正常测试 CA，在真实 TCP/TLS/HTTP/WSS 上独立验签、注入回复与断连。单元测试使用显式校准器构造 SignedTime，没有 SystemTime 默认校准。

形式阶梯只验证其 00/05 文档列出的状态迁移、令牌和完成边界。密码学、TLS 内存模型、交易所事实完整性、账户恢复与共享限流算法不进入会话状态模型；这些接口在对应模块验收。
