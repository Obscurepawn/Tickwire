# Binance 私有流形式阶梯

范围为 `tickwire-binance-private` 的实际单 owner 连接、认证/订阅、续期和关闭。`&mut PrivateClient` 串行调用；首接受回调同步执行，回调内可以消耗时间和拒绝额度，但不能重入 owner。网络可以断开、丢失回复、先发事件、发送过期或过早回复。

| 阶段 | 增量 | 状态 |
| --- | --- | --- |
| L0_session_lifecycle | 认证及 key 生命周期、首次接受、请求关联、缺口与旧事实排空 | PASS |
| L1_transport_scheduling | 精确双 lane 预算、Pong/Close 优先级与帧解析调度 | 后续 |
| L2_account_barrier | 与持久化账户恢复屏障组合 | 后续 |

L0 固定一个账户与凭据，不重新证明签名、TLS、时钟区间数学或账户对账。Source/ClockDomain、JSON 和真实网络边界由本模块及依赖的 Rust 测试验证。不会把测试里的可信许可实现当作共享限流器。
