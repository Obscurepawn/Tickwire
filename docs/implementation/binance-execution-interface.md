# Binance 原签名时间与发送门禁

`tickwire-binance-execution::SharedSessionState` 由单个运行时所有者持有，通过 `Rc<RefCell>` 供编码器和 `GateAuthority` 顺序借用。状态包括校准时钟、共享配额 Endpoint、固定账户绑定、传递预算提供者，以及有界的命令摘要到原签名请求的映射。

`encoder(credentials)` 产生 REST 编码器。首次编码从校准时钟取得 `SignedTime`，用该时间戳签名，保存原时间戳、recvWindow 和完整请求字节。相同命令再次编码返回相同字节。凭据轮换使用新的会话状态；旧请求不重新签名。未编码命令没有发送门禁，恢复账户也不会因此自动获得可发送请求。

`authority(inner)` 包装实际账户网关 Authority。`prepare`、`verify` 原样转交持久授权实现。首次接受字节前依次执行：

1. 查找原签名请求，调用可信 DeliveryBudget。
2. 从 inner 读取当前控制策略。
3. `BinanceTimeClient::check_send_sample` 在预算和策略回调之后重新采样，用原 SignedTime 检查未来偏差、recvWindow、校准有效期和传递预算。
4. 用上一步同一 MonoTime 检查策略代次、租约、请求期限，并消费共享配额。

最后检查之后没有预算或策略回调。账户网关继续检查 Admission，再由现有传输层尝试首次接受字节；持久准备、零字节中止和恢复顺序不变。

顶层运行时通过 `with_resources` 驱动校准或其他网络所有者。例如：

```rust,ignore
let shared = SharedSessionState::new(time, endpoint, binding, budget, config)?;
let encoder = shared.encoder(credentials);
let authority = shared.authority(local_authority);
shared.with_resources(|time, quota| {
    let mut permit = quota.time(time_binding);
    time.start(&mut permit)
})??;
```

公共和私有 permit 也在这个独占借用内从 quota 构造。重入借用返回 Busy。`SessionClock` 是受信接口，生产实现直接使用 BinanceTimeClient；DeliveryBudget 给出从最后检查到交易所验证时间戳的保守时间上界。
