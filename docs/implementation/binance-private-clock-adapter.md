# Binance 私有流校准时钟适配器

`tickwire-binance-session` 将真实 HTTPS 校准客户端接到私有流的必填 Clock 接口。适配器短期独占借用 `BinanceTimeClient`，不复制校准器，不重签已准备请求，不在 `sample` 中发网络请求。

```rust
pub trait DeliveryBudget {
    fn remaining_ns(&mut self, scope: &AccountScope, stamp: &SignedTime,
                    now: &ClockSample) -> tickwire_binance_private::Result<u64>;
}
pub struct ClockAdapter<'a, C: MonotonicClock, B: DeliveryBudget> { /* 私有 */ }
impl<'a, C: MonotonicClock, B: DeliveryBudget> ClockAdapter<'a, C, B> {
    pub fn new(time: &'a mut BinanceTimeClient<C>, scope: AccountScope,
               budget: &'a mut B) -> Result<Self>;
}
// ClockAdapter implements tickwire_binance_private::Clock.
```

构造时校验完整 AccountScope，并以 `TimeClient::source()/domain()` 固定产品、环境与单调域。`sample` 直接保留 TimeClient 的 Instant、MonoTime 和可选 SignedTime；没有有效校准时 SignedTime 为 None，禁止凭 SystemTime 构造可信时间。

`check_signed` 先核调用 scope 与绑定值一致，原 SignedTime 的 Source/domain 一致，recvWindow 微秒转换为纳秒不溢出。随后重新采样，交付预算提供者读取完整 scope、原 stamp 和新样本，返回从最后检查至远端时间校验的剩余时间上界。适配器将原 stamp、原窗口与该预算交给 `TimeClient::check_send`；它会再次采样并检查当前区间。预算回调耗时不能让旧样本跳过最后检查。

Clock 的 Domain、TimeBackwards、Faulted、ClockJump、Overflow 映射为私有流 ClockDomain，使当前 owner 永久停止。Expired、Uncalibrated、Uncertain 映射为暂时 Clock 错误；其他网络、协议和 HTTP 错误也拒绝本次检查，不会产生可信 SignedTime。错误与 Debug 不包含请求凭据。

调用顺序为：reactor 驱动 TimeClient.start/poll 更新校准；创建短借用适配器并调用 PrivateClient.poll；释放适配器后继续驱动两个网络 owner。两者使用同一个底层单调时钟与校准域，无并行可变访问，也不需要共享锁。TimePermit 与 ControlPermit 继续由外部共同额度管理器实现。

实际集成测试使用同一校准客户端连接本地 HTTPS `/api/v3/time`，随后用该校准结果在实际 WSS 签名订阅；服务端独立验签。失败测试覆盖无校准、账户/产品/环境错绑、原 stamp 错域、预算超窗口及预算回调期间校准失效。协议服务端只提供测试时钟读数，适配器与私有流均使用生产实现。

签名窗口与私有请求的本地截止各自独立。PrivateClient 在最后一次适配器检查返回后再次采样，核对原请求及本地截止，随后不再调用许可或预算回调。这样，预算回调推进 MonoTime 后，旧样本不会继续授权首次输出。
