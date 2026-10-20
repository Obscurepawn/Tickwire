# 私有流校准时钟适配规格

实现为 `tickwire-binance-session::ClockAdapter`；公共签名见[接口](../../docs/implementation/binance-private-clock-adapter.md)。适配器借用同一个 `BinanceTimeClient`，提供私有流的 `Clock`，不持有另一份校准区间或发送队列。

构造时验证账户 scope，并将市场/环境映射到 TimeClient 的固定 Source。每次签名检查再比较完整 scope，包括账户、risk domain、凭据代次；原 stamp 的 Source 与 ClockDomain 同样精确匹配。窗口微秒转纳秒使用 checked arithmetic，零值拒绝。

`sample` 保留 TimeClient 的原始 Instant、MonoTime 和可选 SignedTime。尚未校准或校准暂不可用时保留 None；不会创建替代时间戳。`check_signed` 使用原请求的 SignedTime，重新取样后调用交付预算提供者，再通过 `TimeClient::check_send` 取样并验证当前区间。新样本的时间戳不能替换旧 stamp。

永久错误 Domain/TimeBackwards/Faulted/ClockJump/Overflow，以及 TimeClient 代次耗尽，映射为私有流 ClockDomain。私有流收到它会停止当前 owner。其他错误均拒绝本次时钟检查；网络、HTTP 和协议错误不会被转换成可信时间。

交付预算是调用方提供的剩余时间上界，没有默认值。适配器明确把它传给校准器；共享限流和该预算的生产估算由外层实现。单 reactor 在驱动 HTTPS 校准和私有流之间短期创建适配器，Rust 可变借用阻止对同一 TimeClient 并发更新。

最后一次适配器检查返回后，PrivateClient 再采样并核对本地请求截止。实际 HTTPS/WSS 回归使用1秒请求截止、5秒签名窗口及10秒校准有效期；预算回调把 Mono 推进到2秒时，要求首次输出为0字节。新增回归及修复见[本地截止证据](../../docs/validation/binance-private-clock-adapter/local-deadline/README.md)。

[验证记录](../../docs/validation/binance-private-clock-adapter/README.md)保存6项编译成功的行为失败及实现后的10项回归，包括真实 HTTPS→WSS 认证、缺少校准、身份与域、原 stamp 保留、超窗预算、预算回调内过期/变域和错误分类。适配器没有独立可重放状态；形式验证接缝沿用私有流 L0 的必填 Clock 契约和 clock 阶梯的 check_send 约束，不另造无状态形式证明。
