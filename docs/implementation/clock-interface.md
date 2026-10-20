# 签名时钟接口

状态：校准核心、HTTPS 时间客户端、系统单调时钟和私有会话适配已实现；对应测试已在 macOS 和 Linux 通过。源码与全仓检查结果见[组合验证](../validation/clock-session/README.md)，形式验证范围由各阶梯单独记录。

`tickwire-clock` 把一个本地单调时钟域映射到交易所时间区间，向签名器提供有误差上界和有效期的读数。校准状态归单个 owner 所有；每次读数、接收校准回复和首次发送检查都使用同一时钟域。账户的本地期限继续使用单调时间。

## 1. 时间来源与校准

本地 `ClockDomain` 包含机器启动标识和进程内时钟代次，`MonoTime` 只在这个域内比较。上层提供经过校验、包含系统挂起时间的单调纳秒读数；跨进程共享前另行确认时钟域一致。系统 UTC 墙钟不会直接生成可信签名时间。单调时间回退、域不符或计算溢出立即撤销当前校准。

Binance 校准客户端使用真实 `tickwire-transport::Connection` 连接固定产品与环境的 HTTPS authority，校验证书和主机名后请求 Spot `/api/v3/time` 或 USD-M `/fapi/v1/time`。响应必须是完整的成功 HTTP 消息，`serverTime` 为正整数毫秒。客户端在请求可能输出之前记录 `m0`，完整响应到达后记录 `m1`；一个请求令牌绑定产品、环境、时钟域、连接代次和采样序号。旧回复在读取其时间字段之前按令牌拒绝。

服务端时间对应请求与响应之间的某个时刻。令 `S` 为 `serverTime` 换算成纳秒后的值，`q` 为报文精度的保守误差上界，`d` 为单调时钟与服务端时间变化率的最大相对偏差。接收时刻的区间取：

```text
L = max(0, S - q)
U = S + q + ceil((m1 - m0) × (1 + d))
```

这保留往返不对称造成的不确定性，不把半个往返时间当作已测得的单向延迟。核心校验 `q` 至少为一毫秒，产品适配层采用一毫秒起点；它覆盖毫秒量化，服务端时钟额外误差需由部署配置加入。区间计算以服务端时钟从采样请求开始 `m0` 到校准有效期结束，始终按配置的变化率连续推进为前提，不允许未计入误差的阶跃跳变。采样间发生的跳变无法由本地读数立即检测。

在接收后经过单调时间 `a`，外推为：

```text
L(a) = L + floor(a × (1 - d))
U(a) = U + ceil(a × (1 + d))
```

所有乘法、舍入和加减使用整数并检查溢出；`d` 用非负有理数表示，严格小于 1。配置显式给出最大往返时间、最大区间宽度、最大校准年龄、变化率误差和校准跳变检测阈值，没有无限有效的默认值。新样本既要满足自身界限，也要与尚未过期的旧校准外推区间相容；两个区间的间距超过检测阈值时，校准进入 `Faulted`，等待新时钟代次重新采样。该检测只处理新样本已经揭示的变化，不提供采样之间的阶跃容错。宽区间或高延迟样本可以被拒绝而保留尚未过期的旧校准；域错误、时间回退和已检测的大幅跳变会使旧校准一起失效。

每个 owner 至多有一个未完成样本，序号 checked add，结束或取消的样本不复用。重启后重新采样，不把旧进程的映射恢复成有效校准。HTTP 超时或连接失败只结束这次采样；它不会延长旧读数的期限。

## 2. 读数与发送检查

校准器返回私有构造的 `Reading`，包含校准代次、采样时刻、单调有效期及 `[lower_ns, upper_ns]`。编码器从区间中点生成毫秒或微秒时间戳，保留量化后的确切签名值，并把这份读数与请求摘要一起保存。等待 socket 可写不会重新使用当时的校准结论。

首次 TLS 接受应用字节之前，发送 owner 重新读取当前时间，核对时钟域、校准代次、样本年龄、区间宽度及已签名值。设新读数为 `[L, U]`，报文中的精确时间戳为 `T`，`W` 为 `recvWindow`，`B` 为以本地单调时间计的后续传输与服务端处理预算，则要求：

```text
T < L + 1 second
U + ceil(B × (1 + d)) - T <= W
```

另外检查 `now + B < valid_until`；校准期限采用排他边界，预算终点须早于有效期结束。减法按有符号的数学含义检查，溢出即拒绝发送。第一项采用交易所的严格未来时间边界；第二项为发出后的处理保留预算。它只验证请求发出时的本地条件，后续实际传输若超过 `B`，交易所仍可拒绝请求。`recvWindow` 继续服从具体接口的最大值与精度，时钟失效时关闭发送资格。

新请求使用当前校准的 `Reading`；已完成编码的旧报文仍可用当前校准重新检查同一个 `T`，前提是产品、环境和时钟域一致。正常更新增加采样代次；故障后重新启用则更换时钟域代次，并拒绝旧代次报文。跨过首次接受边界的请求继续按发送状态保存不确定结果，时钟故障不构成零发送证明。

公共 API：

```rust
pub struct ClockDomain { /* 启动标识、代次 */ }
pub struct MonoTime { /* 时钟域、纳秒 */ }
pub struct CalibrationConfig { /* 有界误差、年龄、往返和跳变策略 */ }
pub struct SampleToken { /* 私有：域、产品、环境、序号、m0 */ }
pub struct Reading { /* 私有：代次、区间、采样与失效时间 */ }
pub struct SignedTime { /* 私有：来源绑定、精确量化的 T */ }
pub struct Calibrator { /* 单 owner */ }

impl Calibrator {
    pub fn begin(&mut self, now: MonoTime) -> Result<SampleToken>;
    pub fn finish(&mut self, token: &SampleToken, received: MonoTime,
                  server_time_ms: u64) -> Result<Reading>;
    pub fn cancel(&mut self, token: &SampleToken) -> Result<()>;
    pub fn read(&mut self, now: MonoTime) -> Result<Reading>;
    pub fn sign_time(&mut self, now: MonoTime, unit: TimeUnit) -> Result<SignedTime>;
    pub fn check_send(&mut self, stamp: &SignedTime, now: MonoTime,
                      recv_window_ns: u64, remaining_budget_ns: u64) -> Result<()>;
    pub fn status(&self) -> CalibrationStatus;
}
```

`finish` 是可信网络适配层的输入边界：调用方负责把响应与实际 TLS 请求关联；纯校准器本身不验证网络身份。实际 `BinanceTimeClient` 在内部完成关联并调用它，不向应用暴露任意回复注入接口。客户端采用有界 `poll`，不睡眠、不阻塞 DNS，连接额度和请求权重由与私有流相同的外部限流 owner 许可。

HTTP `/time` 响应没有请求 ID。第一版每次采样使用独立连接，完成、超时或取消后均关闭；下一样本使用新连接代次。这样超时请求的迟到回复不会被重新关联给下一次采样。采样开始时间在 TLS 就绪、首次应用字节接受之前读取，避免把握手耗时计入服务器时间查询的往返区间。

## 3. 首批验证

行为测试先覆盖不对称往返、毫秒量化、误差随年龄增长、过期边界、单调回退、旧令牌、错域、样本序号耗尽和全部整数溢出。发送测试分别命中未来严格边界、窗口等号、微秒转毫秒截断、等待期间校准失效及新校准对旧报文的复核。

真实本地 TLS 服务端检查两个产品的 `/time` 请求、Host 和空参数，注入分片响应、非成功状态、重复字段、错误字段类型、迟到与连接中断。私有流 `Clock` 已通过[组合适配器](binance-private-clock-adapter.md)接入；现有 `RestEncoder::TimeProvider` 的编码和最终发送证据仍需组合接线。

TLA 模型对应单 owner 调用顺序，保留令牌代次、区间端点、采样在途、失效和首次发送判定。性质包括旧回复保持当前时钟状态，时间倒退、过期、误差超界和整数溢出时拒绝签发或发送。真实网络延迟是否小于配置预算、操作系统时钟的频率误差和交易所时间跳变由实测与运行监控提供证据。

协议依据为 [Spot Timing security](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-api.md#timing-security)、[Spot server time](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/rest-api.md#check-server-time) 及 [USD-M 通用规则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info)。[USD-M 时间接口](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#check-server-time) 已核对：GET `/fapi/v1/time`，无参数，IP 请求权重 1，成功体包含整数 `serverTime`。


## 4. HTTPS 时间客户端

`tickwire-binance-clock` 单独依赖校准核心和真实网络传输，不把网络依赖引入 `tickwire-clock`。`BinanceTimeClient<C>` 同时持有校准器、单调时钟提供者 `C` 和至多一个在途 HTTPS 连接。它只公开读取、签名时间与发送检查，不公开任意注入服务端时间的入口。生产客户端的发送检查内部重新读取 `C`，不接受调用方缓存的 `now` 作为最终依据。

`MonotonicClock::sample` 返回 `LocalTime { instant, monotonic: MonoTime }`：`Instant` 供现有传输层执行期限检查，`MonoTime` 供校准计算。客户端固定产品、环境和 `ClockDomain`，每次调用都检查域和时间顺序。[系统时钟适配器](system-clock-interface.md)在 Linux 使用 `CLOCK_BOOTTIME`，在 macOS 使用 `CLOCK_MONOTONIC_RAW`，包含挂起时间；`Instant` 与该读数并非原子采样，包含挂起时间的期限以 `MonoTime` 为准。

提供者返回域不符、时间回退、溢出、时钟跳变或永久故障时，客户端保存故障并停止后续采样。即使自定义提供者之后恢复返回成功，也不能重新开放这个客户端；恢复使用新的时钟域与实例。

```rust
pub struct BinanceTimeClient<C: MonotonicClock> { /* 单 owner */ }
impl<C: MonotonicClock> BinanceTimeClient<C> {
    pub fn start(&mut self, permit: &mut impl TimePermit) -> Result<u64>;
    pub fn poll(&mut self, permit: &mut impl TimePermit,
                budget: IoBudget) -> Result<PollReport>;
    pub fn cancel(&mut self) -> Result<()>;
    pub fn sample(&mut self) -> Result<ClockReading>;
    pub fn check_send(&mut self, signed: &SignedTime,
                      recv_window_ns: u64, budget_ns: u64) -> Result<()>;
}
```

`start` 消耗新的连接代次并申请连接额度，随后创建连接。额度键包含产品、环境、`ClockDomain`、连接代次和操作种类；同一 `(Source, ClockDomain)` 只允许一个校准 owner，域身份由外部控制面唯一分配。DNS 解析在外部完成；连接 IP 可配置，TLS 主机名和 HTTP Host 固定为产品对应的 Binance authority。生产和测试环境采用不同 authority，测试服务端通过受信测试 CA 提供该域名证书。

`poll` 每次最多调用一次有界传输 `drive`，然后处理有界接收缓冲。TLS 就绪后才排入空参数的 GET 请求；首次应用字节接受的同步检查申请请求权重，并建立采样令牌。连接握手与限流等待不计入已经开始的请求往返。若暂时接受零字节，已建立的起点可以保留为更保守的区间；下次尝试仍复核当前额度许可，外部额度 owner 对同一键幂等记账。旧令牌不会关联其他连接。

完整 HTTP 响应只有在请求全部应用字节已经接受后才可完成样本。客户端拒绝非 200 状态、重复 `serverTime`、非整数或非正值、超出消息容量及不完整响应；不跟随重定向。完成消息的接收时间在解析完整响应后重新采样。HTTP 状态及响应头作为完成事件交给额度 owner，后者可读取权重与限流反馈。

每个完成事件包含连接代次、可用的 HTTP 元数据和校准结果。失败、取消或超时都会取消仍在途的校准令牌并关闭连接；保留旧校准原有期限。完整响应已经到达时，随后断连不丢弃该消息。下一次 `start` 必须创建新连接，连接代次耗尽后拒绝复用。

私有流适配使用核心的 `MonoTime` 和私有构造的 `SignedTime`，保留原始产品、环境、时钟域和量化后时间戳。`Clock::check_signed` 没有默认放行实现；首次接受闭包在额度检查前后都检查原时间戳，校准刷新只改变检查依据，不重签已经排队的请求。账户 REST 编码的时钟接入还要将这一签名证据绑定到规范命令摘要，并由发送资格 owner 检查；单独实现 `TimeProvider` 只覆盖编码时刻。
