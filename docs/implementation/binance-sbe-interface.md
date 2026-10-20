# Binance Spot SBE 行情接口

`tickwire-binance-sbe` 解码固定版本的 Spot SBE 行情。运行库只使用标准库，不读取 XML，不把二进制消息转成 JSON。传输会话仍负责认证、连接代次、接收时间和额度；解码器不能凭报文声明自行建立标的或来源身份。

来源固定为 Binance 官方仓库提交 `f4a9c0be68235f758b46f571f7313c63d1306b25` 的 [stream_1_0.xml](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/sbe/schemas/stream_1_0.xml)。本地 XML、来源与 SHA256 放在 crate 的 `schema/`，`tools/generate.py` 校验摘要后生成固定偏移常量。schema id 为 1、version 为 0；四种模板分别是 Trades、BestBidAsk、DepthSnapshot、DepthDiff。未知 schema、版本、模板或扩展 block length 明确拒绝。

公共入口：

```rust
decode_frame(bytes: &[u8], limits: Limits) -> Result<Frame<'_>>
decimal_to_units(mantissa: i64, exponent: i8, unit: DecimalUnit) -> Result<i64>
normalize_depth<const N: usize>(
    expected: &ExpectedInstrument<'_>, frame: &Frame<'_>, received_mono_ns: u64,
) -> Result<DepthUpdate<N>>
```

`Frame` 是完整验证后的借用视图。返回前检查帧边界、root/group 长度、重复组数量、枚举、必要整数和符号 UTF-8；所有组都通过后才向调用方交付。组迭代不分配，也不在失败前调用业务回调。Trades 组中的常量 `isBestMatch=true` 不占编码字节。计数与帧长度均受调用方预算和库的硬上限约束。

`ExpectedInstrument` 由已经认证的目录构造，绑定符号、标的/目录版本与摘要、来源和行情代次，以及精确价格 tick、数量 step。规范化结果保留这些身份、接收单调时间和报文事件时间；数值采用整数乘除，只接受整 tick/lot，拒绝溢出和小数残余，不舍入到最近格点。

`DepthUpdate<N>` 使用固定容量数组。Diff 的数量零表示删除，携带首末更新 ID；PartialDepth 只能表示前 20 档，结果明确标记两侧不完整，无法作为完整 REST 初始化基线。PartialDepth 检查严格排序、正数量和非交叉；未知价位不自动解释为零。BestBidAsk 的更新号不作为完整深度连续性的证明。

网络接线使用同一传输层的 Binary 帧，JSON Text 只用于订阅控制和 serverShutdown。SBE 接入按官方要求在 Upgrade 中提供 Ed25519 类型 API key，使用微秒事件时间；密钥引用、连接额度与帧额度独立绑定。[固定连接规则](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/sbe-market-data-streams.md)

`feed::decode_sbe_diff` 和 `DepthSync::on_sbe_delta` 已直接消费二进制；旧连接 token 在解码前拒绝，当前输入与 JSON 共用容量、连续性、快照重放和时钟检查。SBE depth20 不能进入这个 diff 入口。

`PublicDepthClient::new_sbe(config, network, clock, SbeAccess::ed25519_api_key(key))` 使用正式 Spot SBE hostname 做 TLS 校验。入口只接受 `SpotProduction`、`Microseconds` 和默认速度（官方固定 20ms），保持原有 `Config`/`NetworkConfig` 字段和 JSON `new` 入口兼容。Text 仅处理精确订阅 ACK/serverShutdown，行情必须 Binary；配置错误、其他模板和畸形帧会关闭或重建对应源。API key 不实现 Debug，也不进入快照请求。

`Notice::Sync.encoding` 显式标记 Json/SpotSbeV1，`raw_delta` 保留原始 bytes。MarketPublisher 按编码直接解码为规范盘口记录，微秒事件时间精确换算纳秒；没有 JSON 中转。MD 的可选 `Feed.sbe_api_key_file` 启用这条真实 owner，省略时继续 JSON。文件读取有容量和权限检查，连接、Upgrade、订阅/Pong、快照仍走共享 quota 回调。

策略有序输入 WAL 保存实际消费的规范 MarketRecord、arena 载荷和来源时间。当前 MD 没有同步原始交易所帧 WAL；原始 Binary 只在 Notice 借用期提供。
