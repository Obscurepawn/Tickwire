# Spot SBE 行情实现

## 数据与身份

固定 Binance 官方提交 `f4a9c0be68235f758b46f571f7313c63d1306b25` 的 `stream_1_0.xml`，schema=1/version=0。原文及摘要在[来源清单](../../crates/tickwire-binance-sbe/schema/source.json)。生成器只产生编译期常量；运行库不加载 XML/JSON。四个模板覆盖 trade、bestBidAsk、depth20、depth diff；未知模板、版本或组宽度拒绝。

解码先检查整个帧和预算，再交付借用视图。整数 mantissa/exponent 按受信目录 tick/step 精确换算，不使用浮点或舍入。目录绑定包括 instrument/version/catalog/hash/source/generation，报文符号必须一致。SBE 微秒时间 checked 乘 1000，规范记录使用纳秒。截断、尾随字节、非法枚举/null、负时间、溢出和容量超限都失败。

Diff 的零数量表示删除，U/u 进入现有 Spot 连续性规则。depth20 只给出 Through(last price) 或 Unknown，不能充当完整 REST 初始化快照。该流未接入 DepthSync diff owner；Trade/BestBidAsk 当前交付 codec 视图，独立业务 owner 仍属于后续范围。

## 真实 owner 与发布

`PublicDepthClient::new_sbe` 只接 SpotProduction、微秒和默认速度。它连接 `stream-sbe.binance.com`，正常验证 hostname/CA，在 Upgrade 携带明确提供的 Ed25519 API key；API key 的实际类型由服务端校验，client 不持有签名私钥。HTTPS 快照仍连接 `api.binance.com`，且不带此 key。Raw URL 和 Text 订阅两种方式都可用。

Binary 原文直接经过 `decode_sbe_diff`、typed DepthUpdate 和现有 DepthSync。解码在最终 typed 数组内完成精确转换，避免在发布调用栈中额外保留完整归一化数组；固定容量 Book 在 owner 初始化时分配一次。旧 token 在解码前拒绝；缓存、重建、首次 U、快照前缀重放、输入时间和容量检查共用原实现。Binary 可先于订阅 ACK 到达并缓存，精确 ACK 到达后才请求快照。Text 行情、其他产品/环境和不支持的 stream 参数拒绝。连接、Upgrade、订阅/Pong、快照继续使用 PublicPermit 的实际请求身份与共享额度。

Notice 显式携带 DepthEncoding 和原始 Binary，MarketPublisher 直接生成 MarketRecord、arena 载荷、Quote/FeedState，保留 source/ingress/publish 时间。MD 的 `sbe_api_key_file` 可选字段启用该 owner，省略时保持旧 JSON 行为。策略恢复 WAL 保存实际消费的规范记录与载荷，不新增 MD 同步 raw journal。

## 验证与完成边界

[证据目录](../../docs/validation/binance-sbe/)保存编译后的行为 Red、整数边界、typed 协调器与真实本地 TLS/WSS 测试。测试 CA 明确覆盖正式 hostname，没有关闭 TLS 校验。基准字节由独立 Python struct.pack 脚本生成，不读取生成器常量或 XML；四份 literal fixtures 与 XML 常量再生成均逐字节相同。

Linux 专用测试 `tickwire-md --test sbe_publication` 使用真实 Publisher/Subscriber memfd 映射，串联同一 WSS owner、快照、后续 Binary、规范盘口记录和借用读取，检查价格、数量、连续更新号、微秒到纳秒换算及 payload。其执行状态以证据 README 为准。

现有 DepthSync/PublicSession 形式模型保留历史范围；SBE 分支和新增编码标记待完整代码链路完成后统一建模与同步。
