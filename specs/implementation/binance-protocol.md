# Binance 签名与订单协议

实现入口是 [`tickwire-binance`](../../crates/tickwire-binance/src/lib.rs)，契约见[接口](../../docs/implementation/binance-interface.md)，实际命令输出见[验证记录](../../docs/validation/binance-protocol/README.md)。

先落接口，再对签名、参数、十进制、命令和响应写可编译的失败测试。签名测试采用官方 HMAC/FIX Ed25519 向量和独立 OpenSSL 生成的 RSA 签名；随后实现 `SigningKey`、`Params`、`Decimal`、`prepare` 与响应解码。

`PreparedRequest` 私有字段保存不可变请求及完整 `CanonicalCommand.digest()`。REST 的已签名 query 直接用于 HTTP request target；WS 的已签名参数与 JSON 值来自同一组参数。账号、产品、环境不一致时不产生请求。所有字段取自冻结命令，保证金模式等非订单参数的账户状态由发送前协调器核对。

后续测试实际暴露并修正了 JSON 数字前导零、HTTP 200 畸形错误对象误入成功分支、USD-M GTD 单位、条件单 `activatePrice` 名称、`priceProtect` 大小写和错误分类默认值。独立审阅又发现 FIX SenderCompID 字符校验过宽，已补失败断言并按官方字符集收紧。错误分类改为确定拒绝码表，HTTP 408 与新错误码保持未知。错误分类没有释放资金或自动重试的副作用。

集成测试以同一个 `PreparedRequest` 驱动真实 `tickwire-transport::Connection`。本地 TLS 服务端独立校验 REST/WS 签名与参数，返回分块 HTTP 和分片 WS 消息。客户端限制每次接受 17 字节，并在第一次接受应用字节前比较命令摘要。验收针对协议字节、发送门禁示范和解码结果；会话、恢复、业务执行协调仍按接口中的待接入矩阵推进。

本模块没有新增 TLA+ 模型。传输首次输出模型和既有命令摘要持久化模型由对应模块验证；它们不覆盖本模块尚未建立的 Binance 会话状态机。
