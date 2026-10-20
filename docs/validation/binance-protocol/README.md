# Binance 协议验证记录

验证对象是 `tickwire-binance` 的签名、订单编码和响应解码。[接口](../../implementation/binance-interface.md) 列出已实现组合和后续接入项，[实现说明](../../../specs/implementation/binance-protocol.md) 记录调用关系。

## 失败测试与实现

| 日志 | 实际结果及其用途 |
|---|---|
| [primitives-red.log](primitives-red.log) | 13 项断言失败；固定签名向量、字节编码、十进制与容量原子性先于实现 |
| [primitives-green.log](primitives-green.log) | 对应 13 项通过 |
| [requests-responses-red.log](requests-responses-red.log) | 11 项请求断言失败；响应 1 通过、6 失败 |
| [requests-responses-green.log](requests-responses-green.log) | 请求 11 项、响应 7 项通过 |
| [boundary-red.log](boundary-red.log) | 非 JSON 前导零与 HTTP 200 畸形错误误判，各有一项断言失败 |
| [typed-response-red.log](typed-response-red.log) | 5 项断言失败、8 项通过；包含尚未实现的类型化成功响应和前述畸形错误回归 |
| [usdm-fields-red.log](usdm-fields-red.log) | GTD 错误单位、`activationPrice` 错误名称各一项失败；修正为毫秒和 `activatePrice` |
| [algo-protection-red.log](algo-protection-red.log) | 当前官方条件单接口的 `priceProtect` 要求小写字符串，原大写输出断言失败 |
| [fix-sender-red.log](fix-sender-red.log) | 独审发现 SenderCompID 接受非法标点；新增断言先失败，再收紧字符集 |
| [unknown-code-red.log](unknown-code-red.log) | HTTP 408 被分类为拒绝的断言失败；新增错误码也改为保留未知 |

这些 Red 都完成编译并进入断言。最终结果和源文件摘要见下列命令日志及 `SHA256SUMS`。早期日志的测试数量对应当时范围，后续新增用例没有覆盖旧日志。

最终本机与 Linux 各 45 项通过：签名/参数原语 15 项，请求 14 项，响应 14 项，真实网络集成 2 项；doc-test 为 0 项。两平台的格式检查与 Clippy 均通过，见 [green.log](green.log)、[linux.log](linux.log)、[clippy.log](clippy.log)、[fmt.log](fmt.log) 和 [doc.log](doc.log)。Linux 使用固定的 Rust 1.99.1 Bookworm 容器，镜像摘要为 `sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29`。

```sh
cargo test -p tickwire-binance --all-targets
cargo test -p tickwire-binance --doc
cargo fmt -p tickwire-binance -- --check
cargo clippy -p tickwire-binance --all-targets -- -D warnings
```

## 实际网络测试

[`protocol_server.rs`](../../../crates/tickwire-binance/tests/protocol_server.rs) 启动本机 TCP 服务端，生成仅供测试的证书并把证书加入客户端信任根；客户端正常执行证书及主机名校验。测试使用真实 `PreparedRequest`、HTTP/WS 编码器和非阻塞 `Connection`，没有替换成 `MockExchange` 返回值。

两项集成测试共建立五次 TLS 连接：Spot REST 分别验证 HMAC、RSA、Ed25519；USD-M WSS 分别验证 HMAC、Ed25519。服务端从收到的 request target 提取精确 query，从 JSON 独立排序参数重建 WS 原文，再用公开验证算法验签。用例同时检查 Unicode 百分号编码、价格/数量字符串、随机掩码、HTTP chunked 和 WS 分片响应。每次 TLS 接受应用字节的上限为 17 字节；摘要比较门禁只在首次接受字节前调用一次。WS 503/-1007 响应保留 `Unknown`。

请求准备目前允许有界分配。测试没有测量吞吐或尾延迟，也未对该模块建立会话形式模型。成交、费用、余额、订单状态回报到业务记录的完整映射仍在接口的后续接入项中。

## 来源与测试材料

Spot 固定提交为 `f4a9c0be68235f758b46f571f7313c63d1306b25`。[来源索引](sources/spot-source-index.json) 保存官方原文件 URL、完整原始字节 SHA-256 和长度；[脚本](sources/capture-spot.py) 可重新获取并核对，没有把整站复制进仓库。

USD-M 的[选定协议内容](sources/usdm-protocol-selection.json) 保存实际使用的字段名、类型、枚举、单位和错误语义，以及对应官方页面和章节。开发者站点的直接 `urllib` 请求返回 HTTP 202 空正文，本次通过文档文本读取器核对内容。其 SHA-256 对应这份明确选择的记录，未冒充上游完整 HTML 哈希。

[官方固定 HMAC 与 FIX Ed25519 向量](../../../crates/tickwire-binance/tests/protocol_primitives.rs) 直接比较固定预期签名。RSA 使用本地生成的测试专用密钥，预期签名由独立 OpenSSL 命令生成，见[材料说明](../../../crates/tickwire-binance/tests/data/README.md)。Spot REST 文档中名为 Ed25519 的示例包含与 Ed25519 不符的 RSA 风格命令/长度，因此测试采用同一固定提交的 FIX Ed25519 公布向量，没有据错误示例更换算法。

最终源码、测试、来源选择和证据文件以[校验摘要](SHA256SUMS) 固定。源码独立审阅结论与最后一轮平台检查记录在同目录的 `review.md` 和命令日志中。

归档日志的末尾空白行已统一清理，测试输出内容保持原样。
