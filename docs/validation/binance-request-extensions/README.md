# Spot 列表、挂钩价格和 SOR 验证记录

扩展包含 venue canonical tag 11 与 Binance REST/WSS 请求编码。新增测试共 17 项：venue 7 项、请求 10 项。请求测试中的本地服务器使用真实 TCP/TLS：5 类请求分别经 HTTP 和 WSS 发送，共 10 次签名事务；证书由测试 CA 签发，生产客户端仍执行 hostname 校验。服务器从实际收到的 query/JSON 重算 HMAC 并校验端点和业务字段。

## 失败到通过

| 阶段 | 实际结果 | 日志 |
|---|---|---|
| 原 SpotList 请求 | 2 失败 / 1 通过；合法列表返回 UnsupportedCommand | [list-red.log](list-red.log) |
| 新类型接口占位实现 | 2 失败 / 3 通过；合法 peg/SOR 返回 OrderCombination | [canonical-red.log](canonical-red.log) |
| canonical 已实现、协议尚未实现 | 5 失败 / 1 通过；列表/peg/SOR 返回 UnsupportedCommand | [advanced-red.log](advanced-red.log) |
| 新 canonical 首轮 | 5 通过 | [canonical-green-initial.log](canonical-green-initial.log) |
| 新请求首轮 | 6 通过 | [request-green-initial.log](request-green-initial.log) |
| 请求边界与真实 TLS | 10 通过 | [expanded-green.log](expanded-green.log) |
| macOS 完整 venue、binance | 通过 | [venue-green.log](venue-green.log)、[native-green.log](native-green.log) |
| Linux 完整 venue、binance | 通过 | [linux-green.log](linux-green.log) |
| macOS / Linux Clippy | 无 warning | [native-clippy.log](native-clippy.log)、[linux-clippy.log](linux-clippy.log) |

日志中 binance 总测试数还包括同阶段独立开发的行情解码模块；本扩展的验收统计固定为上述 17 项。

## 历史兼容性

[legacy-commands.txt](legacy-commands.txt) 在修改 venue 生产实现前生成，保存原 10 种命令的完整 CBOR 十六进制及 SHA-256。新增回归把当前编码逐字节与这些样本比较。它们是历史兼容性基准，不充当独立协议编码器。

[verify-legacy.py](verify-legacy.py) 从仓库固定提交 `1d2743d7315456816d328354140ec2105754175f` 取出旧生产源码和测试 fixture，在临时目录编译。它验证旧实现仍生成相同 10 条样本，并把四个新子型交给旧解码器，确认全部返回 `Enum`。新输入保存在 [tag11-commands.txt](tag11-commands.txt)，该文件由新实现编码，用途是测试旧消费者的拒绝行为。实际输出见 [legacy-verification.log](legacy-verification.log)。

```sh
python3 docs/validation/binance-request-extensions/verify-legacy.py
cargo test -p tickwire-venue
cargo test -p tickwire-binance
cargo clippy -p tickwire-venue -p tickwire-binance --all-targets -- -D warnings
```

## Linux 环境

固定镜像为 `rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29`，运行架构为 Linux aarch64。目标目录与其他构建分开，避免 Cargo 锁相互等待。

```sh
docker run --rm \
  --mount type=bind,src="$PWD",dst=/work \
  --mount type=bind,src="$PWD/.tools/linux-cargo-registry",dst=/usr/local/cargo/registry \
  -w /work -e CARGO_TARGET_DIR=/work/target/linux-binance-extensions \
  rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29 \
  cargo test -p tickwire-binance -p tickwire-venue
```

将最后一行替换为 `cargo clippy -p tickwire-binance -p tickwire-venue --all-targets -- -D warnings` 可复现 Linux lint 检查。官方文档固定提交、文件字节数和摘要在 [sources.json](sources.json)，实现与测试摘要在 [source-hashes.json](source-hashes.json)。

## 独立源码审阅

主任务独立核对新增 venue 编解码、验证分支及 Binance 请求映射，并复核固定版本官方 REST 参数表与 peg FAQ，未发现阻断项。审阅确认旧编码保留、新参数进入命令摘要、列表共享字段校验以及账户层对未知风险界返回 Unsupported。源码审阅没有替代上述实际测试，也没有新增交易所行为的形式化证明。文档全仓检查通过，输出见 [documentation.log](documentation.log)。
