# Venue 模型与编码验证

`tickwire-venue` 当前包含不可变命令/事件、规范 CBOR、内容摘要和参数语义检查。完整范围见 [接口](../../implementation/venue-interface.md) 与 [实现规格](../../../specs/implementation/venue.md)。本目录保留真实命令输出；日志中的工作区绝对路径统一替换为 `<REPO>`，不改变诊断或测试结果。

## 测试记录

| 阶段 | 命令 | 实际结果 | 证据 |
|---|---|---|---|
| 接口后的行为 Red | `cargo test -p tickwire-venue` | 编译成功；4 项中 3 项断言失败 | `initial-red.log` |
| 首轮实现 | 同上 | 4 项通过 | `initial-green.log` |
| 最小有符号手续费 Red | `cargo test -p tickwire-venue --test events` | 5 项中 `minimum_i128_fee_is_preserved_without_negating_it` 因 `IntegerOverflow` 失败 | `min-fee-red.log` |
| 最小有符号手续费修复 | 同上 | 5 项通过；使用 unsigned_abs，不求 i128::MIN 的有符号绝对值 | `min-fee-green.log` |
| Unicode 标的 Red | `cargo test -p tickwire-venue --test contract` | 编译成功；5 项中 Unicode symbol 因 `Text` 失败 | `unicode-symbol-red.log` |
| Unicode 修复及回归 | `cargo test -p tickwire-venue` | 当时全部 27 项通过 | `unicode-symbol-green.log` |
| 报告现金字段 Red | `cargo test -p tickwire-venue --test events` | 6 项中新增字段往返丢失断言失败 | `reported-cash-red.log` |
| QuoteAmount SELL Red | `cargo test -p tickwire-venue --test commands spot_market_sell_accepts_a_quote_amount_target` | 编译成功；因 `OrderCombination` 失败 | `quote-sell-red.log` |
| 最终单元与集成 | `cargo test --locked -p tickwire-venue` | 35 项通过，0 忽略 | `final-native.log` |
| Linux 单元与集成 | `cargo test --locked -p tickwire-venue`，Linux aarch64 / rustc 1.99.1 | 35 项通过，0 忽略 | `final-linux.log` |
| 静态检查 | `cargo clippy --locked -p tickwire-venue --all-targets -- -D warnings` | 通过 | `clippy.log` |

QuoteAmount SELL 修正来自 root 对 [固定官方 Spot 文档](https://raw.githubusercontent.com/binance/binance-spot-api-docs/f4a9c0be68235f758b46f571f7313c63d1306b25/rest-api.md) 的独立审阅：普通 MARKET 的 quoteOrderQty 同时支持买入与卖出；OTO pending 与 SOR 有独立限制。当前类型未开放 SOR，OTO pending 的 QuoteAmount 仍被拒绝。新增现金字段来自与适配器的字段核对；`pre-cash-vectors/` 保存补齐前的未发布事件样本。

执行时通过 `PATH="$HOME/.cargo/bin:$PATH"` 选择已安装工具链。`cargo fmt -p tickwire-venue --check` 和限定目录的差异空白检查另见 `checks.log`。

当前测试覆盖十种命令分支、六种产品/合约表达、OCO/OTO/OTOCO、Spot 与衍生品移动止损单位、目录和账户上下文摘要、原始父子身份、多资产费用和未知数量。价格步长测试与独立整数算式核对 31×17×5×5 = 13,175 组组合。271 字节命令及 176 字节事件的每个截断前缀都被拒绝；命令逐字节执行三种位变异，成功解码的结果必须仍逐字节规范往返。解析器还检查总长、长度炸弹、非最短头、未知 schema、错误域、不规范十进制和 i128 边界。

Linux 命令使用以下固定镜像与独立构建目录：

```sh
docker run --rm --init \
  --mount "type=bind,source=$PWD,target=/work" \
  --mount "type=bind,source=$PWD/.tools/linux-cargo-registry,target=/usr/local/cargo/registry" \
  -w /work -e CARGO_TARGET_DIR=/work/target/linux-venue \
  -e RUSTUP_TOOLCHAIN=1.99.1 \
  rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29 \
  bash -c 'uname -srmo; rustc --version; cargo test --locked -p tickwire-venue'
```

## 独立测试向量

`reference_vectors.py` 用 Python 标准库逐字段转录 schema，不调用 Rust 编码器。它生成四份固定样本及 SHA-256，Rust 测试直接读取文件并逐字节比较：

| 样本 | 长度 | 内容 |
|---|---:|---|
| `vectors/spot-limit.hex` | 271 | 完整限价买单、目录、策略和触发引用 |
| `vectors/multi-asset-fill.hex` | 176 | 含负手续费和正返佣的双资产成交 |
| `vectors/minimum-i128-fee.hex` | 181 | i128::MIN 费用及大整数 CBOR |

| `vectors/reported-cash.hex` | 203 | 原始累计成交额、本笔成交额与独立已实现盈亏 |

复算命令：`python3 docs/validation/venue/reference_vectors.py`。输出见 `reference-vectors.log`。`encoding_bound.py` 对当前字段作保守尺寸累计，命令上界 1915 字节、事件上界 1564 字节，均低于解码前的 16 KiB 硬上限；互斥选项一并累计，因此这些数值是结构上界。

## 形式检查和集成边界

命令编码是无持久状态的转换。执行器的 [L1_command_binding](../../../tla/execution_models/business_journal/L1_command_binding/00_input.md) 模型检查摘要在预留、依赖闭包、WAL、回执和恢复间的绑定；其中摘要为有限符号，解析和 SHA-256 算法不由该模型证明。该模型由 root 维护，本模块没有为编码器另建不对应实际执行流程的状态机。

当前模块不校验真实账户授权、交易所功能全集或网络签名；具体规则由使用同一 `CanonicalCommand` 的适配器校验。目录与命令的匹配、行情时效、原订单管理权和改单减量比较需要外部已验证状态。
