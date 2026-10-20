# 账户模块验证记录

当前源码在 macOS 与真实 Linux 容器各通过 46 项测试，Clippy `-D warnings` 与格式检查通过。两层账户模型共六个配置完整穷举通过；每层所有具名 Next 动作都生成过状态。

## 实际 Red 与修复

先定义 [接口](../../implementation/account-interface.md)，再编译运行行为测试。下表 Red 均为编译成功后的断言失败；没有把缺少类型或编译器错误当作行为失败。

| 日志 | 失败内容 | 修复后的检查 |
|---|---|---|
| `initial-red.log` | 4 项基本准入、幂等、Market 拒绝及时效 | `first-green.log` |
| `facts-red.log` | 3 项截止重验、超预算实际报价和费用冻结 | `facts-green.log` |
| `futures-red.log` | 1 项相抵策略仓位遗漏毛敞口 | `facts-green.log` 与最终全集 |
| `pool-red.log` | 2 项费用池复用、冻结后的撤单 | `pool-green.log` |
| `ack-red.log` | 2 项 ACK→中止、先中止→迟到 ACK | `ack-green.log` 与独立复现复查 |
| `replay-observation-red.log` | 2 项相同成交携带新 Unknown/终态 | `replay-observation-green.log` 与独立复现复查 |
| `recovery-red.log` | 3 项顺序恢复、完整 image 比较和冻结 | `recovery-green.log` |
| `external-unit-red.log` | 1 项非结算币扣款混入结算原子数 | `external-unit-green.log` |
| `late-terminal-red.log` | 1 项旧终态累计小于后来成交而未冻结 | `late-terminal-green.log` |

基本准入阶段实际使用 `cargo test -p tickwire-account`；各项回归可按以下文件与筛选入口重跑，均加 `PATH="$HOME/.cargo/bin:$PATH"`：

```sh
cargo test -p tickwire-account --test futures
cargo test -p tickwire-account --test admission
cargo test -p tickwire-account --test facts acknowledgment
cargo test -p tickwire-account --test facts repeated_trade
cargo test -p tickwire-account --test recovery
cargo test -p tickwire-account --test futures external_nonsettlement_debit
cargo test -p tickwire-account --test facts new_fill_cannot_silently_exceed
```

ACK 和重复成交观察缺陷由独立源码审阅发现并用单独程序实际复现，程序、日志及修复前源码在 `pre-ack/`、`pre-replay-observation/`。终态迟到成交遗漏在准备 USD-M 模型时沿源码发现，真实测试确认；独立审阅同时排除了“减仓额度超过实际仓位仍不冻结”这一较强候选，源码已有一致性检查，证据为 `reduce-consistency-guard.log`。修复只增加历史终态累计检查，照实保留现金和仓位更新。模型没有被记作这些源码问题的发现者。

## 最终命令与平台

```sh
PATH="$HOME/.cargo/bin:$PATH" cargo fmt -p tickwire-account --check
PATH="$HOME/.cargo/bin:$PATH" cargo test -p tickwire-account
PATH="$HOME/.cargo/bin:$PATH" cargo clippy -p tickwire-account --all-targets -- -D warnings
```

原生结果为 `native.log`，Clippy 为 `clippy.log`。Linux 使用 Rust 1.99.1，容器报告 Linux 6.8.0-117-generic aarch64；命令从仓库根目录执行，结果为 `linux.log`：

```sh
docker run --rm --init \
  --mount "type=bind,source=$PWD,target=/work" \
  --mount "type=bind,source=$PWD/.tools/linux-cargo-registry,target=/usr/local/cargo/registry" \
  -w /work -e CARGO_TARGET_DIR=/work/target/linux-account -e RUSTUP_TOOLCHAIN=1.99.1 \
  rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29 \
  bash -c 'uname -srmo; rustc --version; cargo test --locked -p tickwire-account'
```

测试数量：admission 10、edge_cases 8、facts 15、futures 10、recovery 3。`finite_model_instance_uses_four_capital_atoms_and_three_fee_atoms` 直接调用真实公式，核对 L1 有限输入的 H=4、费用预留=3、成交/费用/终态后 H 仍保留。无忽略测试。

模型 [L0](../../../tla/execution_models/account/L0_spot_facts/05_output.md) 与 [L1](../../../tla/execution_models/account/L1_reduce_shadow/05_output.md) 各记录精确配置、命令、最终覆盖计数、源码与工具 hash。root 和 IPC 对最终模型做独立源码映射审阅；market 对 ACK、重复观察与迟到终态修复分别复查。审阅者没有声称重新运行全部 TLC。

日志仅移除机器路径和行尾空白，保留实际命令输出及历史失败。源码和最终证据摘要见 `results.json`；原始失败归档不因后续修复覆盖。完整能力及剩余接口见 [实现规格](../../../specs/implementation/account.md)。
