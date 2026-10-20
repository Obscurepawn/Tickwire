# 撤单与查询的编码校验

账户模块按主设计修正了过期快照对撤单和查询的阻断后，独立核对发现编码器仍要求所有命令的风险策略版本相同。合法的非增险 Admission 因此无法编码。新增回归也捕获了 image 增险标识与完整命令类别不一致仍可编码的问题。

先更新接口，再运行 `cargo test -p tickwire-account-codec --test nonincreasing`：编译成功，3 项失败、1 项通过，输出为 `red.log`，修复前校验器为 `validate-before.rs`。随后从完整命令分类，核对 `AdmissionImage.increases`，只对增险命令比较风险策略版本。

四项测试分别检查：

- 已有订单的 Cancel/Query，在过期账户基线和旧风险版本下可编码、解码并顺序恢复；原订单预留保持。
- New 的 image 不能将 `increases` 改为 false。
- Query 的 image 不能将 `increases` 改为 true。
- New 仍需匹配记录中的风险策略版本。

实际命令：

```sh
PATH="$HOME/.cargo/bin:$PATH" cargo test -p tickwire-account-codec --test nonincreasing
PATH="$HOME/.cargo/bin:$PATH" cargo test -p tickwire-account-codec
PATH="$HOME/.cargo/bin:$PATH" cargo clippy -p tickwire-account-codec --all-targets -- -D warnings
PATH="$HOME/.cargo/bin:$PATH" cargo fmt -p tickwire-account-codec --check
```

macOS 与 Linux aarch64 各通过 20 项测试，Clippy `-D warnings` 与格式检查通过。Linux 使用账户验证记录中的固定 Rust 1.99.1 容器和挂载方式，将测试包改为 `tickwire-account-codec`。`green.log`、`linux.log`、`clippy.log` 和 `fmt.log` 保存这次增量检查。源码与日志摘要见 `results.json`。

编码格式没有变化，原基准样本继续逐字节比对。此次没有给无状态编码器建立新的形式状态机；账户 L0/L1 没有建模 Cancel/Query，这两类动作的版本与过期检查由本组测试覆盖。账户日志和最终发送的组合验证由相应模块继续完成。
