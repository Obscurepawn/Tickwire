# 账户日志验证

`tickwire-account-journal` 使用真实 `Journal`、`FileStore` 和独占文件锁，保存完整账户基线与八类有序恢复输入。当前代码没有网络发送和第二份资金预留。

最终原生与 Linux 各通过 18 项测试：1 项独立字节样本单元测试、17 项集成测试（其中 1 项是子进程入口）。两端 Clippy、格式和文档校验均通过；环境记录见 `environment.json`。

## 行为失败与实现

| 记录 | 实际结果 |
|---|---|
| `test-compile-fix.log` | 首次测试变量遮蔽导致编译错误，随后修正；不计为行为 Red |
| `first-red.log`、`pre-implementation.rs.txt` | 可编译占位接口，12 项测试全部因 `create → Poisoned` 失败 |
| `green-initial.log` | 文件后端与恢复实现完成，12 项通过 |
| `history-red.log` | 空历史前缀占位返回零摘要，历史绑定测试失败 |
| `history-content-address-red.log`、`history-content-address-pre.rs.txt` | root 独立审阅发现前缀对象摘要与恢复对象服务内容地址不一致；实际 `Persistence::put_object` 比较复现 |
| `green.log` | 修改对象摘要为 `SHA256(bytes)` 后完整回归 |
| `linux.log` | 固定 Linux 镜像运行同一组真实文件与子进程测试 |
| `clippy-first.log`、`clippy-test-style.log` | 测试代码排列、数组分块和条件写法的静态检查结果，均已修正 |
| `clippy.log`、`linux-clippy.log` | 最终静态检查 |

空链和每条记录链继续保留各自域标签；对象本身由 `TWAF0001` 区分类型。`reference_history.py` 不调用 Rust，独立生成确定 bootstrap、空前缀及两个不同 writer 的完整 128 字节样本。记录输入取自既有账户 codec 的独立最小现金流样本。`history-vectors.json` 是固定比较对象，测试不会自动用运行中的实现改写它。

## 实际完成点

文件创建在基线、bootstrap 各自同步文件与目录后返回。记录追加在物理批次同步后才返回收据。故障包装仍操作真实文件：一条测试写到半批后返回错误；另一条完成实际同步后丢失成功回执。两者都会使日志 poisoned，公开序号和前缀保持原样，重开分别丢弃未完成尾部或恢复完整记录。

子进程测试执行同一测试二进制，写入准入和成交后调用 `process::exit(73)`，跳过 Rust 析构；父进程重新打开文件并恢复冻结账户。该结果覆盖进程退出，不表示实际掉电测试。

前缀错误测试先添加一个未完成物理帧，再提供错误受信收据，断言文件保持逐字节不变。结构错误、闭包核验失败和账户重算失败均不能返回部分可用账户。

## 重现

从仓库根目录执行：

```sh
python3 docs/validation/account-journal/reference_history.py
cargo test -p tickwire-account-journal
cargo clippy -p tickwire-account-journal --all-targets -- -D warnings
cargo fmt -p tickwire-account-journal -- --check
docker run --rm --mount type=bind,src="$PWD",dst=/work -w /work \
  -e CARGO_TARGET_DIR=/work/target/linux-account-journal \
  -e CARGO_HOME=/work/.tools/linux-cargo-registry \
  rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29 \
  cargo test -p tickwire-account-journal
```

Linux Clippy 使用相同镜像和挂载，将末尾替换为 `cargo clippy -p tickwire-account-journal --all-targets -- -D warnings`。

## 独立审阅与范围

risk_core 只读审阅了四个核心源码文件，确认创建顺序、完整结构核验、每次 Admit 之前的前缀、同步后发布、失败 poison、恢复冻结和拒绝新增 Admit，未发现阻断项；没有独立重跑测试。这次审阅没有覆盖跨 crate 内容地址互操作，后者由 root 的候选和实际 Red 单独验证修复。

`AppendReceipt` 证明持久字节，不能替代活跃准入或零输出证据。历史 verifier 由后续组合层提供，没有默认放行实现。本模块没有解冻、基线切换、网络重发或账户资金 reducer；账户与物理 WAL 既有模型不能自动证明新增组合。最终源码、样本和接口摘要保存在 `source-hashes.json`。
