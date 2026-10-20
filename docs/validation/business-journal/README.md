# 业务 WAL 验证记录

本轮接通真实物理 WAL、规范业务 codec、私有闭包能力和冻结恢复 runtime。测试中的授权决策是显式 Mock；闭包解析、文件持久化和进程终止是真实执行。

| 文件 | 实际结果 |
| --- | --- |
| `red.log`、`red-evidence.json` | 接口桩编译通过后，10 项行为测试失败 |
| `green-initial.log` | 初版实现 10 项通过 |
| `idempotency-red.log` | writer 从 7 变成 8 后，原订单重试错误返回 epoch 不匹配 |
| `gate-red.log` | I/O 等待后已过订单期限，原实现仍返回提交成功 |
| `green.log` | 本机 23 项通过，2 个子进程入口在普通发现时 ignored |
| `linux-green.log` | Linux aarch64 业务 23、物理 WAL 21、codec 13 项通过 |
| `clippy.log`、`fmt.log` | 当前三个相关 crate 的 Clippy 与业务 crate 格式检查 |
| `environment.json` | 真实环境、命令和源码/测试 SHA-256 |

两个新增 Red 来自实现后的源码复核及行为测试，不归因于 TLC。修复分别将完整请求幂等查询放在新意图代次限制之前，并在 WAL 同步完成后重新取得当前门。变更后全部回归通过。

实际子进程测试启动两个文件闭包/WAL 进程，分别在 Prepared 同步后、Aborted 同步并释放后写入就绪标记，父进程随后 SIGKILL。新进程恢复得到相应持有额 22/0，并保持冻结，Exchange 调用数为 0。另一个隔离子进程在相对路径打开后改变 CWD，验证后续 bootstrap 与物理文件仍写在原加锁目录。普通测试中的两个 ignored 是由这些父测试实际启动的入口。

写入失败测试向真实段写入 8 字节后返回错误；同步失败测试在对应 `sync_segment` 调用返回错误。前者验证未封口 Aborted 尾不会释放资金，后者验证 Prepared/Aborted 未获同步完成时不得发送或释放。没有进程掉电实验，也没有延迟测量。

其他覆盖包括：完整业务身份错配、另一意图的合法闭包不能授权当前请求、全部门字段在最终发送前重验、原订单代次保留、终态缺少成交明细时保留占用、迟到成交/费用、重复事实去重、非法事实保留后冻结、闭包缺失不补造、bootstrap 资本错配、物理合法而业务非法的记录拒绝恢复、SyncReplica 不降级，以及 `i128::MAX` 下先中止再追加下一意图的恢复顺序。

复跑命令：

```bash
cargo test --locked -p tickwire-business-journal
cargo test --locked -p tickwire-journal -p tickwire-journal-codec
cargo fmt -p tickwire-business-journal -- --check
cargo clippy --locked -p tickwire-business-journal -p tickwire-journal -p tickwire-journal-codec --all-targets -- -D warnings
```

Linux 镜像和挂载方式与 `docs/validation/journal/README.md` 相同，使用独立 `target/linux-business-journal`。日志只替换机器路径并去掉行尾空白，保留错误、计数和耗时。

业务模型在 `tla/execution_models/business_journal/L0_commit_abort_recovery`。物理 config getter、目录重试同步和 canonical 路径的追加复核记录在 `docs/validation/journal/followup-business-adapter`；既有冻结模型证据保留。
