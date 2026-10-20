# AccountJournal 并行测试的 fork 锁继承窗口

F 全仓 Linux 运行中，`historical_verifier_failure_and_forged_admission_image_stop_recovery` 在关闭旧 owner 后重新打开 WAL，收到 `WouldBlock`；同一测试 binary 正在执行启动子进程的测试。原始单次失败见 `original-failure-excerpt.log`，来源是冻结快照 `current-system-check-20261020T203009Z` 的 `linux-tests.log`。

复查原 binary 时，单独执行该测试、再以 32 个测试线程运行完整 binary 30 次，全部通过，见 `original-linux.log`。独立副本只给已有子进程增加 300 ms 的 `pre_exec` 停顿后，17 项测试也通过，见 `amplified-attempt.log`。这些重跑未再次自然触发原始偶发失败。

`fork_window.rs.txt` 单独链接冻结版本的真实 `FileBackend`，用两次 Unix socket 系统调用把真实子进程停在 fork 与 exec 之间：父进程关闭 WAL owner，仍无法重新获得锁；放行子进程完成 exec 和退出后，同一 WAL 立即可以重开。`controlled-window.log` 保存实际结果。这确认了测试夹具可以遇到的锁继承机制，未对原始失败的调度作事后追踪。

修复仅改 `crates/tickwire-account-journal/tests/persistence.rs`：普通测试持共享读锁，唯一启动子进程的测试持独占写锁，避免它继承其他并行测试的打开文件。锁在测试入口获取，`Temp::new` 不再次获取，子进程 exec 后使用独立地址空间中的测试锁。WAL 的生产锁、关闭和重新打开行为未改；现有 owner 冲突断言和真实子进程恢复测试保留。

验证命令：

```sh
cargo test -p tickwire-account-journal --test persistence -- --test-threads=32 --nocapture
cargo clippy -p tickwire-account-journal --test persistence -- -D warnings
```

修复后原生与 Linux 各 17 项通过，两端 Clippy 均通过。Linux 首次缺少 Clippy 组件，安装后完成检查，相关记录一并保留。聚焦结果分别保存在 `native-green.log`、`linux-green.log` 和对应 `*-clippy.log`。`source-hashes.json` 固定修复后的测试、相关生产存储源码和复现程序；本次没有运行全仓检查。
