# 离线报告中的待安装收入基线

输入审批 WAL 已同步、账户 `IncomeBaseline` 尚未同步时，原离线报告遗漏该项，错误显示 `input_facts_not_in_account = 0`。在线恢复原本会补装基线；本次修复诊断计数，不改变账户准入或审批权限。

## 实际文件中断记录

测试 `replay::income_tests::synchronized_income_import_is_pending_until_exact_account_baseline_is_durable` 使用独立临时目录中的真实 `FileStore`、`FileBackend` 和 `FileBlobStore`：

1. 创建 USD-M 固定基线和空账户 WAL。输入 WAL 保存对账开始、收入查询、三个快照及专用 risk-admin Ed25519 审批，共 7 条同步记录。
2. 在审批输入同步后停止写入，关闭文件 owner，再以 writer 5 重开。输入恢复实际验证 HTTP 正文、清单引用、签名和 writer 4 的原接受时刻；账户 WAL 仍为空。
3. 断言待安装数量为 1。原实现真实失败：实际为 0，预期为 1，见 `red.log`。
4. 修复后验证同一报告字段为 1；用原审批验证器核对完整绑定，真实同步账户 `IncomeBaseline`，再次关闭并重开两份 WAL，报告为 0、ledger 为 1，账户仍冻结。

查询样本为空收入列表，签名者明确承担测试基线覆盖断言。测试没有把快照时间或余额一致转换成 Binance 提供的原子覆盖证明。中断是停止在两个实际同步完成点之间，没有模拟掉电或外部进程隔离。

## 修改与命令

`AccountIngress::income_import_bindings()` 只读枚举持久审批对应的完整账户绑定。`pair_input` 只有在账户 WAL 中找到完全相同的 `IncomeBaseline` 时才扣除待安装项；钱包 ID 或摘要的局部匹配均不足够。

```sh
cargo test -p tickwire-app --lib synchronized_income_import_is_pending_until_exact_account_baseline_is_durable -- --nocapture
cargo test -p tickwire-app --lib replay:: -- --nocapture
cargo clippy -p tickwire-app -p tickwire-binance-account --all-targets -- -D warnings
```

`red.log` 是编译成功后的行为失败。`green.log` 为 3 项通过（1 项新文件测试、2 项原报告测试）；`clippy.log` 通过。另将 `income_import.rs` 的测试模块移到所有生产 `impl` 之后，消除 `items_after_test_module`，没有添加 lint 豁免。

`source-hashes.json` 保存最终源码和日志摘要。本次没有运行 Linux、全仓测试或形式验证；冻结 F 的源码与历史结果未修改。
