# Linux Spot 策略交接与延迟成交

`./scripts/run-core-chain-linux.sh spot-handover` 在独立 Linux volume 上通过，退出码为 0。实际部署的六个程序与全部构建源码分别固定在[二进制摘要](deployment-binaries-sha256.txt)和[源码摘要](workspace-source-sha256.txt)中。

场景先由 generation 5 的策略提交订单并接收确认，再启动 generation 6 的 Shadow。两个进程消费相同账户前缀后，独立 lifecycle 进程执行 Prepare、Commit、Release。旧策略完成行情退订并退出，随后才释放延迟成交。成交、费用查询与最终账户事实由新的管理者接收。

| 检查 | 实际结果 |
|---|---|
| 旧策略退出 | PID 1966，generation 5，退出码 0 |
| 新策略消费 | PID 1974，generation 6，消费到账户位置 9 |
| 订单管理权 | origin_generation 5，manager_generation 6，owner_revision 2 |
| 成交和费用 | 买入 0.001 BTC；`fee_reconciled=true`、`unknown=false` |
| 余额 | BTC 1.001，USDT 940，未完成订单为空 |
| 日志 | 9 条账户记录、16 条输入记录，无遗漏入账事实 |
| 镜像 | 9 条规范账户记录与源端逐条同序、同字节 |

[账户报告](account-report.json)来自关闭写入者后的离线恢复，因此整体 `frozen=true`。[原始进程日志](logs/exec.stderr.log)记录实际退出和新策略后续消费，[抽取结果](worker-consumed.json)保留两个关键事件。[运行结果](run-result.json)和[控制台](console.log)记录最终校验完成。

该场景覆盖同一执行者中的策略交接。执行者故障后的最终源封口及恢复由[文件级接管测试](../../failover-files/README.md)验证。发布对象、业务状态迁移和回滚恢复的预检另按发布接口开发。
