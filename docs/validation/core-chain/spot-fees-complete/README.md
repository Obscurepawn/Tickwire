# Linux Spot 成交与费用核对

`./scripts/run-core-chain-linux.sh spot-fill` 在独立 Linux volume 中运行通过，退出码为 0。固定源码和实际部署程序分别记录在[源码摘要](workspace-source-sha256.txt)与[二进制摘要](deployment-binaries-sha256.txt)，构建和运行输出见 [console.log](console.log)。

本轮在私有成交入账后，通过真实订单查询、完整成交分页、`FeesReconciled` 和再次账户激活完成费用核对。[协议服务日志](fixture.jsonl)记录实际 `GET /api/v3/order`、`GET /api/v3/myTrades` 及账户快照请求。

| 检查 | 结果 |
|---|---|
| 成交与余额 | 买入 0.001 BTC；BTC 总额 1.001，USDT 总额 940 |
| 费用与订单 | `fee_reconciled=true`、`unknown=false`，待核对订单为空 |
| 账户与输入 | 8 条账户记录，16 条输入记录，无待入账事实 |
| 镜像 | 8 条账户规范记录逐条同序、同字节 |
| 策略消费 | 实际消费到账户序号 8，完整输入摘要与账户 WAL 一致 |

[账户报告](account-report.json)、[运行结果](run-result.json)和[策略消费证明](worker-consumed.json)保留原始输出。账户报告来自冻结状态的离线恢复，因此整体 `frozen=true`；订单的费用责任已闭合。

本轮验证正常成交和费用维护。最终源封口、执行者接管和策略交接分别由独立场景验证。[运行元数据](execution.json)记录保留卷名，脚本已回收其容器。
