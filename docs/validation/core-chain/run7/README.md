# Linux 多进程 Spot 成交

本轮运行退出码为 0。[最终结果](run-result.json)记录了成交入账、完整账户镜像和策略回报消费三项检查。源码对应 [412 个文件摘要](workspace-sources.sha256)，部署程序对应[二进制摘要](binaries.sha256)；[环境记录](environment.json)保存固定镜像、Linux 内核、Rust 版本及 WAL 所在文件系统。

`tickwire-md`、`tickwire-strategy-worker`、`tickwire-app` 和 `tickwire-replica-receiver` 是独立进程。行情与回报通过 Linux 共享内存传递，JOIN 使用 Unix socket，交易和复制使用实际 TLS 连接，账户、输入和镜像日志写入 Docker volume。

运行顺序为：加载快照并追平行情增量、完成账户对账、策略发布买入意图、执行端持久保存 Admit 并发送订单、处理 ACK 和私有成交、镜像复制、策略消费最终账户前缀。实际输出见[执行进程日志](logs/exec.stderr.log)和[交易协议服务记录](fixture.jsonl)。

| 检查 | 结果 |
|---|---|
| 订单 | `lab-spot-1`，累计成交 0.001 BTC，剩余数量为 0 |
| 账户 | BTC 总额 1.001，USDT 总额 940 |
| 账户 WAL | 4 条记录，离线重放没有丢弃尾部 |
| 输入 WAL | 9 条记录，没有尚未进入账户日志的事实 |
| 镜像 | 4 条规范记录与源账户日志逐条同序、同字节 |
| 策略回报 | 实际消费到账户序号 4；完整输入摘要与账户 WAL 重算结果一致 |

[策略消费证明](worker-consumed.json)绑定真实子进程身份、通道和游标身份、READY 序号、账户水位、账本及检查点摘要。验证器同时读取认证启动记录和实际共享内存消费状态，不以单独一条日志作为消费完成依据。

[账户报告](account-report.json)来自冻结状态的离线恢复。该轮没有独立的 `FeesReconciled` 记录，因此订单保留 `fee_reconciled=false`，恢复结果中的 `unknown=true` 和 `fees_pending=true` 表示还需要订单及完整成交分页核对。它们不改变本轮已经重放的成交和余额。

本轮检查了普通复制批次的持久化前缀；尚未执行最终 `SourceRecoverySeal`、隔离凭据和新执行者恢复。本轮也未执行策略交接。

复现入口：

```sh
./scripts/run-core-chain-linux.sh
```

脚本为每次执行创建新 volume，保存结果并回收本轮容器。上述文件保留本轮原始结果，后续源码变化对应新的运行目录。此前定位出的六次运行失败见[失败记录](../startup-failures/README.md)。
