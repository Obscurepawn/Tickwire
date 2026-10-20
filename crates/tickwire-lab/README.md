# Linux 多进程运行环境

在仓库根目录执行：

```sh
./scripts/run-core-chain-linux.sh spot-fill
./scripts/run-core-chain-linux.sh spot-handover
```

省略参数时运行 `spot-fill`。脚本在固定版本的 Rust Linux 镜像内编译程序，为每轮创建独立 Docker volume。部署副本移除调试信息后存入 volume，再计算启动身份中的二进制摘要。`tickwire-md`、`tickwire-strategy-worker`、`tickwire-app` 和 `tickwire-replica-receiver` 分别运行；行情与回报使用真实共享内存通道，JOIN 使用 Unix socket，账户和镜像使用文件 WAL。交易所对端由 `tickwire-lab serve` 提供本地 TLS 协议服务，生成的密钥仅用于本轮环境。

启动器持有实际子进程句柄，将 PID、UID、GID 与启动身份写入私有 admission，再释放启动门禁。执行进程完成账户对账后，对端改变盘口，策略才生成一次买入意图。运行结束后检查：

- 订单累计成交 0.001 BTC，BTC 总额为 1.001，USDT 总额为 940。
- 实际订单与成交查询完成，账户日志包含 `FeesReconciled` 和后续对账激活；离线恢复后的订单费用已闭合，`unknown=false`。
- 输入日志没有尚未进入账户日志的事实。
- 镜像的每条规范记录与账户 WAL 的完整有序前缀一致。
- 策略进程的认证 READY、实际回报游标和持久消费记录一致，最终消费摘要等于从账户 WAL 重新计算的摘要。

`spot-handover` 在旧订单得到 ACK 后暂扣成交回报，从旧策略真实 CONSUMED 读取固定 checkpoint 和完整输入闭包，生成 schema 2 发布清单、迁移集与回滚计划。生产加载器启动下一代 Shadow，实际迁移回调和第三身份的独立回滚探针都成功并持久保存输出后，新策略才发送带完整 completion 闭包的 READY。场景验证两份恢复输出保留已消费动作，防止重发旧订单。两代策略消费同一完整账户前缀后，独立的 `tickwire-lifecycle` 进程签发 Prepare、Commit、Release，执行进程返回签名的实际应用收据。启动器在真实 Committed 收据之前按顺序发布下一代授权，收到 Released 且核对 Host 的旧子进程退出记录后才放行迟到成交。最终还检查订单的创建者保持旧实例、管理者已换为新实例，成交在持久管理权移交之后入账，并由新策略消费到最终账户日志。

日志与结果复制到 `.tools/lab-results/<本轮编号>/`，其中 `workspace-source-sha256.txt` 记录构建前的工作区源文件摘要，`deployment-binaries-sha256.txt` 记录实际部署副本摘要。Linux volume 保留原始日志、共享资源、部署文件和本轮密钥；脚本退出时只清理自己创建的容器。两条命令按顺序执行，共享编译目录在运行期间保持独占。

已有 Linux 环境可直接运行：

```sh
tickwire-lab generate --scenario spot-fill --out /absolute/new-run --bin-dir /absolute/bin
tickwire-lab run --config /absolute/new-run/scenario.json
```

将 `--scenario` 改为 `spot-handover` 可生成移交场景，二进制目录还需包含 `tickwire-lifecycle`。生成目录只运行一次。修改程序后重新生成目录，使启动身份绑定实际二进制摘要；`run-result.json` 只在上述检查全部成功后写入。
