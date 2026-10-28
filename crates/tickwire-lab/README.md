# Linux 多进程运行环境

在仓库根目录执行：

```sh
./scripts/run-core-chain-linux.sh spot-fill
./scripts/run-core-chain-linux.sh spot-handover
./scripts/run-core-chain-linux.sh spot-handover-custom release
```

省略参数时运行 `spot-fill`。脚本在固定版本的 Rust Linux 镜像内编译程序，为每轮创建独立 Docker volume。部署副本移除调试信息后存入 volume，再计算启动身份中的二进制摘要。`tickwire-md`、`tickwire-strategy-worker`、`tickwire-app` 和 `tickwire-replica-receiver` 分别运行；行情与回报使用真实共享内存通道，JOIN 使用 Unix socket，账户和镜像使用文件 WAL。交易所对端由 `tickwire-lab serve` 提供本地 TLS 协议服务，生成的密钥仅用于本轮环境。

启动器持有实际子进程句柄，将 PID、UID、GID 与启动身份写入私有 admission，再释放启动门禁。执行进程完成账户对账后，对端改变盘口，策略才生成一次买入意图。运行结束后检查：

- 订单累计成交 0.001 BTC，BTC 总额为 1.001，USDT 总额为 940。
- 实际订单与成交查询完成，账户日志包含 `FeesReconciled` 和后续对账激活；离线恢复后的订单费用已闭合，`unknown=false`。
- 输入日志没有尚未进入账户日志的事实。
- 镜像的每条规范记录与账户 WAL 的完整有序前缀一致。
- 策略进程的认证 READY、实际回报游标和持久消费记录一致，最终消费摘要等于从账户 WAL 重新计算的摘要。

`spot-handover` 生成 schema 3 操作。在旧策略尚未发单时，从真实 CONSUMED 固定 S checkpoint 与完整输入闭包，再启动下一代 Shadow。新实例实际恢复 S、发送仅含恢复完成根的 READY 后，启动器才写 ARM，让旧策略真实发单并收到 ACK；成交回报此时暂扣。两代策略消费同一账户前缀后，独立 `tickwire-lifecycle` 进程发起 Prepare。生产 Host 启动旧二进制重放 S→C，新实例安装 C，再由第三身份探针恢复实际 C 输出；执行进程签发 `ReplayPrepared`，协调器采纳这份新 readiness 后才能 Commit、Release。

移交功能场景每秒发送一帧深度更新，基础成交场景每 100 毫秒发送一帧；ARM 后第一帧的实际序号和时间写入 fixture 审计。该负载用于检查进程切换和状态连续性，不用于吞吐验收。

场景逐一核验实际源重放、安装和回滚闭包，以及 S<C、旧动作从待执行变为已消费、新实例与探针都保留已消费状态。原 FixedDemo v1 的位表示“仍有待执行动作”，v2 的位表示“已消费”，校验分别处理。启动器在转送 Commit 前发布下一代授权，收到 Released 并核对 Host 的旧子进程退出记录后才放行迟到成交。最终还检查订单创建者保持旧实例、管理者已换为新实例，成交在持久管理权移交之后入账，并由新策略消费到最终账户日志。

日志与结果复制到 `.tools/lab-results/<本轮编号>/`，其中 `workspace-source-sha256.txt` 记录构建前的工作区源文件摘要，`deployment-binaries-sha256.txt` 记录实际部署副本摘要。Linux volume 保留原始日志、共享资源、部署文件和本轮密钥；脚本退出时只清理自己创建的容器。这些命令按顺序执行，共享编译目录在运行期间保持独占。

已有 Linux 环境可直接运行：

```sh
tickwire-lab generate --scenario spot-fill --out /absolute/new-run --bin-dir /absolute/bin
tickwire-lab run --config /absolute/new-run/scenario.json
```

将 `--scenario` 改为 `spot-handover` 可生成移交场景，二进制目录还需包含 `tickwire-lifecycle`。生成目录只运行一次。修改程序后重新生成目录，使启动身份绑定实际二进制摘要；`run-result.json` 只在上述检查全部成功后写入。

## 显式生产构建

```sh
./scripts/run-core-chain-linux.sh spot-handover release
```

第二个参数只接受 `dev` 或 `release`，省略时保持既有 dev 场景。release 使用工作区现有优化配置（仍启用 overflow checks）；30 秒协调器期限、账户有效期和场景期限保持原值。每轮结果保存 `profile.txt`、完整源码 SHA 和实际部署二进制 SHA。dev 的历史失败证据独立保留，优化构建不替代对象命名、持久化和回放正确性修复。


## 独立静态工厂场景

`spot-handover-custom` 使用实际 `tickwire-counting-v1-worker` 和 `tickwire-counting-worker`，不复用内置策略的回调。各实例的 `Bootstrap.factory` 绑定 `counting_example`、独立规范 Parameters 文件、配置版本和内容摘要；两个二进制及其恢复模板分别纳入发布资料。回滚探针使用第三个身份与自己的完整命令参数，经济动作 ID 与来源相同。

调度与内置 schema3 场景相同：从真实未消费动作的 S 启动 Shadow，随后 ARM 让旧工厂真实下单，旧 binary 重放到 C，目标迁移并安装，回滚探针完成后发布。除共同的账户、旧进程退出和镜像检查外，还逐字段检查 JSON 策略状态：S→C 的 observations/reports 实际增长，spent 从 false 变为 true；迁移保留 C 的计数和 action，只改变 schema；新进程继续到 D 时两类计数继续增加且动作仍已消费。最终结果的 `preflight.custom_counters` 保存 S/C/安装/D 四份实际状态。

```sh
tickwire-lab generate --scenario spot-handover-custom --out /absolute/new-run --bin-dir /absolute/bin
```

此场景的二进制目录还需包含 `tickwire-counting-v1-worker` 和 `tickwire-counting-worker`；脚本会编译、部署并记录这两个程序的 SHA。


## 同进程市场扩容与跨银行交接

```sh
./scripts/run-core-chain-linux.sh spot-market-expansion release
```

`spot-market-expansion` 沿用 counting v1/v2 的真实 schema3 交接。启动账户目录已包含 ETHUSDT（标的 ID 为 `u64::MAX`），初始行情只含 BTCUSDT；固定真实 S 并恢复 Shadow 后，启动器向 MD 发布审批计划，新增 ETH REST/WSS owner，三位消费者分别完成实际 Arm、QUIESCED 与新 JOIN。实验室核对 MD、执行、旧 Active、Shadow 的 PID 与启动时刻保持、MD runtime/effective 与每个消费者的 Activated/effective 一致，再要求旧 Active 的持久新流水位覆盖全体激活水位，随后推进 C、来源重放、安装、回滚探针和原账户费用闭环。

`market-expanded.json` 保存扩容阶段证据，最终 `run-result.json.market_expansion` 包含同一份记录；只有最终来源 C 覆盖已核验扩容水位、原账户/镜像/交接验证也通过时才生成完整运行结果。该 loopback 功能场景不代表生产资格或性能验收。


## 声明窗口重建与同进程安装

```sh
./scripts/run-core-chain-linux.sh spot-rebuild-custom release
```

`spot-rebuild-custom` 使用独立的 schema4 调度。旧 counting-v1 完成实际订单准入和 ACK 后，目标 counting-v2 先以 CaptureOnly 模式读取真实行情与账户前缀。动作集合绑定账户 `Admit` 中的完整规范命令；策略参数中的发单模板不能代替它。窗口封口后才固定 Plans、Manifest 与显式旧版本重建回退策略，并启动签名协调器。

App 的 Prepare 驱动同一 capture PID 安装窗口、持续消费后窗输入、独立旧二进制探针和 kind10 回退根绑定。实验室逐一核对七个固定根、当前 READY、完整 v4 依赖图与安装 C→最终 D 的哈希前缀；只有真实 Commit/Release 和旧 PID 退出后才释放迟到成交。最终仍检查账户、镜像、策略回报与费用闭合，`preflight` 保存窗口、PID、根与实际状态计数。此场景不声明恢复了旧策略未公开的私有历史，也不替代生产资格或性能测量。

安装与回退验证是受当前账户期限约束的冷步骤。开发构建曾在窗口封口到 Prepared 校验间耗时约 24.4 秒，超过本场景 30 秒账户有效期的剩余预算，系统因此关闭交接；该失败保留为成本边界，不通过延长账户有效期取得成功。优化构建验收仍使用相同期限和门禁。
