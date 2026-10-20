# 独立行情进程

`tickwire-md` 独占公共 WebSocket、REST 快照与盘口同步。执行进程和策略进程分别通过真实 JOIN、只读 SHM bank 和各自 cursor 消费完整 topic；执行进程不再代替 MD 向策略发布行情。一个 topic 可含多个标的，同一标的可发布到多个 topic。

配置样例在 `examples/md.json`：两个独立 Binance Spot owner 接收 BTCUSDT、ETHUSDT；topic 101 含两者，topic 102 只含 BTCUSDT。这些 JSON 是部署结构示例。部署控制面填入实际 build/qualification/manifest 哈希、host boot/namespace、账户目录已批准的标的及资产版本、价格 tick/数量 step、唯一资源 ID、当前额度窗口和 CA 文件。示例额度是显式有限预算，不是 Binance 额度表；同 IP 的其他进程须从同一额度权威分配互不重叠的预算。

生成 wire header 和启动：

```sh
cargo build -p tickwire-md --bin tickwire-md
chmod 700 /opt/tickwire/md /opt/tickwire/md/admissions
target/debug/tickwire-md --resource /opt/tickwire/md/topic-101.json --output /opt/tickwire/md/topic-101.bin
target/debug/tickwire-md --resource /opt/tickwire/md/topic-102.json --output /opt/tickwire/md/topic-102.bin
target/debug/tickwire-md --config /opt/tickwire/md/md.json
```

`--resource` 只根据已批准身份和容量生成布局；不会生成批准哈希。`target_arch` 来自 identity.arch：x86-64 为 1，AArch64 为 2。生产运行使用 release 构建时，相应更新获批 executable hash。

| 字段 | 作用 |
| --- | --- |
| `identity`、`clock_generation` | MD 进程完整身份及共享主机单调时钟域 |
| `feeds[].owner_id` | 每个网络 owner 独立的额度幂等命名空间 |
| `feeds[].source` | `spot_production`、`spot_test`、`usdm_production` 或 `usdm_test`；固定 TLS 主机名随产品选择 |
| `stream_address`、`snapshot_address` | 实际 TCP 地址，仍按固定官方主机名验证证书 |
| `trust_anchors` | 有界 DER 根证书文件，不关闭 TLS 校验 |
| `instrument` | 完整不可变目录绑定；USD-M 的 `expiry_utc_ns` 为空表示永续，非空表示交割合约 |
| `topics` | 此标的发布到的 topic ID 集合；每个 topic 内标的唯一且共享目录 hash/version |
| `snapshot_limit` | 实际 REST 深度上限；不足范围保留 Unknown |
| `topics[].resource` | 4096 字节资源头；含共享 arena 和 coverage feature |
| `quota_directory` | 有锁的本地额度状态；原子替换并同步，每次重启保留旧 grant 占用 |
| `quota` | 同进程全部公共 owner 共用的实际规则/窗口/有限 grant；过期后关闭请求，不重造预算 |
| `control_directory` | JOIN 请求的持久幂等记录 |
| `admission_directory` | 权限 0700，可信 launcher 写入实际 PID/UID/GID 与精确订阅批准 |
| `maximum_idle_ns` | mio 最长等待；网络 owner 各自给出更早 deadline |

执行订阅使用 `examples/exec-client.json` 的精简形式，填入实际 MD PID/UID/GID、exec 身份和独立 cursor/request ID，然后生成 `ConnectConfig`：

MD 在启动和 bank 轮换后将实际资源头原子写入原文件名追加 `.active` 的文件。客户端模板的 `resource` 指向这个实际资源头，其中包含轮换时确定的 `cutover_seq`、`first_seq` 和 descriptor hash。

```sh
target/debug/tickwire-md --client /opt/tickwire/md/exec-client.json --output /opt/tickwire/md/exec-connect.json
```

执行 launcher 在 exec 实际 PID 已确定后写 `Admission`：`client` 为生成的完整 `ConnectConfig`，其余字段为 `pid/uid/gid/supervisor/supervisor_pid/snapshot_id`。`supervisor` 是获批 role 1 的完整 `IdentityConfig`，`snapshot_id` 每次 JOIN 独立。可以调用 `server::register_admission`，也可以使用：

```sh
target/debug/tickwire-md --admit /opt/tickwire/md/exec-admission.json --directory /opt/tickwire/md/admissions
```

MD 按真实 Unix peer credentials 读取 `{pid}-{control_epoch}.json`，逐字段核对身份。单独知道 socket 路径不产生读取资格。

策略使用 `Bootstrap.external_market` 放置同型 `ConnectConfig`，本地身份为 role 3；`HostConfig.md_admission_directory` 指向上述私有目录。Host 持有实际 Child 后登记批准，再放行 worker 直接 JOIN MD。intent/report 两个通道仍连接 exec；完整 JOIN 快照、ring records、arena payload 和决策输入继续进入 RecoveryWriter。每个 worker 订阅一个完整 topic，多个策略可各自订阅同一或不同 topic。

断档或容量故障通过独立控制通道使订阅失效；旧 bank 不原地复用。控制面提供更高 ring/feed generation 和新 resource ID 的后继资源文件后，MD 在旧 peer 退出且重新获得完整盘口时发布新 bank。每个 topic 最多保留 32 个旧 owner，达到上限时结束进程，由控制面以新进程身份启动。订阅控制 lease 到期也关闭读取；新会话使用新 epoch、cursor、request 和 snapshot ID。
