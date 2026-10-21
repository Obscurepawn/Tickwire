# 静态注册策略示例

本 crate 提供独立用户策略及两个真实可执行入口。`tickwire-counting-v1-worker` 使用状态 schema `0x5457434e0001`；`tickwire-counting-worker` 使用 `0x5457434e0002`，并注册从 V1 到 V2 的迁移方法 `1`。

二者均调用 SDK 的 `registered::run`，处理 `--bootstrap`、`--source-replay`、`--restore-probe` 和 `--replay-rollback`。正常启动由 Host 提供已认证的 Bootstrap 和 SHM 资源；单独运行 binary 不获得交易授权。

Bootstrap 使用 `factory.id = "counting_example"`，并提供配置文件绝对路径、该文件的 `Object { digest, bytes }` 和正数 `revision`。内建演示字段设为 `demo_enabled = false`、`demo_command_file = None`、`demo_minimum_bid_ticks = 0`。发布清单的 `StrategyConfig` 必须绑定同一配置文件及对象。

配置由 `Parameters::bytes()` 生成，字段为 `schema = 1`、`enabled`、`minimum_observations`、`minimum_bid_ticks` 和 `command`。`command` 保存完整 `CanonicalCommand` 的字节；启用时必填，当前示例只接受 Spot Limit New。命令的实例、策略代次、逻辑策略和配置 revision 必须与目标一致。配置解码拒绝未知字段、非规范 JSON 和不支持的订单类型。

每次有效行情回调增加 `observations`，账户回调增加 `reports`。达到配置阈值后，策略用实际当前时间和行情 trigger 构造一次命令，并保存 `spent`。检查点还保留原经济意图对应的动作标识。V1→V2 迁移只改变状态 schema，保留 C 上的计数、动作标识和 `spent`；随后新 ConfigApply 更换目标参数，继续接收输入。新参数改变原动作标识时拒绝安装。

验证入口：

```sh
cargo test -p tickwire-strategy-example --all-targets
```

四项单元覆盖规范配置、实际已核验文件构造用户工厂、C 上迁移保留状态，以及旧回调 S→C 重放后新配置接续不重发历史命令。另有一个原生子进程测试，实际执行 V1 worker 重放持久输入，并核对其 PID、成功退出和完整 C 检查点。完整进程交接使用共同 schema3 发布流程，证据归入 [策略回放验证](../../docs/validation/strategy-replay/report.md)。
