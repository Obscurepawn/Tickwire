# Tickwire

面向 Binance Spot 与 USDⓈ-M 的 Rust 超低延迟生产交易系统。目前完成 v0.3 详细设计及可核验契约，交易引擎尚未实现。

系统采用独立行情进程、每策略实例独立进程、按账户风险域独立的执行进程，以及控制监督与紧急执行进程。行情通过 Disruptor 式共享广播 ring 分发，多策略可以独立新增、升级、回滚和退出。默认订单路径使用本地持久化意图和后台异步复制；控制面共识负责低频配置与授权协调。

- [生产系统设计](docs/design/binance-ultra-low-latency.md)：进程、协议、风控、订单、恢复、完整链路延迟与上线门禁。
- [共享内存 IPC 机制](docs/design/shared-memory-ipc.md)与[二进制 ABI](docs/design/ipc-abi.md)：广播游标、bank 切换、精确布局、消息编号、资源权限和编码向量。
- [多策略生命周期](docs/design/strategy-lifecycle.md)：独立发布、Shadow、存量订单/持仓原位移交、跨域协调和回滚。
- [复制与故障接管](docs/design/replication-and-failover.md)：本地日志、异步副本、执行权隔离、恢复证据及可选同步画像。
- [风险计算](docs/design/risk-calculations.md)：现货与合约的预留、费用、保证金、减险规则及数值案例。
- [部署与 SLO](docs/design/deployment-and-slo.md)：主备拓扑、16 Active＋16 Shadow 参考容量、CPU/内存/磁盘预算及测量合同。
- [配置文件与控制 API](docs/design/configuration.md)：校验、计划、发布、回滚、本地文件持久同步、进程授权投影和崩溃恢复；附[参考配置](config/reference.toml)、[生产输入模板](config/production.template.toml)、[配置 schema](specs/contracts/system-config.schema.json)和[OpenAPI](specs/contracts/control-api.openapi.json)。
- [生产验证方案](docs/verification-plan.md)：单元/性质测试、真实多进程集成、故障注入与 vibe-tla/TLC；[设计契约校验命令](scripts/README.md)。
- [交互式架构图](docs/assets/tickwire-architecture.html)：本地浏览器打开，支持主题切换、缩放与导出；[其余设计图](docs/assets/README.md)。
- 调研：[本地技术文章](docs/research/local-notes.md)、[币安官方协议](docs/research/binance-protocols.md)、[开源系统与 Rust 组件](docs/research/opensource-rust.md)、[共享内存与广播组件](docs/research/shared-memory-ipc.md)。
- [设计交付核验](docs/design-review.md)与[开发规格](specs/INDEX.md)。

性能数字是待验证目标。当前校验覆盖设计文件、布局、算术、配置与图片；生产资格需要后续实现和运行证据。
