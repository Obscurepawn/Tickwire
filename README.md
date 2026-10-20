# Tickwire

面向 Binance Spot 与 USDⓈ-M 的 Rust 超低延迟交易系统。v0.3 详细设计已完成，当前正在实现盘口、风险、IPC 与执行内核。外部存储和交易所通过服务接口接入，已有本地 MockService 闭环；完整生产链路仍在开发。

目标设计采用独立行情进程、每策略实例独立进程、按账户风险域独立的执行进程，以及控制监督与紧急执行进程。固定记录共享广播 ring 已实现 Linux 映射、单写权限、受保护借用和协作退出，并通过最多 32 个消费者进程的传输测试。业务进程、bank 切换、真实持久化意图、后台复制与控制面共识仍在开发。

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
- [设计交付核验](docs/design-review.md)、[实现状态](specs/20261020-implementation.md)与[开发规格](specs/INDEX.md)。
- [高性能盘口源码对照](docs/research/order-book-implementation.md)、[批量优化与散布负载取舍](docs/validation/book-optimization/report.md)。
- [共享 ring 接口](docs/implementation/shm-ring-interface.md)、[Linux 映射规格](specs/implementation/shm-mapping.md)与[真实多进程测试](docs/validation/shm/README.md)。

本地开发使用固定的 Rust 1.99.1：

```sh
./scripts/check-rust.sh
./scripts/check-linux.sh  # 需要运行中的 Docker
cargo run -p tickwire-mock --example spot-roundtrip
./scripts/check-tla.sh
```

示例串起归一化盘口、策略、定点风险、IPC 编解码、Mock journal、Mock 交易所、成交/撤单与冻结恢复，不需要凭据。`tickwire-execution::ports` 定义配置、对象存储、journal 与交易所边界；`tickwire-mock` 提供内存关系库/KV、丢收据、写失败和 ACK 丢失等确定性调度。

盘口性能基准运行 `cargo bench -p tickwire-book --bench depth_apply`；批量与散布负载使用 `batch_apply`、`sparse_apply`，完整前后对照命令见[报告](docs/validation/book-optimization/report.md)。计时覆盖单事件 `apply → view → best bid/ask`，排除解码、排队和 I/O。生产完整链路的性能数字仍是待验证目标。
