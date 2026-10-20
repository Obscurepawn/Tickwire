# Project Specs

这里保存 Tickwire 的生产目标、设计决策、验收条件和接续开发入口。设计交付与交易系统实现分别登记。

## Drafts

- 无待补全的本轮设计规格。

## Active

- [Rust 实现与验证](20261020-implementation.md)：接口、TDD、Specs 与 TLA+ 流程；外部存储可用 Mock，Binance 连接器完整实现并由官方协议 Mock 测试。

## Completed

- [Binance 现货与合约生产交易系统详细设计](20261020-binance-ultra-low-latency.md)：v0.3 设计产物完成，包含多进程/多策略、共享广播、复制与接管、二进制 ABI、风控、部署及配置控制 API；Rust 实现正在进行，生产资格验证尚未开始。
