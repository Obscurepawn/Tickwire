# L0 输入

输入是已实现的 Binance JSON 盘口协调器，目标明确为启动、请求重试、连接重建与缓存回放。此前 book_state L0/L1 已检查 Book 生命周期与有限深度知识，但没有检查 snapshot/delta 缓存协调。本层直接展开所需的真实 policy，不把此前 PASS 当作自动组合证明。

源代码：`crates/tickwire-binance/src/depth_sync.rs:139-485`；Book 的 `BinanceSpot/BinanceUsdM` policy 在 `crates/tickwire-book/src/lib.rs:182-224`，install/apply/rebuild/view 在 `crates/tickwire-book/src/lib.rs:590-761`。固定配置来自已验证的 Config 构造器；真实 JSON 字段与整数网格验证由 feed 完成。

root 已独立源码审阅通过，测试证据为 macOS/Linux 各 108 项全 crate（其中本模块新增 20 项），不作为本层 TLC 运行结果。参考 [实现验证报告](../../../../docs/validation/binance-depth-sync/report.md)。
