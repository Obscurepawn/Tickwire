# Binance depth_sync 验证计划

目标是检查真实 `DepthSync` 在异步网络完成回调之间如何保护盘口同步轮次：旧连接/旧请求不能改变当前盘口，订阅与首消息先于快照请求，丢失缓存后必须重建，整批回放失败不交付部分 Live。

源码范围为 `crates/tickwire-binance/src/depth_sync.rs`，其 Book policy 和存储状态来自 `crates/tickwire-book/src/lib.rs`。实现与 20 项新增行为/协议测试已冻结；root 已独立审阅。源码摘要由 `docs/validation/binance-depth-sync/SOURCE.sha256` 固定。模型独立归档，不修改这些源码或原证据。

真实并发模型是单 owner、公共方法独占 `&mut self`，一个调用执行完才开始下一个。网络回调可以迟到、重复或对应已关闭连接，但不会在同一个 snapshot 回放循环中插入另一个公共调用。模型用递归纯函数原子折叠整个 FIFO，不制造 Rust API 无法出现的中途观察。

| 层 | 新关注点 | 交付 |
| --- | --- | --- |
| L0_coordinator | 有限缓存、连接/同步代次、请求令牌、期限和原增量时刻 | Spot/USD-M 各主配置及扩大缓存配置 |
| 后续（未实现） | 真实会话调度与多源发布/增险调用方组合 | 由对应 owner 实现后另建层 |

一个数量变量代表已经通过序列检查的具体价位更新值，作为内容见证；它不代表完整数组或未知外侧深度。数组排序、精确十进制、TLS/framing、元数据资格与账户风险门禁不在本层。有限深度知识由既有 book_state L1 和本模块真实 Rust 回归支撑，跨模型没有机器检查 refinement。
