# JOIN owner 验证计划

源码根为 `crates/tickwire-join`，同时读入 `tickwire-shm` 的实际 pending cursor、激活及退出检查。L0 验证固定已认证身份下的准备、交付、追平、超时和协作回收；L1 留给持久 owner 恢复、订阅代次重用和退出 holder 证明。既有 ring L0 和 control L0/L1 分别提供槽位保护与认证请求前提，本级没有宣称自动完成它们之间的形式细化。

owner 的发布和控制方法持有同一 `&mut JoinOwner`，构造闭包执行期间不能重入发布。消费者是独立进程，可以在 owner 构造其他订阅、等待 socket 或处理控制调用之间读取并推进实际 cursor。所有者只在有效 READY 后采纳 pending cursor 的 released。副作用之后构造、交付和存储错误均保留门禁。

容量固定为源码最小值 64。主配置在 S=4 的有限发布窗口中枚举构造/交付失败与超时；两个 wrap 配置从合法可达前缀开始：全部读者在 S=4 登记并完成映射/资源确认，owner 发布 5..68，读者已释放并应用 5..67，但仍 pending。模型继续枚举第 68 条的活引用及第 69 条的槽位复用。该前缀由真实 SDK 可执行步骤构成，并与 Linux Rust 测试的 64 条门禁见证一致。
