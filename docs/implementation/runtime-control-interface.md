# 运行控制与持久请求

运行控制复用 `tickwire-control::Session` 的身份、租约及请求票据。`tickwire-control-store` 为 `RequestStore` 提供真实 WAL：先同步 Received，再允许调用方取得唯一执行票据；完成结果同步后才返回成功。相同请求恢复为 Pending 或保存的 Completed，不在重启时重新分发副作用。

一个目录固定 owner、风险域、日志流代次和容量配置。记录只包含请求键、内容摘要及最终响应，配置、Received、Completed 按同一物理日志顺序恢复。恢复验证每个请求只有一个 Received、Completed 有对应前驱且摘要一致；写入结果不确定时实例停止服务，重开扫描实际持久前缀。

`DurableRequests::create/recover` 使用 `tickwire-journal` 的文件、目录同步和单写者锁。恢复 writer epoch 可以提高，不能低于历史记录。每个请求的控制会话 epoch 与物理 writer epoch 独立。请求总数、Pending 数、响应大小和累计保留字节都有显式上限，不淘汰历史结果。

控制请求的幂等结果不代替业务恢复。Pending 的 operation 由实际 owner 检查其日志和进程状态后完成；不能仅凭 Received 判定副作用完成，也不通过重新执行来探测结果。进程 launcher 持有真实 Child，记录逻辑身份与启动代次，退出和隔离证据必须对应该实例。

核心运行链路完成后统一补模块及全局形式验证；当前通过真实文件重开、请求冲突、未完成请求、完成回执丢失和写入失败测试检查持久边界。
