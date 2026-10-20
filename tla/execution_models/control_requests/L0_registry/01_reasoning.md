# 抽象与范围

使用忠实转录模式，同时显式转录 Mock 提供的 fail_next 故障。每次调用是同步串行的：ArmFailure 只能发生在调用之间；Admit 的首次 lookup 会消耗失败标志，不会凭空在 lookup 与 insert 之间插入另一个写者。后端其他实现的 I/O 歧义不在此级范围。

keys 是已经认证的完整键的有限集合；commands 是无碰撞摘要的有限符号。命令 c 的 expected version 固定为 c-1，current_version 是每次请求的有界输入。response 符号代表经过 ExecutionTicket 关联检查的完整响应和版本；BadCompletion 单独覆盖校验失败不调用 Store、也不消耗 fail_next。Correlated completion 只能使用曾经返回票据的键及其固定摘要。

entries 保存原摘要和终态响应，0 表示不存在；issued 是测试观察器，累计某键交付 Execute 的次数；registered/firstResult 是不可擦除的观察器，分别检查登记记录不被淘汰及已完成内容不被改写。observedRetry 记录已有同摘要记录的查询见证，检查返回 Pending/Completed 不受后来 current_version 影响。

总记录容量按配置缩小为 1/2/3；pending 上限也缩小以覆盖两种不同容量门禁。生产上限 256 和真实 SHA-256、64 位算术、字节码解析由 Rust 测试与源码审核覆盖。模型不涉及实际副作用、清理策略、owner 重启、SQL/KV 或磁盘 durability。票据只交付一次不等于调用者只能使用一次外部资源。
