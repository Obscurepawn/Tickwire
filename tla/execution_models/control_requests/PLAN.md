# 控制请求验证阶梯

源码范围是 `tickwire-control`。L0 验证已认证会话内请求登记与 MockRequestStore；L1 加入会话期限和续租；L2 再组合真实持久请求存储与 owner 恢复。模型逐级验证实际实现，不预先假定尚未接入的 owner 原子提交。

当前调用使用 `&mut Session` 和 `&mut MockRequestStore`，同步完成，没有 await 或共享内部状态。一次 admit/complete 是串行调用；下一调用可重试、改命令、改变 owner 的实际版本或注入 Mock 存储失败。持久后端可在实现 RequestStore 时使用内部事务，但其恢复性质留给 L2。
