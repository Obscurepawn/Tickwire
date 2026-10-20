# 后续接线

当前 Owner 保存最多 M 个累计登记，含失败、过期及已回收条目；耗尽后返回 Capacity，保留旧代次历史。要允许长期反复接入，先实现持久代次高水位和 owner 恢复规则，再建立 L1。

真实 Linux 三进程测试由 owner 主进程、独立 exec Supervisor、独立 exec consumer 组成；可信最终 JOIN/header 从 launcher 交接，实际 bank/snapshot/cursor 使用 SCM_RIGHTS。常驻 Supervisor 分发和死亡 holder 回收尚未实现。生产集成还需提供真实完整状态源和控制 RequestStore；测试中的 MockRequestStore 不改变实际共享内存门禁。
