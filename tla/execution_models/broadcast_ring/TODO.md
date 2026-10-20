# 广播 ring 后续动作

- L0 已完成：五个 cfg、全部动作覆盖、源码哈希与 anchors；risk_core 独立审阅通过。结果见 [L0 输出](L0_publish_borrow_reclaim/05_output.md)。
- L1：控制面实现后加入 identity/generation 变化与 bank 轮换。
- L2：退出认证实现后加入 fail-stop 与映射持有者退出证据。
- 全局 `tla/MEMORY.md` 和 `tla/ROADMAP.md` 由 risk_core 协调合并，保留 execution L2 的收口状态。
