# Broadcast ring 验证阶梯

验证对象是 `tickwire-shm` 已实现的单发布者、独立消费者游标和借用回收协议。L0 在真实容量 64 下检查发布、借用、连续释放、缓存最小水位和 Freeze；源码没有实现的退出认证、bank 切换与 arena 回收另列后续级。

发布者由 `&mut self` 串行操作，各消费者各自拥有 `RefCell<Reader>` 与唯一可写 cursor mapping；消费者可以与发布者并行。写入分成 claim、逐槽写 payload、Release 发布三步，普通读取只发生在 Acquire 观察发布后。游标 Release/Acquire 的内存顺序是本级的底层契约，Linux seal、只读页原子 load 和 CPU 指令由独立 Rust/Linux 测试验证。

| 级别 | 新增关注点 | 输入范围 |
| --- | --- | --- |
| L0_publish_borrow_reclaim | 槽位在借用期间不被复用，发布和回收水位连续，pending JOIN 与真实 cursor detach | capacity=64；1/2 读者；窗口 1/2；普通前缀与一次合法 64 条批发布后的跨 wrap 前缀 |
| L1_identity_rotation | 身份代次、旧 cursor、Freeze 与新 bank 接管 | 实现控制面后扩展 |
| L2_exit_reclaim | 消费者停止、进程退出证据与映射回收 | 实现退出认证后扩展 |

L0 采用源码转录和任意进度停顿。进程停止、忘记释放借用、遗失 claim 对安全性的效果均为相关动作不再发生；无公平性假设，也不声明最终推进。允许同一身份加入一次，序号与调用次数有明确输入边界；后续级加入身份循环。源码变动后的语义审查与逐行 anchors 检查共同决定旧结果能否沿用。
