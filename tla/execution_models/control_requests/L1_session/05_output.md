# 输出

PASS：请求登记与同一 CLOCK_MONOTONIC 时钟域的会话期限组合。检查每键最多交付一次票据、先登记再交付、同摘要重试先于版本比较、不淘汰已登记记录、终态响应不可覆盖、容量及 pending 门禁。检查时间倒退/到期后不能再交付新票据，续租要求正确 nonce 与更高 revision；关闭后仍可保存旧票据的结果。

## 忠实度与覆盖

变量与动作锚定实际 Rust 源码，L0 由 ipc_codec 独立审阅；L1 的时间赋值顺序、关闭后完成、错误 nonce/revision 处理也由 ipc_codec 独立审阅。每个配置所有 Next 动作均生成后继；本级引用源码行随 anchors.lock 检查，源码 SHA-256 存在 results.json。

摘要/响应使用有限符号，认证解析与响应关联用 Rust 验证；生产 256 pending、2^63 上界、SHA-256 与 syscall 不由此有限模型证明。Mock 存储失败是下一调用进入时出错；没有假设事务型持久后端已实现。实际业务副作用、owner 崩溃恢复、经济 operation 去重和磁盘 durability 仍在后续 L2 范围。
