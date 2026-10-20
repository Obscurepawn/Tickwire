# 抽象与范围

L0 的登记/完成迁移原样提升，并为接收请求添加 check_time。有效请求先更新时间，再调用原登记操作；无效帧同样先更新时间。时间倒退时保留原 last_now 并关闭；时间达到 deadline 时先写 last_now 再关闭；已关闭会话保持关闭。关闭时的票据计数作为观察器，检查以后不再交付新票据。

renew 只有 nonce 相等、revision 更高时更新期限和 revision。错误 nonce/revision 仍执行先行 check_time；续租不修改 RequestStore 和先前票据。已经交付的票据可以在关闭后保存结果，这对应代码中 complete 不借用 Session，也使迟到的真实完成事实不会被抹除。

固定的 Lease、时间参数和 revision 集合是有限等价类。生产 `<2^63` 与 checked_add 的耗尽边界由 Rust 源码和测试承担。没有把控制会话续租等同于增险授权、reader 回收或订单发送资格。模型也不证明 raw syscall、字节解析、nonce 随机性和生产 launcher 的 epoch 唯一分配。
