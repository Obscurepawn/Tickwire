# 输入

继承 L0 请求登记与完成逻辑，增加 `Session.check_time/validate/renew`。固定一个已成功握手的会话：初始 now=1、deadline=1+Lease、revision=1、closed=false；相应构造与初始状态见 `crates/tickwire-control/src/lib.rs:247-282`。身份和协商的字节级检查仍由 Rust 测试负责。

时间检查、帧检查和续租来自 `crates/tickwire-control/src/lib.rs:386-457`；请求部分源锚点与 L0 一致。没有后台线程隐式续租，没有自动过期定时器；过期在下次 validate/renew 的 check_time 处变为永久关闭。
