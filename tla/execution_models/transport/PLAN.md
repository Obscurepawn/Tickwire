# 传输验证阶梯

L0 检查 `Connection` 的唯一发送槽、同步首次门禁、TLS 明文接受、部分输出、期限和失败后的接受证据。一个事件循环独占 `&mut Connection`，API 调用之间可以改变门禁和时钟；授权闭包与第一次 writer.write 属于同一次同步调用。TLS 和 socket 提供的 WouldBlock、短写及断连是接口允许的故障结果。

L1 计划接入 Gateway 的 durable intent、授权版本和业务 ACK 关联。HTTP/WS/FIX 语法、TLS 密码学、内核 TCP 实现和 CPU 弱内存不在本阶梯 L0 范围。解析和实际加密 socket 行为由 `crates/tickwire-transport/tests/` 的回归测试覆盖。
