# 输入

目标是 `crates/tickwire-transport/src/connection.rs` 的发送生命周期。`PreparedSend` 保存不可变报文和期限，`queue` 只占用有界槽，`try_send` 才能首次接受，`drive` 只冲刷已接受数据。源码在 TLS 明文接受后保留 `accepted_bytes`，即使尚无密文进入 socket 也不能返回 NoBytes。

真实 socket 回归已经发现并修复解密读取超过每次预算、EOF 提前丢弃未读取明文、完整冲刷后同一次读取 EOF 导致完成记录延后等问题。它们来自实际测试，不是 TLC 发现。原始日志放在 `docs/validation/transport/`。
