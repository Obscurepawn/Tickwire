# 输入

从用户的实现与 TLA+ 验证任务选取 `Session::admit` 和 `MockRequestStore`。控制连接的 Linux 认证与报文校验由独立 Rust 测试覆盖，此级只接收已通过 Session.validate 的请求。

源码锚点：`crates/tickwire-control/src/requests.rs:6-32` 数据和后端契约；`:35-108` 执行票据与响应关联；`:109-169` 摘要、重试和登记；`:171-244` Mock 全部状态与操作。源码 SHA 和实际运行结果归档到证据目录。

先行测试包含失败编译 Red、功能性 Red 和 Green。固定请求键对应 authenticated instance/epoch/request_id；不同摘要包含不同 expected_state_version，不能独立修改摘要中的版本。
