# IPC 编解码接口

`tickwire-ipc` 实现 ABI 1.0 的字节边界。输入是调用者拥有的稳定字节切片，输出是拥有固定数组和整数的 Rust 值；调用者负责共享内存发布屏障、借用期和文件描述符权限。`ProducerPage`、`CursorPage` 的 codec 只处理已经复制或静止的页面镜像，不能用于正在被其他进程写入的原子页。

| 接口 | 输入与输出 | 拒绝条件 |
|---|---|---|
| `Wire::decode` | 精确长度的 `&[u8]` → 固定布局值 | 长度错误、非零保留区 |
| `Wire::encode` | 固定布局值 → 调用者的 `&mut [u8]` | 输出长度错误；保留区始终写零 |
| `decode_record` | 一条 Market/Intent/Report/Ownership record | 未知类型、版本、长度、保留联合体字段、非法枚举/位、CRC、payload 约束 |
| `ControlFrame::decode` | 完整控制帧 | 包头/包体 CRC、opcode、flags、长度与 FD 数不匹配 |
| `ControlFrame::decode_negotiated` | 控制帧、协商 feature mask | 在解析配置包体前拒绝未协商的 ConfigControlV1 |
| `validate_config_binding` | 已解析控制帧、已验证身份和提交证据的上下文 | 角色越权、修订冲突、身份不符、缺失收据证据 |
| `validate_topic_bundle` | 完整不可变 topic snapshot、注册表边界与 JOIN | 错误长度/offset/hash、缺失标的、边界不一致 |

36 个布局按机器契约生成固定 Rust 结构及显式小端读写语句。运行库不读取 JSON，不通过字段名称查找，不把整个 Rust 结构转换为字节。所有输入偏移先检查，再切片；嵌套数组有固定上界。`Wire` 是布局层；接收业务消息必须继续经过相应的语义入口。

实现顺序是固定向量测试、编解码器、消息和上下文校验、失败边界测试。共享内存映射、原子访问、ring/arena 分配器、Unix socket 和 SCM_RIGHTS 在后续模块实现。

接收路径使用 `decode_record`；只有 `payload_kind=ArenaLevels` 时继续调用 `validate_arena_payload`。控制会话使用 `decode_negotiated`，再将配置消息交给 `validate_config_binding`；上下文中的身份和证据由调用者从已认证连接与已验证 artifact 提供。完整 snapshot 校验后，调用者仍需检查过期时间与自己的 Ready 条件。

发送路径用具体 Rust 结构填写字段，调用 `Wire::encode` 写入预分配数组；记录可再次通过 `decode_record` 验证。`Wire::encode` 不自动生成 CRC，启用 CRC 的发送者先生成 payload CRC，再把两个 CRC 字段置零计算 record CRC。控制帧先计算 body CRC，再只把 header CRC 字段置零计算 header CRC。
