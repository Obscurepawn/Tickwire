# 控制会话与请求登记

`tickwire-control` 在现有二进制 ABI 上验证控制会话。Linux 传输由 `tickwire-control-sys` 提供；系统读取的 PID/UID/GID 与监督器预先登记的实例、代次、host boot、namespace、角色及工件哈希共同确定身份。HELLO 中的声明不能替换登记信息。

服务端先验证 HELLO、共同 ABI/feature、架构与页画像，再分配控制 epoch、不可复用 nonce 和资源会话截止时间，返回 WELCOME。客户端核对内核对端身份、请求关联、服务端实例、host boot、协商结果与截止时间。控制帧均使用既有 128 字节 envelope、准确 body 长度和 CRC。

`Session` 保存两个端点及协商结果，验证 sender/target、epoch、frame 上限、feature 与严格 `now < deadline`。时间来自同一主机、同一 Linux time namespace 的 CLOCK_MONOTONIC；时间倒退或过期使会话永久关闭。资源会话续租只更新自身 revision/deadline，不能恢复交易授权，也不能解除共享内存容量门禁。

请求按 `(认证实例、control_epoch、request_id)` 登记，摘要覆盖 opcode、operation ID/revision、expected version 与完整 body。登记接口由持久存储实现，MockStore 用于确定性故障与进程演示。相同键/相同摘要先返回已有结果，再比较状态版本；不同摘要拒绝。首次请求先保存 Received，再交付一次执行票据；未完成记录的重试只返回 Pending，恢复查询由 owner 决定，不能重复执行副作用。

普通终态记录不会因内存压力淘汰；达到容量上限拒绝新键。完成时保存准确响应与 owner 状态版本。资源状态、journal 与控制结果之间的崩溃恢复由各 owner 的恢复协议衔接；内存 Mock 不声称具备真实持久提交。具有经济效果的 operation 还需独立持久业务键，本层不会把会话请求去重当成授权/订单去重。

测试先约束内核身份与 HELLO 不符、版本/特性/哈希不符、WELCOME 关联错误、过期与旧 epoch、续租回退、同键冲突、重试先于版本检查、Pending 不重复交付及存储失败。随后把实际会话/请求迁移纳入有限 TLA+ 模型；真实 FD 生命周期由 Linux 测试验证。
