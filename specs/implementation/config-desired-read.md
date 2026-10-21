# 已提交配置读取规格

实现位于 `tickwire-config-service::request`。`begin_request` 创建实际 Node 的 ReadIndex ticket；`poll_request` 只在该 ticket 达成多数屏障、Replica 业务扫描完成后读取 `CommittedHead`，并重新检查 TLS 资格和此 head 中的角色绑定。

`desired` 与 `revisions/<revision>` 要求集群读取权限。`head` 允许已授权的账户范围角色读取版本、摘要和 ETag，不返回配置内容。三个响应的 ETag HTTP header 与正文完全相同；正文最大为 16 MiB 加 4096 字节封套。

目前持久 head 来源为首次签名引导。响应使用 OpenAPI `BootstrapHead`/`BootstrapDesired`：`origin=bootstrap`，`control_commit_ref` 为真实 `raft:<term>:<index>`；不存在提交 UTC 或业务 operation ID。原普通提交 schema 保留在 `oneOf` 的另一分支，未被用于合成当前事实。

相同 revision 返回现有规范配置，其他 revision 在当前安全读及集群权限检查后返回 `404 NotFound`。尚无可信 head 时仍为 `503 ControlUnavailable`；尚未实现业务 reducer 的 plan、operation、effective、watch 视图为 `503 UnsupportedEndpoint`，不以 404 或空状态代替实现。写入接口不因本次读取扩展获得新的提交能力。

真实双向 TLS 测试连接实际三份文件日志的 Replica，覆盖多数隔离/恢复、规范配置与摘要/ETag、固定离线 OpenAPI 校验、账户范围拒绝、证书撤销和未知 revision/未实现视图区分。
