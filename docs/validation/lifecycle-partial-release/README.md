# 部分成员已放行时恢复协调器

协调器已向两个执行域发出 Release，其中 A 已实际 Released、B 仍为 Committed。旧实现重启并收齐本轮 QueryApplied 后，再向两个成员发 Release；执行端 A 只允许从 Committed 应用 Release，因此新的请求被拒绝，协调器无法收齐本轮回执。

[red.log](red.log) 记录实际内存日志与 Ed25519 签名测试：预期待发请求只有 B，实际得到两个。另一个测试确认改变门禁版本或返回 Halted 时保持关闭。修复在生产 `Coordinator::plan` 中跳过已由本轮查询确认 Released 的成员，[green.log](green.log) 中两项测试通过。

测试分别覆盖 A 的首次 Released 回执已被协调器保存，以及回执在协调器收到前丢失。两种情况都要求收到两个新的 QueryApplied 回复，并显式调用 `resume_after_queries`。新的组凭据仍包含所有成员，A 的 `committed_receipt_digest` 引用原来真正的 Committed 签名回执，不能改用 Released 查询回执；B 引用本轮实际 Committed 查询回执。只有 B 的实际 Released 到达后，协调器才进入 ActiveAll。

命令：`cargo test -p tickwire-lifecycle --lib partial_release_recovery`。Red 退出码 101，Green 退出码 0。测试使用真实协调器状态机、签名和内存 Store，不运行文件 inbox 或进程场景。源码摘要见 [source-hashes.json](source-hashes.json)。
