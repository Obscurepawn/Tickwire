# 多域发布协调器

实现位于 `crates/tickwire-lifecycle`，接口见 [多域发布协调器接口](../../docs/implementation/lifecycle-coordinator-interface.md)。

确定行为：

- 成员清单有序、固定并纳入摘要；每个成员使用自己的单调时钟域和期限。
- Prepare、Commit、Release、QueryApplied、Halt 请求均签名。回执必须来自固定成员公钥，并匹配当前挑战；发送请求本身不推进应用状态。
- 只有全部实际 Committed 才签发 GroupRelease；只有全部实际 Released 才进入 ActiveAll。部分提交保留每域事实并保持尚未完成的流程。
- readiness 更新只允许尚未提交的成员，并要求新一轮 Prepared；其他成员的已提交事实保留。
- 状态更新比较版本，候选状态在同步成功后发布。同步不确定失败使 owner Frozen；恢复先查询实际状态，Halted 或门禁版本改变不能恢复旧操作授权。
- 部分 Released 后恢复，收齐本轮 QueryApplied 并显式继续，只向剩余成员发 Release；组凭据保留已 Released 成员原来的 Committed 回执。真实两域签名测试覆盖回执丢失、缺查询、门禁改变和 Halted，见[修复记录](../../docs/validation/lifecycle-partial-release/README.md)。
- 逻辑事件分块存入真实物理 Journal，最后提交长度与摘要。恢复处理完整尾部并支持 required physical frontier；有界目录传输通过真实文件同步与原子发布交付签名请求。

纯单元覆盖成员收齐条件、跨主机时钟、签名篡改、旧挑战拒绝、恢复后显式继续、门禁撤销、同步失败、期限后的关闭、固定配置冲突，以及部分提交期间的 readiness 更新。当前检查限定编译与纯单元；集成与形式验证留待核心功能统一验收。
