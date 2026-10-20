# 控制 Raft 的持久边界

当前 `tickwire-control-raft` 固定三个完整 voter，使用 `raft 0.7.0`、Safe ReadIndex、pre_vote 与 check_quorum。生产存储实现 `Storage`，以现有物理 journal 保存更新；内存索引只在同步完成后替换。命令最大 16 MiB，保留量按配置有界，当前阶段保留完整日志。成员替换、snapshot 转移和网络服务仍在后续实现范围。

每个 Ready 的 entries 与 HardState 先持久化，之后才允许发送其消息。LightReady 的 commit 推进也先持久化。控制历史的 applied 记录同步后才推进 Raft applied，并输出具体 term/index/正文。输出只是控制命令的提交事实；上层 reducer 负责保存业务接受或拒绝的确定结果。

大更新按 Begin、连续 Chunk、End 写入物理 journal。每个 Begin 固定逻辑长度和 SHA-256，只有完整 End 才产生可恢复更新；未完成尾部由新的 Begin 放弃。物理 journal 自身继续检查批次同步、连续序号与校验和。任何写盘失败使本实例停止工作，整个 drive 的消息均不返回；恢复核对初始身份、term/vote、连续日志及 commit/applied 水位。

leader 接受业务提案前等待本任期 barrier 提交应用。安全读使用随机 context，等待多数派 ReadIndex 与本地 applied，并在领取时复核 leader、term 和请求身份；同一读证明只能领取一次。失去多数后不会完成新的命令或安全读。

认证输入先核对 peer/from/to 与允许的消息种类。Append 的首 index、连续性和非递减 term 在进入 raft-rs 前检查；Heartbeat 的 commit 不得超过本地实际日志尾，包括尚未落盘的 unstable entries。两项边界均曾在实际调用中触发 raft-rs panic，现由协议错误返回替代。

实际文件测试覆盖三 voter 提交与全组重启、100,000 字节命令分片、少数派隔离和重新追平、持久投票、六个同步失败切点、启动身份、错误 peer、Append 与 Heartbeat 边界，共 8 项通过。完整证据见[验证记录](../../docs/validation/control-raft/README.md)；形式验证留待全部系统代码完成后执行。
