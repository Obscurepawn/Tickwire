# 控制 Raft 适配接口

`tickwire-control-raft` 按[复制设计](../design/replication-and-failover.md)使用固定 `raft = "=0.7.0"`。它接收有界控制命令和经过身份核对的同组消息，保存三 voter 的完整控制日志，输出已经持久提交的命令及安全读水位。配置、发布和授权 reducer 在该接口之上解释命令，订单 WAL 保留在执行域。

## 所有权与持久化

一个 `Node` 独占 `RawNode<DurableStorage>` 及其本地文件锁。`DurableStorage` 实现 raft-rs `Storage`，内存中的日志索引只反映已同步的本地记录。生产路径使用 `tickwire-journal::FileStore`，不使用 `MemStorage`。

当前阶段固定三个 voter，保留完整日志，以同步顺序处理每个 Ready：先将 entries 和 HardState 写入 journal 并同步，再推进 raft-rs 的持久水位，最后释放消息。LightReady 推进 commit 时也先同步新的 commit 水位。控制命令历史的 applied 记录同步后才向上层返回提交事实。任何写入或同步失败毒化节点；该实例停止发消息、接收提案和提供安全读，恢复由重新打开日志完成。snapshot 和成员变更消息在入口拒绝；后续实现会将 snapshot、ConfState 和业务状态恢复纳入同一持久协议。

单个逻辑存储记录可能超过物理 batch 上限，因此按 Begin、连续 Chunk、End 分片。Begin 固定记录长度、摘要和序号，End 完整核对后才更新存储缓存。崩溃留下的未完成逻辑尾部不产生 Raft 持久状态；下一次恢复会保留完整前缀并开始新的逻辑事务。物理 journal 继续检查自己的 framing、校验和、序号与已同步边界。

```rust,ignore
Node::create(store, config) -> Result<Node>;
Node::recover(store, config) -> Result<Node>;
Node::tick() -> Result<()>;
Node::campaign() -> Result<()>;
Node::step(authenticated_sender, message) -> Result<()>;
Node::drive() -> Result<Output>;
Node::propose(command_bytes) -> Result<Proposal>;
Node::request_read() -> Result<ReadId>;
Node::take_read(read_id) -> Result<Option<ReadBarrier>>;
```

`Output` 包含待发的 Raft 消息和本次新持久提交的命令。`Proposal` 只标识本地提案；只有日志里的同 term/index/content 被提交应用后，才能匹配到完成结果。业务 reducer 若拒绝命令，也要保存这个确定结果，不能把共识提交直接解释为业务成功。

节点配置固定 group、node ID、初始三个不同 voter、journal 身份和容量。网络输入核对实际认证 peer 与消息 `from/to`，限制消息尺寸和 entry 类型；本地 campaign、tick、proposal 不允许经网络消息注入。认证握手和 TLS 连接由网络服务适配器负责。

## 读与恢复

tick 为 10 ms，心跳 5 tick，选举 50–100 tick；启用 pre_vote、check_quorum 和 `ReadOnlyOption::Safe`。成为 leader 后，先等待本任期空 barrier 提交并持久应用，才接受需要控制证明的业务提案。每个读请求使用独立随机 context；ReadIndex 返回后继续等待本地 applied 水位。返回读证明时再次检查当前 term、leader 状态和请求身份。旧任期、未知 context 和重复消费不会返回证明。

恢复核对持久 HardState 的 term/vote/commit、日志连续性、applied 和初始 voter 身份。已提交命令保留完整内容，所有 voter 都能重建控制状态。容量耗尽返回明确错误，不删除幂等历史或绕过多数派。

先用三个真实文件节点验证选举、一次故障仍提交、失去多数拒绝新完成、重启恢复、旧 leader 安全读被阻止，以及每个持久边界失败不泄露肯定消息。之后接入成员替换、snapshot 转移、网络服务和控制业务 reducer。每一阶段保留实际失败和修复测试；全部系统代码完成后统一执行形式验证。

Ready 与应用推进顺序依据 [raft-rs 0.7.0 RawNode](https://docs.rs/raft/0.7.0/raft/raw_node/struct.RawNode.html) 和[固定版本源码](https://github.com/tikv/raft-rs/blob/10c6e9db6792b85c81784e44fc278f895d5f0ab0/src/raw_node.rs)实现。
