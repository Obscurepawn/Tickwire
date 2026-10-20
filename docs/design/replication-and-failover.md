# Tickwire 持久化、复制与执行接管

版本：v0.3 · 2026-10-20 · 状态：生产设计契约，尚未实现

**生产推荐基线为 `LocalDurable+AsyncReplica`：单写 exec 在本地完成 `durable_intent`，再发送订单；后台复制日志和恢复依赖，预热备用 exec。订单热路径不等待 Raft 或远端复制确认。** 三副本 Raft 只承担低频控制配置、发布 operation 和执行权协调。业务明确要求“执行主机及本地存储永久丢失后，所有可能已发送意图仍零丢失”时，才启用另一个具体方案 `SyncReplica`，逐单等待指定镜像持久化；它也不需要数据面 Raft。

异步备机可以缩短追平和恢复时间，但不知道旧主最后发送到哪里时，不能仅凭自己的复制水位自动接管。进程崩溃且本地盘完整可恢复；跨主机恢复还要求实际隔离旧执行者。原盘永久丢失且远端缺尾部时，域进入 `EvidenceLost/Frozen`，交易所对账和人工决定也不能凭空恢复丢失的策略状态。生产可靠性来自明确故障语义与可验证恢复，不等于所有订单必须跨区共识。

本文具体化[主设计](binance-ultra-low-latency.md)第 8、9、12 节，与[策略生命周期](strategy-lifecycle.md)和[共享内存 IPC](shared-memory-ipc.md)共同生效。参数、证据字段和故障场景见 [replication-profile-v1.json](../../specs/contracts/replication-profile-v1.json)。数量和时限是参考生产配置，不表示用户已有这些硬件或完成验收。

## 1. 两种明确的提交模式

| 项目 | 默认 `LocalDurable+AsyncReplica` | 可选 `SyncReplica` |
|---|---|---|
| 数据副本 | 本地单写 journal；另一故障域一个完整 warm standby | 本地 journal；另一故障域一个指定的同步镜像 |
| 增险发送前 | 本地意图、预留、授权及完整恢复闭包同步完成并应用 | 默认条件，加指定镜像对相同前缀及完整闭包的持久确认 |
| 数据面 Raft | 不使用 | 不使用 |
| 网络等待进入逐单路径 | 无复制等待；本地依赖落盘仍需等待 | 指定镜像往返与 flush 均进入完整延迟 |
| 进程崩溃、本地盘完整 | 从完整本地提交前缀重放与对账 | 同左 |
| 原主机/存储永久丢失 | 远端可能缺尾部；不承诺零丢失或自动恢复 | 指定镜像完整且协议证据有效时，保住所有可能已发送的意图及依赖 |
| 镜像不可用 | 本地继续持久化，备机标为不可提升；复制延迟达到保护期限后禁增险 | 未达到镜像确认的意图不 Accepted、不发送；不自动降级异步 |
| 自动提升条件 | 取得原盘完整前缀/源封口证明并补齐，实际隔离、对账和恢复完成 | 指定镜像证明覆盖所有可能发送意图，实际隔离、对账和恢复完成；恢复新的同步镜像后再增险 |

这里的恢复点目标（Recovery Point Objective，RPO）描述故障后允许丢失的记录范围。默认提交模式只在本地可靠存储仍可读取的故障范围内保证已接纳意图可恢复；异步复制延迟不是已保证的 RPO 上限。`SyncReplica` 的跨机零丢失范围是**可能发送的意图及恢复闭包**，并不表示交易所尚未交付的成交回报已经存在本地。

参考部署为两个交易主机 A/B，分别承载单写主执行者与 warm standby，位于独立故障域；每个参考主机至少 64 个物理核、256 GiB 内存。控制组在 A/B/C 三个独立故障域各有一个有状态 voter，C 为至少 8 个物理核、32 GiB 内存的控制专用主机；它持久保存完整控制状态，不冒充订单日志副本或无数据 witness。跨可用区会增加同步镜像等待，按真实拓扑压测。guardian 和隔离控制接口至少有一套位于当前 exec 故障域之外。

## 2. 日志所有权与控制 Raft

每个风险域由一个 exec 单写订单、资金预留、授权和账户事实，**该域本地 journal 是业务顺序的权威来源**。复制端按这个顺序保存并重放，不参与选举，不创造另一套可独立提交的业务事实。备用 exec 无正常交易出口。共享账户不能因为有多个数据副本就产生多个风险所有者。

只有 `control` 运行一个 Raft 组，保存配置、发布 operation、发布文件目录、拟执行权、隔离请求/证据和共享限流授权。控制指令提交后，exec 把指令及其证明导入本域 journal，本地持久化并应用后返回 `AppliedReceipt`。控制组提交不等于 exec 已应用，跨风险域发布继续按生命周期契约逐域确认，没有全局原子提交。

选定 TiKV **raft-rs `raft = "=0.7.0"`**，固定 `v0.7.0` 提交 `10c6e9db6792b85c81784e44fc278f895d5f0ab0`；crate SHA-256 为 `f12688b23a649902762d4c11d854d73c49c9b93138f2de16403ef9f571ad5bae`。使用 `RawNode`、`Storage` 和 `ConfChangeV2`，Tickwire 实现存储/网络适配器。`MemStorage` 不进入生产持久化路径，升级重跑崩溃与配置变更测试。[raft-rs 0.7.0](https://docs.rs/raft/0.7.0/raft/)

控制组每 10 ms 驱动 tick，心跳 50 ms，随机选举超时 `[500,1,000)` ms，启用 `pre_vote/check_quorum`，授权查询使用 `ReadOnlyOption::Safe`。选票依赖日志新旧关系，term/vote 在肯定响应前持久化。新 leader 先提交并应用本任期 barrier，才能签发控制提交证明；旧任期记录不能只因现在有多数副本就单独提交，须由本任期记录提交带动安全前缀。[配置接口](https://docs.rs/raft/0.7.0/raft/struct.Config.html)、[Raft 原论文，第 5 节](https://raft.github.io/raft.pdf)

存储适配器按 `Ready.number` 保存有序批次。缓存更新可由 `Storage` 读取后调用 `advance_append_async`；条目、HardState 和快照同步完成后，才按连续前缀调用 `on_persist_ready`，释放 `persisted_messages` 和肯定投票/追加响应。Tickwire 额外要求对外可见的控制 commit 在状态应用前持久化；处理后续 Ready/LightReady 也遵循此顺序，应用后才推进 `advance_apply_to`。`must_sync=false` 不授权提前对外宣布控制指令成功。[Ready](https://docs.rs/raft/0.7.0/raft/raw_node/struct.Ready.html)、[RawNode](https://docs.rs/raft/0.7.0/raft/raw_node/struct.RawNode.html)

控制消息绑定 `(cluster_epoch, group_id, node_id, node_incarnation)`，通过双向传输层安全协议（mutual Transport Layer Security，mTLS）认证节点身份。更高 Raft term 不授予 Binance 发单权。正常 exec 也不要求与控制 leader 同机；控制 leader 单独变化不重启交易会话。

控制指令包含 `operation_id/revision/request_id/payload_hash/target_risk_domain/expected_exec_epoch` 和控制提交位置。同键同内容幂等，同键不同内容拒绝；迟到旧 revision 或旧 exec 代次不得复活授权。exec 缓存已应用授权，每 25 ms 续期，寿命 100 ms；期限从本次启动中的本地挑战发送单调时间计算，迟到响应不从接收时重新计时。逐单只检查本地有效授权，不访问控制 Raft；到期禁增险，事实处理与独立预授权取消继续。

## 3. 本地提交、逐单状态与批处理

默认路径为 `Received → Prepared → LocalDurable → Applied/Accepted → final_send_gate → MayHaveSent`。`Prepared` 保存完整动作、不可复用订单 ID、风险预留、交易操作去重键、策略/执行代次、已应用授权引用和 `RecoveryManifest`。同步提交模式在 `LocalDurable` 之后增加 `MirrorDurable` 条件，达到它才可 Accepted。

提交记录包含“此动作在提交后可能发送”的语义。崩溃发生在发送与本地回调之间时，恢复仍将该意图视为可能已发；不能因没有 `MayHaveSent` 回调就推断未发送。这样无需在正常路径额外做一次“已发送标记”的同步提交。确定未进入输出缓冲的 `AbortedBeforeSend` 必须持久记录后才释放预留。

pending 预留参与后续风控，防止一批订单重复占用同一余额；同批后续条目看到前序预留。事实与撤权保持有界处理预算。未提交的成交输入可使风险更保守，不能提前增加可用资金；状态按 journal 顺序持久化并由单写 reducer 应用。

参考 group commit（合批持久化）上限为 32 条或 64 KiB，最老记录等待最多 25 µs，任一条件满足即同步；孤立记录不等待凑满。每域新风险 pending 上限 16 MiB，耗尽拒绝新 Prepared，保留回报与撤权容量。磁盘、文件系统与控制器需要真实掉电验证；页缓存、`mmap`、writer 收到消息均不是提交。

发送前重新检查 exec 权限、策略代次、发布 operation、期限、行情/账户状态与预留。等待本地落盘、同步镜像或 socket 可写期间发生撤权，旧记录即使已经持久化也不得绕过此门禁。`Accepted` 仍不表示交易所接受。

默认路径的完整延迟包含本地输入闭包、合批等待、flush、状态应用、编码签名、IPC 与 socket 排队，参考资格目标为 p99 ≤ 300 µs、p99.9 ≤ 1 ms；同步提交模式再加入镜像网络和存储等待，其参考候选目标为 p99 ≤ 2 ms、p99.9 ≤ 5 ms。两种提交模式都等待完整实测，不复用分位数，不拿纯计算小于 100 µs 代替完整发送结论。部署清单分别报告完整 `t_plain→t_socket_accepted` 与 `t_risk_done→t_commit`；控制 Raft 不在逐单路径。

## 4. WAL 帧、水位与崩溃恢复

预写日志（Write-Ahead Log，WAL）保存状态应用前的恢复事实。每域独立分段，单段 256 MiB、单帧最多 1 MiB，大对象按内容哈希引用。固定小端帧使用 136 字节头、8 字节对齐和 40 字节尾；JSON 定义字段顺序、长度与类型。数据域使用 `journal_epoch/record_seq`，控制域帧另保存 Raft index/term，两套编号不得互相代替。

帧包括 magic、版本、长度、类型、domain/group、segment、磁盘日志序号（Log Sequence Number，LSN）、业务 seq 或 Raft 位置、批次编号、前帧 SHA-256、头/载荷 CRC-32C。循环冗余校验（Cyclic Redundancy Check，CRC）检测损坏与撕裂写，哈希链定位错接内容。头 CRC 计算时自身置零，载荷 CRC 只覆盖有效载荷；padding 为零。尾部包含帧哈希、重复总长和结束 magic，帧哈希覆盖头、载荷、padding、重复总长和结束 magic，不递归包含自身。

`BatchBegin/BatchSeal` 封装完整批次。业务内容及所有依赖完成同步后才发布本地 `LocalCommitProof`；数据域的 seal 与记录一次 flush 即形成提交，不需要再为每单建立一个 Raft commit 水位。文件创建、rename 和 unlink 还同步目录。[Linux fsync/fdatasync](https://man7.org/linux/man-pages/man2/fsync.2.html)

| 水位 | 定义 |
|---|---|
| `last_seq` | 本地已编号追加的末端，可含未提交批次 |
| `local_durable_seq` | 本地完整批次与恢复闭包已同步的连续前缀 |
| `commit_seq` | 当前提交模式满足发送前条件的连续前缀：默认等于 local durable；同步提交模式还受 mirror durable 限制 |
| `applied_seq` | 本进程 reducer 已应用的提交前缀 |
| `snapshot_seq` | 已验证状态快照对应的事件边界 |
| `replica_received_seq` | 备机已收到的连续序号，不代表持久化 |
| `replica_durable_seq` | 备机日志及闭包完成同步的连续前缀 |
| `replica_applied_seq` | 备机只读 reducer 已重放的位置 |

正常本地满足 `snapshot_seq ≤ applied_seq ≤ commit_seq ≤ local_durable_seq ≤ last_seq`。同步提交模式的 commit 还要求匹配镜像的 `replica_durable_seq` 覆盖；异步复制模式不要求这项条件。备机比源端已看到的 ACK 更靠前是允许的，收到的尾部仍按原意图保守对账，不当作一次新交易。

重启只丢弃最后没有完整 seal 的未完成批次；封口批次 CRC/哈希错误、中部洞、epoch 回退或依赖丢失都进入 `StorageQuarantined`，不能截断已确认历史后继续交易。恢复从同一快照边界重放完整已提交批次；applied 数字不能脱离状态独立恢复。未提交尾部不会直接触发外部动作。

数据主备不采用“新 leader 自动覆盖旧日志”的 Raft 规则：同一 `(domain,journal_epoch,seq)` 出现不同内容即停机取证，不能任选一边。只有最后未封口的物理尾部可截断。正常提升先取得前缀完整证明，再创建绑定新 exec epoch 的新 journal epoch，保留原链前驱并继续 seq，所有订单 ID 还绑定执行代次，避免重用。

控制 Raft 的冲突未提交后缀可以通过同一追加批次中的 `TruncateSuffix`、新条目和 HardState 原子替换，禁止删掉 commit 内记录。启动 `RawNode` 使用同一快照 S 的 ConfState 与 `Config.applied=S`，先重放至持久 commit、恢复配置变化，再开放投票与业务网络；不能把较新 ConfState 和较旧快照 applied 混装。

## 5. 异步复制协议、延迟与保留

后台 `tickwire-replicator` 只读取本地已封口且同步完成的前缀，发送 `ReplicaHello/AppendBatch/NeedRange/DurableAck`。握手绑定风险域、journal epoch、源 exec 身份、前驱快照/日志哈希、接收方身份和复制协议版本；连接经 mTLS 认证。每批包含连续 `[first_seq,last_seq]`、前驱逻辑哈希、条目哈希、批次根和恢复 manifest 引用。

逻辑链独立于物理 WAL：`H_i=SHA256(H_previous || canonical(domain,journal_epoch,seq,type,payload_hash))`。不同主机的磁盘分段、LSN 和批次编号可以不同，不能比较物理帧哈希来判断业务前缀一致。重复批次内容一致则返回原结果；序号有洞返回 NeedRange，乱序批次最多缓存 8 MiB，超过后背压并重传。相同序号不同哈希立即隔离该复制会话。

备机先拉取缺少的内容寻址对象，校验依赖闭包，再写批次并同步；之后才能返回 `DurableAck(domain,epoch,last_seq,prefix_hash,dependency_root,receiver_boot_id,storage_batch_id)`。接收/写页缓存只推进 received，不推进 durable。ACK 丢失时源端重发同一序号/哈希；旧 receiver boot 的 ACK 不证明重启后仍完好，新会话先验证介质并交换水位。

正常尾部每 1 ms 或 64 KiB 发送，最多 128 个在途批次。记录 `source_local_durable_seq-replica_durable_seq`、字节差和最老未复制记录年龄；100 ms 告警，1,000 ms 标记 `ReplicaUnavailable/NotPromotable` 并按本配置禁止新增风险，事实入账与取消继续。这是异步复制恶化时的风险政策，不是每单等待远端 ACK，也不能在任意故障下保证最多丢失一秒数据。复制追平并验证后，exec 通过新的本地授权屏障解除该冻结；本地保留空间另有容量保护线。

源端保留备机未确认的日志区间与依赖 pin。达到 80% 磁盘预算告警，90% 禁止新策略准入和新增风险，留下事实/取消审计空间；不能删除未复制尾部维持表面吞吐。备机落后超过在线日志范围时，从完整 snapshot S 及其依赖重建，再连续接 S+1。切换到 snapshot 追平前，源端必须已有可恢复新快照并固定其 pin，不能先删除追平所需数据。

异步 standby 可以实时重放并建立只读账户视图，但 `replica_applied_seq` 只说明它处理完自己拥有的数据。它永远不能凭“延迟很小”“最后心跳正常”或最近一次 ACK 自行证明已经拥有旧主全部可能发送记录。

## 6. 恢复依赖闭包与提交证明

`RecoveryManifest` 的内容根覆盖不可变依赖图：意图/预留/去重、授权、发布 operation、策略发布文件/模型/配置/schema，以及策略检查点和输入。当前意图叶子只哈希不含 manifest 引用的 payload，Raft/本地日志 envelope 再引用 manifest 根，提交证明绑定二者，避免自引用。存储收据与动态 pin 持有者是旁附证据，不参与内容根。

`deterministic_replay` 必须保存检查点、随机状态/逻辑时间，以及直到决策边界的实际有序输入，覆盖行情、exec 回报、定时器、配置和模型更新。可信 SDK 输入 sequencer 或隔离适配器固定次序，独立持久化服务验证连续字节与哈希链后出具覆盖收据；策略自报水位无效。md 的 Disruptor 共享广播不改成复制日志，只有策略恢复所需的实际输入通过可靠通道转为内容寻址 blob。

`declared_rebuild` 保存经批准的重建契约、所需行情窗口与账户事实、预热及验证条件。不能在丢失精确输入后临时更改恢复语义。默认提交模式要求完整闭包在当前本地可靠存储完成；复制端只有自己也保存完整闭包，才确认相应 durable seq。同步提交模式要求本地和指定镜像均满足闭包，因此不能只镜像订单 WAL 而把策略状态留在原机。

对象采用临时文件、内容校验、同步、rename 和目录同步后发布。代码等大对象预置，输入以 256 KiB/50 µs 上限的批次持续落盘，意图等待其决策边界。live intent、未知订单、保留快照、发布 operation 和待安装快照持有 pin；只有持久 DependencyRelease 且不存在活引用，才能回收。未知订单的依赖不能按保留天数直接删除。

证明分成三个类型：`LocalCommitProof` 证明本地完整批次及闭包，默认 Accepted 依赖它；`MirrorCommitProof` 在本地证明上加入指定镜像同 epoch/seq/hash 的持久收据，只有同步提交模式使用；`ControlCommitProof` 证明控制 Raft 的当前任期安全前缀和有效 membership 多数派。三者不能互换，数据域没有一个虚构的 Raft term。

exec 将证明绑定 `(domain, intent_id, payload_hash, manifest_root, exec_epoch, strategy_generation, action_grant_ref, operation_revision)` 并核对自己的 applied 结果。证据使用简明二进制对象表示（Concise Binary Object Representation，CBOR）确定性编码，禁止重复键、浮点数和不定长项；签名使用 Ed25519，加入记录类型/schema 域分隔。哈希与签名说明内容及来源，不代替实际存储语义。[CBOR 确定性编码](https://www.rfc-editor.org/rfc/rfc8949.html#section-4.2.1)

## 7. 快照、压缩与成员维护

每 100,000 条已提交记录或 512 MiB 新 WAL 启动快照，取同一个已应用边界 S。快照保存 `seq/epoch/log_prefix_hash/state_schema/reducer_build_hash`、订单/预留/管理权/授权/去重、ID 高水位、operation 和依赖 pin；控制快照还保存 Raft index/term/ConfState。生成不可变状态视图，不能跨多个时刻拼接。

快照正文与闭包同步后，写 `SnapshotPublish` 和新 CURRENT manifest，再 rename、同步目录。恢复只选择完整匹配的一代；保留当前和前一份快照。默认本地压缩要求本地新快照完整，并继续为落后备机保留增量或固定可供安装的快照/闭包；备份与审计归档另行保留。同步镜像的压缩必须保证指定镜像仍有所有可能发送记录的可恢复快照和后续区间。

传输采用 4 MiB 分块 CRC 和最终 SHA-256。备机先暂存并校验完整快照/闭包，再原子切换 CURRENT，重放连续尾部，最后报告安装完成。网络传输结束不算安装成功；快照安装不产生交易动作。业务去重范围和 ID 高水位不会随 WAL 压缩消失。

数据备机替换是**复制会话维护**：控制组登记新接收者，旧源按 snapshot+连续日志复制，完成闭包和重启验证后才发布它的 ReplicaReady；异步复制不需要联合投票。同步镜像替换需先冻结增险，追平新镜像至源封口边界，提交新的 mirror assignment 和 journal epoch 后才能恢复。不能将滞后的异步备机直接命名为同步镜像。

控制 Raft 的 voter 替换使用 `ConfChangeV2/Explicit`，每次最多增加一个 learner，同组仅一个配置 operation。例 `{A,B,C}→{A,B,D}`：先让 D 追平完整控制快照/日志，提交并应用 joint，再提交空变更退出 joint。选举和提交分别满足旧、新多数派，最终配置提交应用前 C 不下线。raft-rs 在 `apply_conf_change` 时生效，必须持久化返回的 ConfState，而非照论文把“刚 append”当生效。[固定版本 ConfChangeV2](https://raw.githubusercontent.com/tikv/raft-rs/10c6e9db6792b85c81784e44fc278f895d5f0ab0/proto/proto/eraftpb.proto)、[Raft 原论文，第 6 节](https://raft.github.io/raft.pdf)

## 8. 实际隔离与执行权状态机

| 状态 | 允许动作与进入条件 |
|---|---|
| `Standby` | 重放和健康检查，无正常交易出口 |
| `Active` | 当前 ExecutionGrant 已由 exec 持久应用；逐单提交与最终门禁均满足 |
| `FreezeRequested` | 健康旧 exec 立即禁增险，继续入账和原有取消权限 |
| `FencePending` | 隔离证据未齐；guardian 只有独立预授权 CancelOnly，不能授新 exec |
| `Fenced` | 旧身份的持续隔离证据已核验；guardian 可正式清理 |
| `Recovering` | 验证源前缀/镜像证明、重放、对账与策略恢复，仍禁增险 |
| `GrantPrepared` | 新基线与身份已持久化，新出口准备完成，等待最终 ActivateExecution |
| `EvidenceLost/Frozen` | 原前缀或其他证据不足；查询/批准的取消可继续，时间不改变事实 |

参考执行身份绑定 `(host_id,boot_id,exec_instance_id,exec_epoch,network_identity,credential_access_identity)`。交易流量只经登记的外部控制点，备用出口也纳入清单。隔离控制器位于独立故障域，先持久登记旧身份 deny tombstone，再阻断全部外部交易出口、撤销既有连接状态并读回设备配置。

仅禁止新连接的安全组规则不够；设备必须能撤销存量会话，或使用虚拟网卡断开、外部端口隔离、电源关闭。旧主可达时，host agent 禁用自动拉起、终止 exec cgroup，并用 pidfd 验证退出及无残留成员。不可达时使用独立管理网络完成外部网络/电源隔离。PID 数值、SIGSTOP、单次 ping 不通、Raft term 和授权超时都不构成隔离证据。[Linux pidfd](https://man7.org/linux/man-pages/man2/pidfd_open.2.html)

`FenceEvidence` 绑定控制 operation/提交位置、旧身份、全部出口集合哈希、设备动作 ID/revision、进程或电源状态、密钥访问撤销、fresh readback/探测和控制器签名。设备操作须幂等并拒绝旧版本；不支持 revision 的设备由唯一控制器串行化，重启先恢复未完成动作。无法确定旧 enable 是否仍会执行时，使用独立端口/电源隔离或保持 Frozen。旧身份在接管中永不重新启用，新主使用新身份。

控制器执行 enable 前通过控制 ReadIndex 确认当前 operation；激活前再核验旧 deny 仍生效。隔离证据分别写入控制日志和接管域本地日志。实际隔离阻止旧身份继续输出，不能消除已经离机的请求；这些意图仍需对账，不能直接释放 Unknown。

`GuardianEmergencyGrant` 在健康期预提交，限定账户、guardian 身份、触发条件、接口、速率与取消范围。隔离前可依据这份原有授权尽力查询/取消；控制或数据存储不可用时记录独立应急日志，复制不可用标记 `audit_degraded`，不等待一条无法提交的新授权。普通单、orderList、algo 和触发子单均在清理范围。CancelOnly 不授权新 exec、不转成市价平仓，也不宣称 Binance 存在未提供的 cancel-only 密钥权限。

## 9. 提升算法与证据缺口

默认异步复制模式使用以下顺序，控制 operation 保存每一步结果，重试沿用原操作键：

1. 冻结新授权，触发实际隔离。exec 主循环进度每 10 ms 更新，50 ms 无进展即怀疑；独立心跳线程不能掩盖主循环卡死。本地终止预算 500 ms、网络隔离预算 2,000 ms，电源隔离使用剩余预算，隔离总截止时间 5,000 ms；超过即 Frozen，不假设隔离成功。
2. 原盘可读时，在旧写者已隔离后取得**完整源前缀证明 `SourceRecoverySeal`**：可由健康源写入 FreezeSeal 并关闭写入，或由可信恢复工具读取隔离主机的盘、校验最后完整批次和全部闭包，记录最终 F、哈希、存储身份及 FenceEvidence。源退出前最后一个 ACK 不是 SourceRecoverySeal。
3. 新主取得 snapshot+日志+闭包至 F；缺多少复制多少，核对源封口证明并持久化 `PrefixComplete`。备用盘上的最大 seq、低复制延迟或静默一段时间都不能替代 F。原盘暂时不可用时保持 Frozen 等待取回。
4. guardian 正式清理；新 exec 建立并缓存私有流，查询全部未决意图、普通/条件/子单、成交、余额与持仓，通过重叠窗口、完整分页和幂等合并重建交易结果记录。可能晚到的旧请求保持跟踪，一次 not found 或超时不释放预留。
5. 写入新 `ReconciliationBaseline`、订单/成交去重与新 exec epoch，恢复策略检查点/输入或声明重建状态，重建发布 operation 实际管理权。回放输出不得进入真实交易入口。
6. 控制组提交拟 ExecutionGrant，新 exec 本地持久化 `GrantPrepared`，绑定完整前缀、隔离/对账/策略屏障和控制挑战期限。隔离控制器复核旧 deny，为新身份返回 `EgressReady`。
7. exec 本地持久化并应用 `ActivateExecution`，再次验证控制授权、全部屏障和新出口，再开放最终发送门禁。旧主回归只作为隔离备机重建，丢弃旧发送缓冲和授权；迟到回调按原动作身份合并，不重发。

如果原主机和本地存储永久丢失，异步备机没有 F，就无法证明不存在未复制但已经发送的订单。进入 `EvidenceLost`，保留所有已有预留，导出已知前缀、订单 ID 命名空间、复制缺口和交易所查询覆盖。即使查询找回了订单与成交，也可能找不回授权细节、策略有序输入或随机状态；不能把经济对账完成等同于精确状态恢复。

恢复人员必须逐项记录交易所事实能证明什么、历史可查询范围和仍未解决项。若完整证据最终无法取得，原精确恢复契约不能继续宣称成立；只能依据另行批准的灾难恢复/策略状态重建变更，在闭合账户事实、定义剩余未知风险及新策略基线后决定是否恢复。人工签字本身不补齐缺失数据，未能约束的账户风险继续冻结。默认提交模式因此不承诺原存储永久丢失后的固定恢复时间目标（Recovery Time Objective，RTO）。

进程故障、本地盘完整，以及指定负载下的已完整复制接管，可在部署清单中设置 30,000 ms 恢复资格目标；对账阶段预算 20,000 ms、策略恢复 5,000 ms，阶段预算不能延长总期限。超时产生具体未决项，不自动 Active；源盘等待、EvidenceLost 和交易所历史不足不纳入这个固定目标。

## 10. SyncReplica 的启用与恢复

只有业务明确选择跨机“可能发送意图零丢失”，并接受实测同步镜像等待及镜像故障时停止增险，才启用 `SyncReplica`。切换必须先冻结域，封口源前缀，验证镜像到该边界及全部依赖，再把 `durability_profile_version/mirror_id/mirror_assignment_epoch` 写入控制日志和本地 journal。切换不能追认此前异步发送的缺失尾部已经同步。

正常每单仍由本地 exec 单写。指定镜像对完整连续批次及其闭包 flush 后返回签名 DurableAck，源端同时满足本地 durable、镜像身份/assignment epoch、相同 seq/hash/manifest 和有效执行权，才产生 MirrorCommitProof、Accepted 并允许发送。ACK 丢失按原键查询重试；一旦可能发送，不因超时重新编号。镜像故障使提交停住，绝不自动降级 LocalDurable。

镜像确认不要求源端再为每单做一次独立磁盘 flush；本地和镜像已持久化的数据构成恢复基础，证明可由相同前缀重新取得。源端重启后缺少 ACK 回调或缓存的 MirrorCommitProof，不能断言记录未发送；它按本地完整记录与指定镜像重建水位，证据暂不可取的记录保持潜在已发/Unknown。同步协议可在本地 flush 期间先传递完整批次以重叠网络时间，但源端必须等本地和镜像均完成同步；这种提前传输不改变默认异步协议的已持久前缀规则。

原主永久丢失后，先实际隔离旧身份，再让指定镜像封闭旧复制 epoch，生成 `MirrorRecoverySeal`，保留其所有完整前缀。所有可能发送记录都要求该镜像先持久确认，因此这份前缀覆盖可能已发动作；未被源收到 ACK 的镜像尾部也按潜在意图对账。旧 epoch 迟到复制包不能在新 epoch 继续追加。

此后按相同的对账、策略恢复和授权流程推进；新主恢复增险前，必须再建立一个满足完整闭包的指定同步镜像。只剩一份数据时可以查询和取消，不能偷偷转成单副本发送。没有可用替代镜像或交易所事实不足时保持 Frozen，也不承诺固定 RTO。同步镜像解决指定故障范围的意图持久性，实际隔离和交易所事实仍是独立条件。

## 11. 确定性故障验收

| ID | 注入序列 | 必须观察到的状态 |
|---|---|---|
| `RF-01` | 默认提交模式本地 flush 前杀 exec | 无 Accepted/发送；丢弃未封口尾部 |
| `RF-02` | 默认提交模式本地提交并发送、远端未复制时杀进程，本地盘完整 | 从原盘恢复原 ID/预留；策略重试不产生新交易操作 |
| `RF-03` | 默认提交模式原机与盘永久丢失，远端缺尾部 | EvidenceLost/Frozen，不凭备机水位自动提升 |
| `RF-04` | 备机收到日志但缺策略检查点/blob | received 可前进，durable 不前进；不可用它证明完整恢复 |
| `RF-05` | 控制 leader 仅有旧任期多数副本，没有本任期 barrier | 不签发新的控制提交证明和执行授权 |
| `RF-06` | 控制 joint `{A,B,C}→{A,B,D}`，B 故障、D 分区，仅 A/C 互通 | 旧集合多数派不够，不退出 joint、不提前回收 C |
| `RF-07` | snapshot 落盘后、CURRENT 目录同步前掉电 | 只恢复完整匹配的一代，不混装 applied 与状态 |
| `RF-08` | 尾部半批/封口中部 CRC 错误分别恢复 | 仅未封口尾部可丢弃；封口损坏隔离存储 |
| `RF-09` | 旧 exec SIGSTOP 后恢复，旧续期/enable 迟到 | 不从接收时间延长租期，旧身份 deny 保持，旧缓冲不能发出 |
| `RF-10` | 旧请求在实际隔离后到达交易所 | 对账前保持 Unknown/占额，时间不释放预留 |
| `RF-11` | 控制 quorum 不可用，guardian 原有应急授权有效 | 允许限定查询/取消并记审计，无正常新单和新 exec 权限 |
| `RF-12` | 等待本地或镜像提交时策略代次撤销 | 后续提交成功也由最终门禁拒绝旧代次 |
| `RF-13` | SyncReplica 本地 flush 成功，镜像 flush/ACK 未完成 | 不 Accepted、不发送，不降级异步 |
| `RF-14` | SyncReplica 镜像确认后原机永久丢失 | 镜像保留可能发送记录；隔离、对账、补新镜像后才增险 |
| `RF-15` | 异步主备同 epoch/seq 不同哈希 | 停止会话并取证，不自行覆盖一边日志 |
| `RF-16` | 异步复制断开直至本地磁盘达到保护线 | 保留未复制区间与闭包，禁止新增风险，不删尾部维持发送 |

实现阶段验证本地存储、异步与同步复制、控制 Raft、外部隔离设备和交易所模拟器的实际结果。形式模型覆盖 `local_commit`、`async_replica_promotion`、`sync_mirror_commit`、`dependency_closure`、`control_membership_joint` 和 `exec_fencing`，动作锚定源码。本文没有 Rust 实现、TLC 通过结果或生产延迟实测。

## 参考资料

- [Raft 原论文扩展版](https://raft.github.io/raft.pdf)：仅用于控制共识的选举、提交和联合成员变化依据。
- [raft-rs 固定源码](https://github.com/tikv/raft-rs/tree/10c6e9db6792b85c81784e44fc278f895d5f0ab0)、[crate 元数据](https://crates.io/api/v1/crates/raft/0.7.0)：2026-10-20 核对的版本、提交和归档校验值。
- [Ready](https://docs.rs/raft/0.7.0/raft/raw_node/struct.Ready.html)、[RawNode](https://docs.rs/raft/0.7.0/raft/raw_node/struct.RawNode.html)、[Storage](https://docs.rs/raft/0.7.0/raft/storage/trait.Storage.html)、[ConfChangeV2](https://raw.githubusercontent.com/tikv/raft-rs/10c6e9db6792b85c81784e44fc278f895d5f0ab0/proto/proto/eraftpb.proto)：控制适配器契约。
- [Linux fsync/fdatasync](https://man7.org/linux/man-pages/man2/fsync.2.html)、[pidfd](https://man7.org/linux/man-pages/man2/pidfd_open.2.html)、[CBOR 确定性编码](https://www.rfc-editor.org/rfc/rfc8949.html#section-4.2.1)：存储、身份核验与证据编码依据。

默认异步协议、同步镜像和执行隔离是 Tickwire 的设计选择，不代表 raft-rs 已实现交易恢复，也不代表生产交易系统普遍要求逐单共识。
