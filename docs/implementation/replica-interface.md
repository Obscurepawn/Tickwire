# 复制与封口接口

`tickwire-replica` 复制已经同步的账户记录及恢复依赖。源端使用 `Journal::export_durable` 读回原始记录；镜像使用同一个物理 WAL 实现保存连续复制元数据，在对象、闭包和该批日志全部同步后签发 ACK。

## 源端与镜像

`Stream` 的规范字节绑定风险域、账户日志代次、旧执行身份、存储身份、初始对象、`profile_version` 和 `commit_mode`。初始对象包括真实 `CanonicalBaseline` 与 `account_journal::bootstrap_bytes`。配置在源端按风险域、日志代次、执行代次持久绑定，相同身份不能在重启时换模式。

- `LocalDurableAsync { max_unreplicated_age_ns }`：本地记录及闭包完成同步后可产生 `CommitEvidence`；后台未复制记录超过配置时限时停止放行。进程重启后，先取得覆盖恢复前缀的镜像 ACK，再提供新的提交证据，避免用新进程时间重置旧积压年龄。
- `SyncReplica { mirror_id, assignment_epoch }`：相同前缀取得指定镜像当前启动代次的签名 ACK 后才产生证据。镜像不可用时保持等待；不会改用异步完成条件。

`SourceWorker::restore_prefix` 在启动阶段读入完整历史，不占用实时队列。`ingest` 接受实际 `DurableExport` 和各记录额外依赖。首次选择的 `Entry` 按流摘要与序号写入不可变 `ReplicaEntry`；重试、重启和离线封口复用该选择，再核对原 WAL 字节。

`spawn_source_worker` 将阻塞网络 I/O 放在独立线程。`WorkerHandle::try_export`、`try_event` 使用有界通道，满队列把原任务交回调用者。`Committed` 携带私有构造的 `CommitEvidence`；执行方仍须核对账户收据、执行权、时效与原命令。复制 ACK 不提供旧执行者已隔离的证明。

`Mirror::append` 拒绝缺口和冲突，精确重试可查询同一前缀。物理写入或同步失败后镜像停止追加。`recover(required_prefix)` 缺少指定完整前缀时保持 Frozen。

## 实际传输与边界

`PeerClient` 和 `MirrorListener` 使用 TLS 1.3、双向证书验证、双方叶证书摘要固定，以及独立的 Ed25519 帧签名。响应绑定请求摘要与随机数。配置没有跳过证书验证的入口。

单对象最多 64 MiB，分块最多 4 MiB，每个请求帧最多 4 MiB + 64 KiB。握手与读写使用绝对截止时间；连接建立另受连接超时约束。源端每次轮询有请求数与复制载荷预算。镜像在完整请求之间检查轮询字节预算，最后一个请求最多增加一个有界帧；不会拆开已接纳请求。执行线程不等待这些网络操作。

镜像进程入口：

```sh
cargo run -p tickwire-replica --bin tickwire-replica-receiver -- /absolute/receiver.json
```

配置类型为 `replica::config::ReceiverConfig`，证书采用 DER 文件。镜像每次打开生成新 `receiver_boot_id`，旧启动代次 ACK 不能继续作为新确认。

## 输入与策略交接依赖

`TWIPFX01` 对象绑定账户流、输入流、基线和连续输入记录。每条记录包含序号、writer epoch、长度和原文摘要；最多 16,384 条。源端与镜像检查已有输入前缀的公共部分完全一致。`IngressEvent` 必须被相应输入前缀覆盖；`Reactivated` 还引用其真实查询覆盖对象。

策略交接使用共享 `recovery::HandoverSeal`。复制方验证检查点 `TWHS0001`、完整 `TWINP001` 输入、其中的 `TWAI0001` 账户回调及十类依赖。完整交接输入必须覆盖交接记录之前的全部账户序号，与实际 WAL 原文一致；当前账户输入摘要、ledger 和订单集合摘要也必须一致。账户输入摘要内部的上一摘要由完整账户记录重算，不要求另存一串祖先对象。

## 离线封口与冻结恢复

`tickwire-app seal --config /absolute/seal.json` 使用 `failover::SealConfig`。工具先验证控制方签名的 `FenceEvidence`，取得原账户目录和输入目录的独占文件锁，完成恢复扫描后消费两个日志写者，生成 `SourceRecoverySeal`。封口包含最后全部输入记录，即使部分输入尚未产生账户记录。可选 `copy_to_mirror` 在有界轮次内补齐镜像及封口对象。

`tickwire-app restore --config /absolute/restore.json` 使用 `failover::RestoreConfig`，只接受新的目标目录：

- `source_seal`：核对源签名、隔离证据及完全相同的最终前缀。
- `sync_replica`：只适用于流内固定的同步模式；指定镜像先持久关闭旧复制入口，再生成 `MirrorRecoverySeal`。

工具复制全部恢复对象与绑定，重建原格式账户 WAL、输入 WAL，再调用真实 `AccountJournal` 和输入日志恢复。完成文件绑定新执行身份、隔离证据、封口及恢复前缀。账户仍冻结；私有流对账、策略恢复和新执行授权由后续启动流程完成。

三个目标目录在写入恢复数据前各自同步相同的 `FAILOVER-IN-PROGRESS` 标记。只有完整重建和语义恢复成功，工具才同步账户目录内的 `FAILOVER-COMPLETE.json`；恢复失败时保留标记。启动入口 `failover::validate_start` 核对三个标记、完成清单、基线、bootstrap、目标身份及最终输入对象，并返回两份日志的最低物理前缀，交给 `RecoverFrozen` 强制校验。任一标记存在而完成清单缺失、损坏或绑定不一致，都拒绝启动。

空账户日志尚无批封口，因此完成清单只在完整逻辑前缀为零时允许 `account_frontier: null`；输入日志包含配置记录，始终要求真实物理封口。完成标记记录先前通过 fresh fence 与完整前缀验证的恢复操作，本身不重新签发执行授权。
