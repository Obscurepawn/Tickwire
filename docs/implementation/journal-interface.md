# 物理 WAL 与本地同步接口

`tickwire-journal` 保存一个数据域的连续物理批次，复用 [replication-profile-v1.json](../../specs/contracts/replication-profile-v1.json) 的 WAL v1。接口在实现前确定；交易意图、恢复依赖闭包和镜像确认仍由上层组合。该 crate 不实现 execution 的 `Journal`，返回的 `PhysicalSyncReceipt` 也不能转换为其 `CommitReceipt`。

## 帧与批次

帧是 136 字节固定头、有效载荷、零 padding、40 字节尾，总长按 8 对齐且最多 1 MiB。字段偏移为：magic 0、version 8、header_len 10、total_len 12、type 16、flags 18、payload_len 20、stream_epoch 24、domain 40、segment 56、LSN 64、seq 72、exec_epoch 80、batch 88、前帧 SHA-256 96、头 CRC32C 128、载荷 CRC32C 132。头 CRC 的自身字段置零；帧 SHA-256 跳过尾部自身的 32 字节，其余字节全部参与。

通用 codec 接受契约中的全部 12 种 frame type；数据域 writer 只写 `BatchBegin → DataRecord+ → BatchSeal`。两种批边界载荷为空、flags 为零；Begin 的 seq 是首条，Seal 的 seq 是末条。批内身份、执行代次、批号一致，记录 seq 严格连续，前帧哈希连到前一批；Seal 本身绑定整条物理前缀。LSN 为跨段累计字节位置，首帧为零；segment_id 从 1 连续增长，只在批边界切换。

`append_batch(exec_epoch, payloads)` 限制为 1–32 条、整个物理批最多 64 KiB。执行代次只能保持或递增；stream UUID 在一个目录内固定。编码和容量检查先于 I/O。写入或同步报错后 writer 永久 poisoned，调用者关闭并恢复，禁止在不确定尾部后继续追加。

## 文件与完成点

`FileStore` 持有目录中的单写文件锁；Unix 新建目录权限为 0700，已有目录及其父目录由调用者保证可信。每个 `segment-00000000000000000001.wal` 默认最多 256 MiB；测试可缩小容量但不改变帧格式。`MANIFEST.json` 只保存格式、身份、分段配置、当前段和目录代次，带 SHA-256 校验。建立新段后同步文件及目录，再写临时 manifest、同步、rename 和同步目录；批数据写入发生在这个过程完成后。

完整批写入后，`File::sync_data` 成功才产生私有构造的 `PhysicalSyncReceipt`。它只证明当前后端完成了物理批同步，包含前缀身份、末 seq、批号、末 seal 的位置和哈希。每批使用一次文件同步；manifest 不保存逐批 commit 水位。`Store` 抽象使真实文件 I/O 可注入短写及同步失败，自定义实现承担其同步语义。

`PhysicalFrontier` 可由收据取得，也可由外部已验证证据提供给 `recover`。恢复逐段检查头、CRC、SHA 链、LSN、seq、批边界和执行代次，并核对该前缀仍存在。完整已封口批保留；最后一个不完整批可截去。完整帧校验失败、中间未封口、缺段、identity 或 epoch 错误均隔离。恢复先验证全部文件，再截断最后尾部；隔离时不改动现场。

成功同步保证此前字节保留的存储假设下，进程崩溃和未完成尾部可恢复。若设备在成功同步后无声删除整个封口尾批，只有外部可信前缀能证明丢失；单个自描述 WAL 无法区分它和本来较短的历史。真实掉电、依赖闭包、快照/压缩、跨主机复制、epoch 提升和交易授权均不在这个物理接口内。

## 先行验收

测试先覆盖字段偏移和独立已知 CRC/SHA 向量、逐字节破坏、完整/截断批、封口损坏、顺序与代次、外部前缀、段切换、manifest 损坏、文件锁、短写、同步失败，以及 Linux 子进程在尾部写入或同步前后被 SIGKILL。实际 Red/Green 命令和结果记录到 [journal 规格](../../specs/implementation/journal.md)。
