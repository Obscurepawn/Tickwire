# 物理日志实现规格

`tickwire-journal` 已实现 WAL v1 帧、数据域批次、真实文件同步、分段和 manifest 恢复。接口设计见 journal-interface.md，证据见 验证记录。它是上层持久化适配器的底层组件，当前未接入 execution 的业务 `Journal`。

## 格式与接口

- `frame::{encode, decode}` 严格复用 136 字节头、40 字节尾、8 对齐、最多 1 MiB；支持契约全部 12 个 type。当前 v1 拒绝非零 flags，padding 必须为零。解码返回消费长度，允许缓冲区后面紧接下一帧。
- `Journal::create(Store, Config)` 建立唯一域/stream 身份。数据批包含空载荷 Begin、1–32 条 DataRecord、空载荷 Seal；整个物理批最多 64 KiB，不能跨段。默认段容量 256 MiB。
- `append_batch(epoch, payloads)` 先检查数量、长度、epoch 和数值溢出，再执行 I/O。完整批后的一次 `sync_data` 成功才返回 `PhysicalSyncReceipt`；发生 I/O 错误后实例 poisoned。
- `recover(Store, Option<&PhysicalFrontier>)` 验证所有段和可选外部前缀后，才截断最后未完成批并重新同步。已经完整封口的批，即便提交者没有收到收据，也保留并重放。
- `FileStore` 用独占文件锁序列化写者，Unix 新建目录权限为 0700。锁取得后立即交给私有 guard，构造失败及唯一 owner 正常销毁时显式解锁，再关闭描述符；另一个线程启动子进程时临时继承的描述符副本不能延长已经销毁的 owner 的锁。副本不是独立写者，副本后来关闭也不能解除新 owner 的锁。调用者负责已有目录的访问权限和可信父目录；其他程序不能绕过 advisory lock 修改文件。

`MANIFEST.json` 是带 SHA-256 校验的分段目录元数据，包含 version、config、generation、active_segment，不是业务恢复依赖的 `RecoveryManifest`。段从 1 连续编号；建立新段时先同步新文件和目录，再同步临时 manifest、rename、同步目录。恢复允许最多一个 `active_segment + 1` 的孤儿段，要求所有物理链连续；空中段、缺少 manifest 已指明的段或更大的段号差都拒绝。

`PhysicalFrontier` 精确绑定域、stream、segment、段内末偏移、累计 LSN、末 seq、epoch、批号和 seal hash。由本次扫描自行取得的前缀不构成独立证据；调用者需要从先前收据或已验证外部存储证据提供它。

## Red 与 Green

| 阶段 | 实际命令 | 结果 |
| --- | --- | --- |
| 帧 Red | `cargo test -p tickwire-journal --test frame` | 已编译接口桩，5 个行为测试失败，exit 101 |
| 批次 Red | `cargo test -p tickwire-journal --test journal` | 已编译接口桩，7 个行为测试失败，exit 101 |
| 目录权限 Red | `cargo test -p tickwire-journal newly_created_directory_is_owner_only` | 读取真实新建目录，`mode & 077 = 45`，断言失败 |
| native Green | `cargo test -p tickwire-journal -- --nocapture` | 21 passed；1 helper 显式 ignored，由父测试四次启动 |
| Linux Green | 同一 Cargo 命令，固定 Rust 1.99.1 bookworm 镜像 | 21 passed；四个 SIGKILL 边界完成 |
| 静态检查 | `cargo fmt -p tickwire-journal -- --check`；`cargo clippy -p tickwire-journal --all-targets -- -D warnings` | 通过 |

初始 Red 使用返回错误的接口桩和真实断言，随后实现算法。额外回归覆盖正确 checksum 下的序号洞、epoch 回退和身份变化；manifest 发布前失败与真实 rename 后注入目录同步失败；每批只调用一次数据同步，普通批不发布 manifest。

Linux 运行环境为 `Linux 6.8.0-117-generic aarch64`，Rust `1.99.1`。四个子进程暂停点为半批写入、同步之前、同步之后但收据返回之前、收据返回之后，父进程实际发送 SIGKILL 并等待退出信号 9。半批测试丢弃 372 字节，保留原有一条记录；另外三种情况恢复三条完整记录。同步前的记录能在 SIGKILL 后从仍存活的页缓存读取，恢复会再次同步，不能据此称它们在首次写入时已经持久化。

## 恢复边界

完整帧的 CRC、padding、尾部或 SHA 错误，即使处于最后批，也隔离并保留现场。最后批缺少完整帧或 Seal 时才可截去；若给定的可信前缀因此缺失，仍先报错，不修复文件。成功同步后的全部尾批被媒体无声删除，在没有外部前缀时无法检测。

单段 [L0 形式模型](../../tla/execution_models/physical_journal/L0_batch_sync_recovery/05_output.md) 使用帧粒度的有限实例，覆盖同步、收据丢失、poison、崩溃、尾部恢复和可信前缀；分段/manifest 目录事务保留给 L1。Rust 测试覆盖实际格式与文件系统调用。当前未做设备掉电、真实复制、快照压缩或恢复闭包验证，也未产生可供交易授权的 LocalCommitProof。
