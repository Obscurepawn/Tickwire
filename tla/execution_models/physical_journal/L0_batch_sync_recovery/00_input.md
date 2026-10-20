# L0 输入：物理批同步与恢复

对象是新增 `tickwire-journal`，不是原有 Mock `Journal`。依据主设计 §4 和 replication profile 的 136/40 字节完整帧；接口先于实现写入 `docs/implementation/journal-interface.md`。帧 codec 的真实 Red/Green 和独立审查已完成。

源码范围：`crates/tickwire-journal/src/journal.rs:177-369` 恢复，`:371-484` 追加；`crates/tickwire-journal/src/store.rs:135-154` 文件追加/同步/截断。模型从已经建立且持锁的单段空日志开始，段切换和 manifest 在下一层。

root 已独立核对恢复前缀、孤儿段、仅末尾截断、一次 sync 和 poison 分支，无开放安全候选；提出补 manifest 失败证据和新建目录权限。权限测试真实 Red 后增加 0700，manifest 失败测试覆盖写入前及 rename 后目录同步报错。ipc_codec 独立核对完整 frame codec，无开放候选。

`PhysicalSyncReceipt` 只记录物理同步。完整交易授权所需的恢复闭包、逻辑 hash、grant/strategy/operation 和镜像证明不在此类型内；没有 execution CommitReceipt 转换。
