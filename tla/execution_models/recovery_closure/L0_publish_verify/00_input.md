# L0 输入

验证 `tickwire-recovery::Persistence::{publish,verify}` 与 `FileBlobStore::{open,put_sync}`。目标是能力交付时精确 binding 和全部依赖均已完成同步，失败/崩溃不能绕过完成点，竞争内容不能更换一个已绑定 recovery_object 的 root。

代码串行，目录有独占文件锁；对象按内容寻址，可靠存储完成后不丢失已同步内容。允许任意 API 微步崩溃、写入/同步失败、rename 成功后目录同步失败，以及同键的重试/内容冲突。不同目录的并发、恶意绕过文件锁和已确认存储失效不属于该服务的容错声明。

## 当前源码锚点

- `crates/tickwire-recovery/src/lib.rs:153-248`：对象保存、Publish/Verify 及依赖重新同步。
- `crates/tickwire-recovery/src/files.rs:15-60`：目录创建、重开、父目录同步。
- `crates/tickwire-recovery/src/files.rs:98-147`：同内容重试、临时文件同步、rename、目录同步与错误返回。

独立审阅后故障域已扩大到任意脏键存活子集，修正前文件位于 `pre-review/`，不属于当前三配置的结论。

## 账户依赖扩展后的同步审查

当前源码增加完整命令种类 13、账户基线种类 14 和账户前缀种类 15，并增加三个只读摘要。种类 14/15 必须各一个且同时存在种类 13；这些规则由 Rust 形状检查和账户协调器验证。本模型继续抽象 schema/内容检查的成功与失败出口，没有具体表示种类编号、账户前缀或摘要字段。

本次只同步源码锚点与范围，模型状态、动作、性质及三个配置均未改变，没有重跑 TLC。历史日志和结果继续证明原有有限实例的发布、重同步、崩溃和不可变绑定关系。更新前的完整模型、锚点、结果和原源码保存在 [pre-sync](../../../../docs/validation/account-recovery-binding/pre-sync/recovery_closure/L0_publish_verify/00_input.md)。
