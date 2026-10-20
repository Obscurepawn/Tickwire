# L0 输入

验证 `tickwire-recovery::Persistence::{publish,verify}` 与 `FileBlobStore::{open,put_sync}`。目标是能力交付时精确 binding 和全部依赖均已完成同步，失败/崩溃不能绕过完成点，竞争内容不能更换一个已绑定 recovery_object 的 root。

代码串行，目录有独占文件锁；对象按内容寻址，可靠存储完成后不丢失已同步内容。允许任意 API 微步崩溃、写入/同步失败、rename 成功后目录同步失败，以及同键的重试/内容冲突。不同目录的并发、恶意绕过文件锁和已确认存储失效不属于该服务的容错声明。
