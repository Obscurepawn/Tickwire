# L0 证据说明

CRC32C 和物理帧 SHA-256 遵从现有契约。Begin/Seal 的空载荷和 LSN 的全局字节偏移由本轮接口明确，root 审阅认可；未扩充帧头，也未创建第二种 WAL 格式。

每批不更新 manifest，保持一个 fdatasync 完成点。manifest 仅覆盖目录/分段身份；识别曾确认尾部的完全删除需要外部可信 PhysicalFrontier。PhysicalSyncReceipt 没有 LocalCommitProof、依赖闭包或远端复制语义。

## 业务适配后的源码锚点复核

Journal 的只读 config getter 位于 impl 末尾，追加/恢复算法未变。FileStore.open 增加固定 canonical 目录和新建/重试均同步父目录，导致文件追加/同步/截断从旧 135–154 行移动到 143–162 行；模型注释和当前 anchors.lock 已刷新。冻结的 00/05、旧 results.json 与源码摘要保留原版本，更新前 lock 另存 docs/validation/journal/followup-business-adapter。

本级从已建立且加锁的日志开始；mkdir、父目录同步与路径固定仍是未建模的初始化步骤，不能据本级 PASS 宣称已验证其故障微步。实际 CWD 子进程回归和物理/codec 全量回归见追加证据目录。
