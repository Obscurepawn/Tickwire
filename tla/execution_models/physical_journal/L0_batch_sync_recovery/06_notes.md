# L0 证据说明

CRC32C 和物理帧 SHA-256 遵从现有契约。Begin/Seal 的空载荷和 LSN 的全局字节偏移由本轮接口明确，root 审阅认可；未扩充帧头，也未创建第二种 WAL 格式。

每批不更新 manifest，保持一个 fdatasync 完成点。manifest 仅覆盖目录/分段身份；识别曾确认尾部的完全删除需要外部可信 PhysicalFrontier。PhysicalSyncReceipt 没有 LocalCommitProof、依赖闭包或远端复制语义。
