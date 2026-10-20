# 业务适配期间的物理日志复核

本轮对物理 Journal 增加只读 `config()`，对 FileStore 固定 canonical 根路径，并在新建或重试打开目录时都同步父目录。固定路径避免进程修改 CWD 后将后续 I/O 导向另一目录；重复父目录同步完成先前 mkdir 已可见但父目录同步失败的重试。

父目录重试问题由源码审查确认，没有注入真实父目录 fsync 故障的 Red，不能把它归入故障实验。CWD 行为由业务 crate 的隔离子进程实际验证：相对路径打开后切换目录，bootstrap/manifest/segment 仍写在原锁目录。

`native-regression.log` 保留物理日志和 codec 的回归输出；业务目录 `linux-green.log` 在 Linux aarch64 运行同一组测试。原 `docs/validation/journal/environment.json` 和物理 L0 的冻结结果保留原版本摘要。这两个目录操作不在物理 L0 的帧级模型中；本轮是模型范围之外的源码修正，不据旧模型宣称已经验证 mkdir/目录同步微步。

codec 的新 `business_payload()` 只公开现有业务摘要的规范原像，`business_digest()` 改为复用它。既有 6 条事件和整数测试向量继续通过，编码和摘要没有变化。
