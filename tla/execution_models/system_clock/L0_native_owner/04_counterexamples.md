# L0 反例记录

四个最终配置没有违反所列不变量，也没有产生 Rust 缺陷候选。源码保持不变。

独立审阅将 `nativeReads` 的含义改为 `native_nanos` 尝试次数，因为 Unsupported 分支不执行 OS syscall。初轮的 Generic 覆盖入口合并通过添加等价的结果分类条件处理。这两项属于模型说明和覆盖归档，不是 TLC 发现的生产缺陷。
