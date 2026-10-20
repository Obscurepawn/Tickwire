# 恢复闭包验证阶梯

单个 `Persistence<S>` 通过 `&mut self` 串行操作。FileBlobStore 对目录持有独占锁，当前无删除 API。L0 检查对象、manifest、不可变 binding 和私有能力之间的完成顺序，按真实文件 rename 可见与目录同步完成分开建模。

后续 L1 纳入持久 DependencyRelease、多 pin 所有者和垃圾回收；当前尚无这段生产代码，不在 L0 猜测实现。授权签名、策略输入采集完整性及跨主机复制由对应服务验证。
