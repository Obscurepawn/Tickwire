# 账户恢复依赖验证

kind 14 和 15 分别绑定完整账户基线及准入之前的历史前缀。测试先在旧种类范围上运行，成对依赖用例因 `object descriptor` 拒绝而失败，见 `red.log`。新增检查后，`tickwire-recovery --all-targets` 的 16 项测试通过，见 `native.log`。

新增测试检查完整绑定的 publish/verify 与 manifest 编码、旧 manifest 的两个账户摘要为空、缺少任一种依赖或完整命令、重复种类，以及对象实际字节缺失。只有完成存储校验与同步的 `VerifiedClosure` 才能返回账户摘要。

真实文件与发送组合见[账户发送验证](../account-gateway/README.md)。旧恢复依赖模型的持久化状态迁移没有变化，kind 13～15 的具体形状和账户语义由当前 Rust 测试覆盖；旧 TLC 结果不扩展为这些新增语义的形式证明。
