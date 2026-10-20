# 恢复依赖闭包与持久绑定

`tickwire-recovery` 在业务 WAL 提交前保存并验证恢复依赖。`Persistence<S>` 串行持有一个 `BlobStore`；真实文件存储和 Mock 使用同一接口。只有服务逐对象核对内容、依赖图和实际有序输入，并同步不可变绑定后，才能产生私有构造的 `VerifiedClosure`。业务 runtime 将该值与精确订单摘要核对，再提交 WAL。

`ClosureBinding` 包含 risk domain、intent ID、原执行代次、业务摘要、策略代次、配置版本及 `recovery_object`。`(risk_domain_id,recovery_object)` 对应的 manifest root 一旦发布不可更改；不同订单或不同摘要复用此键返回冲突。恢复按 WAL 中的同一绑定重新读取全部依赖，缺失或损坏即失败。

## 内容和输入

`RecoveryManifest` 保存 binding、逻辑策略/实例、授权与 operation 引用、恢复模式、故障模型、pin ID、输入边界、对象及依赖边。对象描述符按 `(kind,content_hash)` 严格排序；边按 `(from,to)` 排序且唯一。十二类必需依赖与既有复制契约一致，完整对象集合由 manifest 引用，内部边必须组成有向无环图。hash 使用 SHA-256，根覆盖固定数组结构的确定性 CBOR，包含不可变 pin ID，排除存储收据和动态 pin 所有者。

kind 12 保存实际有序输入块。块带 stream、起始序号、前缀 hash 和逐条输入：类型、序号、长度及完整载荷。五种输入类型分别是行情、执行回报、定时器、配置和模型更新。服务重新计算逐条链，按序连接所有块，要求无洞、无重叠，且准确到达 manifest 的决策边界。kind 11 保存检查点或批准的重建契约；它的内容 schema 和批准权限由受信授权提供者验证，闭包服务不根据一个 hash 声称策略已恢复。

输入块由受信输入 sequencer 或隔离适配器提交。这里验证完整字节及顺序；是否确为策略处理的全部输入仍依赖该采集边界。`deterministic_replay` 与 `declared_rebuild` 保持显式类型，不能在缺少输入时自动切换。

种类 13 保存完整交易命令，最多一个。`VerifiedClosure.command_digest()` 只返回实际校验并同步的该对象摘要；没有种类 13 时返回 `None`。schema 2 业务日志要求它与意图的命令摘要一致，详见[命令绑定接口](command-binding-interface.md)。

## 文件完成点

`BlobStore::put_sync` 必须幂等，已有键的不同字节返回冲突。`FileBlobStore` 对私有目录持有独占文件锁；先写临时文件并同步，再 rename 到固定内容键，最后同步目录。重试遇到相同文件时仍同步文件与目录，覆盖上次 rename 成功而目录同步失败的情况。`publish` 和恢复用 `verify` 都重新完成各依赖、manifest、binding 的同步后才签发能力；只读到正确字节不足以证明先前的目录同步完成。读取限制实际字节数，拒绝符号链接和非普通文件；父目录及已有目录必须来自受信部署。

对象和 manifest 完成同步后才写 binding 文件。此 binding 同时成为持续 pin：当前实现不提供删除或垃圾回收，因此不会回收未知订单的依赖。崩溃留下的未绑定对象只占空间，不授予发送能力。每个对象最多 64 MiB、闭包最多 256 MiB、最多 4096 个对象及 16384 条边；容量不足拒绝新增闭包。后续持久 DependencyRelease 和 pin 回收需要单独状态机。

`VerifiedClosure` 证明该次验证与后端存储完成点，不能替代执行授权、资本基线、Ed25519 LocalCommitProof、镜像收据或接管资格。外部授权服务通过独立接口接入；测试的 Mock 明确保留这个身份。

## 先行测试

先以编译通过的失败实现验证完整闭包发布/重开、精确绑定与冲突、缺对象/损坏、缺种类/环路、实际输入缺口/重叠/链错、失败同步不产生能力，再补 canonical 编解码和严格长度边界。文件测试核对原目录重开及持久绑定；模型验证对象完成、manifest、binding 与能力交付之间的故障次序。
