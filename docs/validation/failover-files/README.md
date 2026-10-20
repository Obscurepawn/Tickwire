# 源日志封口与文件恢复组合

`crates/tickwire-app/tests/failover_files.rs` 的 5 项定向测试通过，命令为：

```sh
cargo test -p tickwire-app --test failover_files -- --nocapture
```

`green.log` 对应最终格式化后的测试；`first-run.log` 是格式化前的首次可执行结果，亦为 5 项通过。本轮新增测试和 `ring` 测试依赖，未修改生产恢复逻辑。`results.json` 保存最终命令、测试名及相关源码摘要。

## 实际完成的组合

测试各自创建临时目录。真实 `AccountJournal<FileBackend>` 从规范基线开始，同步记录一笔 USDT +7 的未归属现金流；真实 `AccountIngress<FileStore>` 随后创建输入日志。基线、bootstrap 和输入原文写入 `SharedBlobStore<FileBlobStore>`。源端关闭两个写入者后，`SourceWorker::seal_account` 逐条读取实际日志并生成带签名的 `SourceRecoverySeal`。最终输入根只由这个封口引用，普通账户批次不含它。

镜像通过 `object_chunk` 接收实际对象，通过 `append` 同步记录实际账户前缀。生产 `failover::restore` 从镜像恢复到全新目录，再以真实账户和输入 API 重开，最后写入完成绑定。`validate_start` 返回的两个 required frontier 与原始物理封口逐字段相同；账户和输入记录的字节、原 writer 代次都保持一致。恢复账户总额为 1007 USDT，账本序号为 1，状态仍为冻结；新部署 writer 为 5，旧记录 writer 为 4。

该现金流无需交易授权闭包。测试仍绑定同一规范基线、账户历史、输入前缀和 bootstrap；未构造可默认通过的 `Admit` 验证器。

## 拒绝条件

- 最终输入对象尚未复制：即使镜像账户前缀完整，恢复也在创建目标目录前失败。
- 两份日志物化后，过小的输入容量使语义打开失败：三个目录保留 `FAILOVER-IN-PROGRESS`，没有完成文件，启动校验拒绝。
- 已完成目录删除完成文件或最终输入根对象：启动校验拒绝。
- 已完成目录被另一 writer 代次使用：完成绑定不匹配，启动校验拒绝。
- 测试控制器签名被改、旧执行者身份不匹配或证据过期：真实 `Verifier` 拒绝。

每种目录损坏使用独立恢复目标。测试不读写运行中的场景卷、二进制或私钥文件。

## 可信输入边界

测试控制器使用专用测试密钥，声明旧执行者出口、会话和凭据已被隔离。生产 `Verifier` 实际检查签名、完整身份绑定与时效；这些测试没有执行主机或网络隔离，因而不构成外部隔离验收。镜像对象通过库接口传输，未启动 mTLS 网络或进程接管场景；文件写入和同步调用是真实的，未模拟掉电。
