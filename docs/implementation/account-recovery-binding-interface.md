# 账户恢复依赖绑定接口

账户准入的恢复依赖使用现有 `RecoveryManifest`，新增两个可选种类：

| 种类 | 内容 | 摘要来源 |
|---|---|---|
| 14 | 完整账户基线及目录 | `CanonicalBaseline::digest()` |
| 15 | 准入之前的账户日志前缀 | `HistoryFrontier::digest()` |

这两个种类必须成对出现、各一个，同时必须包含一个完整命令对象（种类 13）。种类 1 保存完整 `CanonicalRecord::Admit`，其摘要作为 `ClosureBinding.business_digest`。种类 1～12 的原有依赖要求不变；旧 manifest 的编码和摘要保持原样。

`VerifiedClosure` 增加只读的 `account_baseline_digest()` 与 `account_history_digest()`。只有真实读取并同步全部对象后，`Persistence::publish/verify` 才返回这些摘要。没有账户扩展的旧 manifest 返回 `None`，不能用于账户发送协调。

账户发送协调器以自己持有的基线和日志前缀核对两个摘要，同时核对完整准入记录、命令和请求身份。恢复时，日志先验证物理记录，再逐条重建前缀，闭包验证使用该准入记录之前的精确前缀。对象种类检查只保证依赖完整；它不代替账户重放或执行授权。

前缀保存基线、日志流和连续历史的摘要。完整历史仍由同一账户 WAL 保存和恢复，前缀对象不能替代缺失的日志记录。未来复制实现必须同时保存这些已有依赖。
