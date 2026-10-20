# 顶层索引增量

`execution_models/recovery_closure/L0_publish_verify` 已关闭，忠实故障注入，三配置 PASS：L0_RecoveryClosure 5,093 generated / 2,345 distinct / depth 51；L0_Conflict 37,333 / 15,097 / depth 56；L0_MultipleDependencies 9,456 / 3,898 / depth 66。每组 Init 和 22 个动作全部生成状态。

初版只选择完整 disk/cache，risk_core 独立审阅发现多次失败可积累多个脏键；当前 Survivors 独立枚举每个脏键的旧/新内容，DirSync 成功推进整个 cache，失败可部分完成。故障域修正后保持原配置规模，三组重新穷举通过，risk_core 二审无阻断项。初版模型及日志原样保存在 L0/pre-review。

源码：crates/tickwire-recovery/src/lib.rs 的 put_object/publish/verify/check、files.rs 的 open/get/put_sync。当前源码锚点和完整 SHA 清单位于 L0 与阶梯目录。字节解析/输入覆盖/业务授权由具体 Rust 测试及可信服务契约承担，L0 未形式验证这些算法；L1 等真实释放与 GC 代码。
