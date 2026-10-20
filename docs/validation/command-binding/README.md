# 命令摘要与持久对象验证

`red-all.log` 保存编译成功后的三项行为失败；`dependency-red.log` 保存缺少完整命令依赖仍可提交的失败断言。`green-initial.log` 是摘要编码修正后的回归；`dependency-green.log` 保存引入命令依赖时发现的旧测试输入不一致：测试改变 intent ID 后没有更新命令摘要，已修正该输入。`green.log` 保存其后的专项回归。

`generate-vectors.py` 独立计算 schema 2 CBOR 和哈希；原 schema 1 文件留在原目录，用作旧版本拒绝样本。专项命令与语义见[实现规格](../../../specs/implementation/command-binding.md)。

完整检查使用同一份源码，结果如下：

| 检查 | 结果与记录 |
|---|---|
| macOS | `scripts/check-rust.sh` 退出码 0；[日志](macos.log)包含工作区单元测试、集成测试、文档测试、格式和 Clippy 检查 |
| Linux aarch64 | `scripts/check-linux.sh` 退出码 0；[日志](linux.log)包含相同检查及 Linux 内核接口测试 |
| 源码与配置 | [source.sha256](source.sha256)记录 110 个源码、配置、样本和生成器文件；两份测试目录与提交暂存内容逐字节一致 |
| 独立编码器 | [schema 2](vectors.log)和[原 schema 1](legacy-vectors.log)固定样本均匹配 |
| 文档与设计约束 | [文档链接](documentation.json)和[设计约束](contracts.json)检查通过 |
| TLA+ | [业务日志 L1](../../../tla/execution_models/business_journal/L1_command_binding/05_output.md)的三组配置通过 |

`line-mapping.json` 保存本轮源码行号映射，`prior-anchors/` 保存修改前的锁文件。重新定位的既有模型公式不变：submission L0 和 mock_abort L0 固定合法请求；business_journal L0 固定成功匹配的闭包；recovery_closure L0 将对象内容和种类校验抽象为验证结果。新增的命令摘要与依赖匹配在业务日志 L1 展开。历史 submission L1/L2 的事件顺序差异仍保持原来的历史状态。
