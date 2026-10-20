# 不可变快照与业务日志阶段验证

在 `f0cf9d5` 之后，本阶段加入不可变 memfd、Topic 快照接收业务校验和业务日志确定性编码。源码均在验证前冻结；完整 source SHA-256 见 [source.json](source.json)。

| 检查 | 结果与证据 |
| --- | --- |
| macOS arm64 全仓格式、全部 targets、doctest、Clippy | [native.log](native.log)，`scripts/check-rust.sh` 返回 0 |
| Linux aarch64 同一组全仓检查 | [linux.log](linux.log)，`scripts/check-linux.sh` 返回 0，镜像摘要由脚本固定 |
| 设计契约与黄金向量、风险、配置、部署算术 | [contracts.log](contracts.log)，验证器返回 0 |
| 本地文档引用及图片资产 | [documentation.log](documentation.log)，验证器返回 0 |
| 源码锚点与盘口模型证据核对 | [model-audit.json](model-audit.json)，9 组锁共 2,888 行无漂移，八组新配置摘要/状态数/动作覆盖匹配原始 TLC 输出 |

Rust 日志中的 benchmark target 使用 1000 样本，只承担可运行性检查；性能结论仍引用先前完成的[盘口五轮完整实验](../book-small-batch/report.md)。控制握手、物理 WAL、ring 和 Mock 的既有回归同时运行通过。

本阶段专项证据分别位于[不可变资源](../immutable-resource/README.md)、[快照校验与交付](../topic-snapshot/README.md)、[业务日志编码](../journal-codec/README.md)。两个源码审阅发现均有实际 Rust 失败断言和修复后回归：重复成员 snapshot ID，以及 writer 执行代次与原订单代次混用。其归因是源码审查与测试，不是 TLA+ 反例。

盘口生命周期及覆盖知识的有限 TLA+ 模型使用已提交且未再改动的 book 源码，模型自己的范围、配置、覆盖和结果另存于 [book_state](../../../tla/execution_models/book_state/ROADMAP.md)。当前实现仍未接通完整 JOIN owner/cursor 协议，也未把业务 codec 作为真实 execution Journal 的提交与恢复适配器。
