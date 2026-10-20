# L0 反例记录

正式三组配置没有性质违反。首次 authoring-incomplete-state.log 是模型赋值括号错误，修复后消失，未形成代码候选。

真实实现的四个失败已先由 Rust Red 复现，见 [Mock 报告](../../../../docs/validation/mock-abort/report.md)。本模型验证修复后的有限路径，不把这些源码/Rust 发现改记为 TLC 发现。
