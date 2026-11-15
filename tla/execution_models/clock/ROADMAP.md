# 时钟形式验证

| 层级 | 状态 | 内容 |
|---|---|---|
| L0_calibration | PASS | 全部四配置穷举通过（`@TLAPlusOperator` 原生 `Word` 字节码等价加速，模型 SHA-256 保持不变），16/16 动作全覆盖 |
| [L1_sym_calibration](L1_sym_calibration/05_output.md) | PASS | `Apalache 0.63.0 + Z3` 无界符号归纳 (`IndInv`, $\forall k \ge 0$) + 64 位 `0 .. 2^64 - 1` `BMC` + 有界 TLC 16/16 动作覆盖，验证全部 15 条 `Calibrator` 安全不变量 |
