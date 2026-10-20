# USD-M 收入链阶段验证

当前证据覆盖规范输入、账户去重/保守占用、稀疏观察绑定、输入 WAL、收入区间扫描与专用基线审批。源码摘要见 `stage1-sources.json`。stage1-sources.json 保留 app 调度前的摘要；stage2-sources.json 对应新增 owner 接线。新资金费和观察的闭合解除仍在后续实现范围。

app 可选配置现已接真实收入 reader、三个快照、未签名提案输出和独立签名文件轮询。`controller-check.log` 的 `cargo check -p tickwire-app` 通过；`controller-unit.log` 的 `cargo test -p tickwire-app --lib reconciliation` 3 项通过，其中 1 项为新增文件形状/长度校验，另外 2 项为原对账维护回归。未运行新的网络场景。

## 已执行命令

命令均在仓库根执行，Cargo 位于 `$HOME/.cargo/bin`。Red 日志来自可编译后的行为断言失败；编译错误未作为 Red。

| 命令 | Red | 最终结果 |
|---|---|---|
| `cargo test -p tickwire-venue --test account_input` | `canonical-red.log` | `canonical-green.log`：3 项通过 |
| `cargo test -p tickwire-account --test account_inputs` | `account-red.log` | `account-final-green.log`：5 项通过 |
| `cargo test -p tickwire-account-codec --test account_inputs` | `codec-red.log` | `codec-final-green.log`：1 项通过 |
| `cargo test -p tickwire-binance-account --test account_observation` | `observation-red.log` | `observation-final-green.log`：2 项通过 |
| `cargo test -p tickwire-binance-account --lib income` | `income-red.log` | `income-green.log`：3 项收入解析/扫描通过 |
| `cargo test -p tickwire-binance-account --lib income_import` | `import-red.log` | `import-recovery-green.log`：3 项通过 |
| `cargo test -p tickwire-binance-account --lib` | — | `account-lib-green.log`：20 项通过，包含上表收入和审批单元，不重复计数 |
| `cargo check -p tickwire-app -p tickwire-binance-account` | — | `import-check.log`：通过 |

`persisted_approval_reopens_with_original_acceptance_and_preserves_coverage` 使用测试内存 Store 执行真实物理帧、输入记录编码、同步接口和恢复解析。它以 writer 4 接受审批，以 writer 5 重新打开，确认历史审批按原接受时刻重验、持久请求 ID 保留、基线收入不重复入账；截止前迟到的新键保持负额占用和冻结。该测试的签名者是显式配置的测试 risk-admin，生产模块不含私钥或自动审批函数。

`complete_queries_require_separate_risk_admin_approval` 覆盖有效专用签名、错误签名、到期和 writer 错配。`risk_admin_cannot_replace_missing_query_evidence` 覆盖缺少收入页及原文与规范行冲突。审批断言与查询证据分别校验，资产 updateTime 未用于构造覆盖权限。

本目录没有收入 HTTP 网络运行、掉电、Linux 或形式验证结果。`IncomeReader` 复用真实有界 TLS 请求实现；本阶段编译和纯单元不替代后续实际 owner 链运行。
