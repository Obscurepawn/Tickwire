# Linux Spot 发布预检 v2 与策略交接

F2 固定源码上的 `./scripts/run-core-chain-linux.sh spot-handover` 实际通过，退出码 0。场景使用独立 MD、exec、策略、lifecycle 协调器和 mTLS 镜像进程，WAL 位于 Linux Docker volume。运行身份见 [metadata.json](metadata.json)，原始 [源码 SHA](workspace-source-sha256.txt) 和 [部署二进制 SHA](deployment-binaries-sha256.txt) 保持原文件字节。

本次 operation 使用 schema2：从旧策略实际消费的账户位置 3 取得完整恢复资料，执行目标状态迁移和独立回滚探测，再由真实签名 AppliedReceipt 推进 Prepare、Commit、Release。旧策略完成行情退订并退出后，场景才释放迟到成交；新管理者继续消费成交、费用闭合和重新对账记录。

| 检查 | 实际结果 |
|---|---|
| 目标恢复与回滚探测 | 目标 PID 2485；独立 probe PID 2487；`action_remains_consumed=true`，没有重发已消费动作 |
| 交接回执 | Prepared、Committed、Released 齐全；管理权记录位于账户序号 4 |
| 旧策略退出 | PID 2476，generation 5，Host 记录退出码 0 |
| 新策略消费 | generation 6，真实 READY/CONSUMED 确认到账户位置 9 |
| 订单管理权 | origin instance `02…02` / generation 5；manager instance `06…06` / generation 6；owner_revision 2 |
| 成交和费用 | 买入 0.001 BTC；terminal、fee_reconciled 均为 true，unknown 为 false |
| 最终账户 | BTC 1.001、USDT 940；预留和赤字均为零；未决订单为空 |
| 日志与镜像 | 9 条账户记录、16 条输入记录；源端与镜像的 9 条规范账户记录同序同字节 |

[关键 JSON](key-results.json)抽取实际预检、签名回执字段、旧进程退出、新进程消费和账户结果。原始请求与签名回执保存在 [artifacts/handover](artifacts/handover/)，实际最终消费凭据见 [CONSUMED](artifacts/next/reports/CONSUMED-06060606060606060606060606060606-6.json)。[账户报告](account-report.json)来自写入者关闭后的离线恢复，整体 `frozen=true`，不表示运行中的交接失败。

归档时再次核对了 469 个源码/合同文件、6 个已部署二进制的 SHA，并将导出的 9 条规范记录与保留 volume 中的镜像内容对象逐字节比较；结果见 [provenance-check.json](provenance-check.json)和[记录比较](canonical-record-comparison.json)。[原始结果校验表](original-result-sha256.txt)覆盖本次直接复制的全部 18 个文件；原日志未改写。

本次验证同一执行域内置 FixedDemo 的实际迁移、回滚探测和交接。泛型策略从固定来源到当前切点的完整合并输入尾部重放仍未实现；多执行域发布和执行者故障隔离接管另有各自范围。较早的 [schema1 场景](../spot-handover/README.md)保持独立，不替换其历史结果。
