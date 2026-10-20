# 检查结果

PASS。三个配置均完整穷举，检查 10 个不变量。本级覆盖Spot 双资产预留、重复与迟到成交费用、ACK/中止、Unknown 和终态累计矛盾。独立审阅者 root、IPC 已对照实际源码复核转移和公式，未保留开放候选。

| 配置 | generated | distinct | 深度 | 时间 |
|---|---:|---:|---:|---|
| L0_SpotFacts_v3 | 7892392 | 176596 | 16 | 40s |
| L0_SpotFacts_v3_two | 8626384 | 192984 | 15 | 36s |
| L0_SpotFacts_v3_tight | 8053279 | 180131 | 16 | 41s |

## 源码映射与覆盖

变量和动作映射位于模型头部及逐动作注释，输入域见 `00_input.md`，抽象理由见 `01_reasoning.md`。本级选定设计面包含 21 个 Next 转移，全部有源码引用、没有 UNCLEAR；三个配置的每个 Next 与 Init 都实际生成过状态。完整 generated 计数见 `03_run_log.md`。这是指定设计面的模型覆盖，不是可执行程序行覆盖率。

一次公开账户调用是原子动作，符合源码独占 `&mut self` 加候选整体替换。Fill 的空内嵌费用与已确认非终态观察组成一个完整输入，独立 Fee/Terminal 表示随后另一次调用。有限参数直接对应可执行 Rust 实例；身份/字节解析、i128 全域、其他产品参数、WAL 同步与恢复资格未纳入本级性质。

源代码、配置、工具和最终日志摘要保存在 `04_evidence/results.json`，精确引用行由 `02_models/anchors.lock` 固定。已完成独立审阅、全部 cfg、动作覆盖及范围核对；后续源码语义变动需重新审查和运行。

本次源码同步只调整 Cancel/Query 的快照时效检查，并新增所有者主动冻结入口。模型选定的准入均为基线有效期内的增险命令，没有撤单、查询或外部 freeze 调用；转移与不变量不变，未重跑 TLC。已按最新源码刷新引用行，原模型、源码摘要和锁记录保存在 `docs/validation/account/pre-stale-cancel/`。新增行为由 `tests/nonincreasing.rs` 的真实 Red→Green 覆盖。
