# L0 运行记录

2026-10-20，TLC 2.19（rev 5a47802，官方 v1.7.4 发布包），Java 1.8.0_462 x86_64，macOS 15.7 aarch64。所有配置固定 1 worker，无 fairness。模型、cfg 与 TLC jar 的 SHA-256 记录在 [results.json](04_evidence/results.json)。

```sh
level=tla/execution_models/broadcast_ring/L0_publish_borrow_reclaim
for cfg in "$level"/02_models/*.cfg; do
  name=$(basename "$cfg" .cfg)
  bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh \
    "$level/02_models/L0_BroadcastRing.tla" "$cfg" --det --coverage \
    > "$level/04_evidence/$name.log" 2>&1 || exit $?
done
```

全仓入口是 `bash scripts/check-tla.sh`；它校验固定 jar 哈希，为各次运行分配 scratch，并检查每个 cfg 的全部实际动作 generated 大于零。本级初次执行使用 skill 的 canonical runner；同一模型目录内五次运行串行，没有共用其他阶梯的 states。

| cfg | distinct | generated | depth | wall time | result |
| --- | ---: | ---: | ---: | ---: | --- |
| L0_BroadcastRing.cfg | 29,889 | 196,588 | 25 | 3 s | PASS |
| L0_BroadcastRing_two_readers.cfg | 930,364 | 9,587,623 | 33 | 92 s | PASS |
| L0_BroadcastRing_wrap.cfg | 11,324 | 71,291 | 21 | 3 s | PASS |
| L0_BroadcastRing_pending_wrap.cfg | 12,834 | 84,451 | 21 | 3 s | PASS |
| L0_BroadcastRing_wrap_two.cfg | 40,796 | 341,833 | 24 | 6 s | PASS |

五次穷举合计 107 秒；这是建模运行时间，不是 ring 性能结果。初次起草出现依赖量词作用域错误和 TLC 计算 `2^63` 的整数溢出，均在成功建模前修正，不计入状态空间结果。全部已保存 cfg 已实际运行，全部九个 invariant 通过。

## 动作覆盖

下表是各日志最后一段 coverage 的 generated 数；没有生成数为零的已列动作。`Init` 单列，不计入 17 个转换。TLC 将嵌套依赖量词中的 Release、Apply、Detach 标为带行列范围的 `SuffixNext`，表格按三个调用位置映射回动作名，原始范围保留在日志中。

| 动作 | primary | two_readers | wrap | pending_wrap | wrap_two |
| --- | ---: | ---: | ---: | ---: | ---: |
| Init | 1 | 1 | 1 | 1 | 1 |
| Join | 40 | 5368 | 前缀已完成 | 前缀已完成 | 前缀已完成 |
| Ready | 2604 | 309960 | 前缀已完成 | 424 | 前缀已完成 |
| Observe | 14796 | 970340 | 3744 | 4724 | 41552 |
| Claim | 7643 | 85848 | 4914 | 6424 | 12360 |
| Write | 5362 | 47543 | 2886 | 2886 | 3136 |
| Publish | 2471 | 23434 | 1314 | 1314 | 1568 |
| Abort | 7833 | 70977 | 4200 | 4200 | 4704 |
| Prepare | 19048 | 596834 | 7376 | 9188 | 30264 |
| Freeze | 12532 | 560970 | 3436 | 4040 | 20960 |
| Borrow | 2160 | 96984 | 162 | 282 | 1176 |
| BorrowIdle | 8760 | 636168 | 820 | 1120 | 7784 |
| StartQuiesce | 8668 | 530432 | 1722 | 2282 | 14392 |
| Quiesced | 12692 | 848744 | 3606 | 4236 | 29512 |
| RejectForged | 18030 | 1400756 | 3734 | 5234 | 40376 |
| Release | 8472 | 348720 | 872 | 1672 | 4704 |
| Apply | 62530 | 2828108 | 31670 | 35420 | 118648 |
| Detach | 2946 | 226436 | 834 | 1004 | 10696 |

还运行了 `coverage.sh .../L0_BroadcastRing.tla --det`，见 [coverage-parser.log](04_evidence/coverage-parser.log)：18 项（含 Init）全部触发。其余四份原始 coverage 使用同一 generated>0 规则检查。前缀配置使用专门 Next，不把已完成的 JOIN/ReadReady 列为后缀覆盖。

## 源码锁定与独立审查

完整源码哈希保存在 [results.json](04_evidence/results.json)。`mapped.rs` 的 guard 内部表示已从长期切片引用改为 `NonNull<[u8]>`，`bytes(&self)` 构造受 guard 借用约束的短引用；重新审查确认协议动作不变，沿用本轮 TLC 结果。

`anchors.sh` 在只复制本级输入与模型的临时 workspace 中运行 update/check，再把 lock 放回本级；临时 config 的 REPO_ROOT 指向当前仓库。这样不会重写并行完成的 submission 阶梯。结果见 [anchors-update.log](04_evidence/anchors-update.log) 和 [anchors-check.log](04_evidence/anchors-check.log)。后续可以运行 `anchors.sh check tla` 检查全仓。

risk_core 独立只读审阅了输入、抽象、模型与源码 guard，确认 pending min、Full 的 cache 副作用、实际槽内容见证、独立连续水位、真实 cursor detach 和 Freeze 后 drain；未发现安全反例或遗漏的关键 guard。Observe 的 claim 外限制来自 SDK 串行域，reasoning 已补充保守采样依据，模型无需修改。
