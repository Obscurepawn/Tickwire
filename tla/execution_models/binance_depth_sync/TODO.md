# 当前待办

L0 的 Rust 源码与模型文本已冻结，主配置两产品通过；扩大配置仍在完整枚举。模型 SHA256 为 `5cf86194dd707b058fdebdd0b4d5255b7f491445b73c78c09bf53c59218f5c97`，四份配置域未缩减。

## STAGED

- Spot 扩大配置：exec session 60186，Java PID 94824，正式 `02_models` 目录运行，输出 `L0_coordinator/runs/L0_SpotBuffered.log`。
- USD-M 扩大配置：exec session 75198，Java PID 95685，在 `runs/parallel-usdm` 的字节相同副本中运行，输出 `L0_coordinator/runs/L0_UsdMBuffered.log`。
- 两进程各 1 worker、coverage 1；不同 states 目录。待两者自然结束后运行 `L0_coordinator/check_results.py`，只接受完整 exit 0 日志。
- 最终文档生成脚本暂存在 `/tmp/finalize-depth-sync-docs.py`，读取 results.json 才会生成 03/05 与索引条目。生成后核对本地链接/围栏、风格、575 行锚点与所有摘要，确认源/并行副本文本一致。
- 首轮满队列重复事件枚举已作关系等价优化，ipc 已独审；旧文本/已完成主日志/中止日志在 runs 中保留。最终统计不能混用旧结果。
- 约束可达性探测已得到 7 步 LateReplayFailure 轨迹；它不作为安全性 PASS。四份正式配置均无状态约束。

顶层索引由 root 更新，本目录完成时提供 INDEX_ENTRY.md。
