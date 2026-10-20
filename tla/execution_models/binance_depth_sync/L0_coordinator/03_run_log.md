# L0 运行记录（尚未完成）

最终模型 SHA256 为 `5cf86194dd707b058fdebdd0b4d5255b7f491445b73c78c09bf53c59218f5c97`。两份主配置已通过；两份扩大配置仍在完整搜索，本层尚未关闭。

| 配置 | generated | distinct | 深度 | 实际状态 |
| --- | ---: | ---: | ---: | --- |
| [Spot 主配置](02_models/L0_DepthSync.cfg) | 1,851,021 | 73,269 | 14 | PASS，exit 0；TLC 11 秒，runner 12 秒 |
| [USD-M 主配置](02_models/L0_UsdM.cfg) | 1,876,613 | 73,897 | 13 | PASS，exit 0；TLC 12 秒，runner 13 秒 |
| [Spot 扩大配置](02_models/L0_SpotBuffered.cfg) | — | — | — | 运行中，session 60186 / PID 94824 |
| [USD-M 扩大配置](02_models/L0_UsdMBuffered.cfg) | — | — | — | 运行中，session 75198 / PID 95685 |

两份已完成日志均有 Init 和全部 22 个 Next 动作的最终覆盖行，generated 全部大于 0。扩大配置的实时进度在 [Spot 日志](runs/L0_SpotBuffered.log) 和 [USD-M 日志](runs/L0_UsdMBuffered.log)；中途统计不作为最终 PASS 结果。

所有配置使用相同冻结模型、1 worker、coverage 1 minute，未添加状态约束。USD-M 扩大配置在 [parallel-usdm](runs/parallel-usdm/inputs.sha256) 的逐字节相同副本中独立使用 states 目录。两进程仍正常运行；资源与主机暂停观察分别见 [resource-observation.txt](runs/resource-observation.txt) 和 [host-pause-observation.txt](runs/host-pause-observation.txt)。

自然结束后，[check_results.py](check_results.py) 调用仓库 `scripts/tlc_coverage.py::final_counts` 校验每份日志成功结束、最终覆盖块完整、最终队列归零，再检查本层 22 个 Next 动作逐配置均产生后继。只有四份均通过后，才生成最终计数、配置/日志摘要与本层关闭输出。
