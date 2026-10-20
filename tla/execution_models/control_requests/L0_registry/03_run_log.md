# TLC 运行

TLC 1.7.4 发布包、单 worker、完整 BFS；通过 `run_tlc.sh <module.tla> <config.cfg> --det --coverage` 运行，每次独立 `JAVA_TOOL_OPTIONS=-Djava.io.tmpdir=<temporary-directory>`，结束清理 scratch。所有配置已实际运行。

| 配置 | generated | distinct | depth | 结果 |
| --- | ---: | ---: | ---: | --- |
| L0_ControlRequests.cfg | 5,682 | 370 | 6 | PASS |
| L0_ControlRequests_pending.cfg | 352,574 | 13,442 | 10 | PASS |
| L0_ControlRequests_two.cfg | 28,026 | 1,570 | 8 | PASS |

完整输出位于 `04_evidence/*.log`，每个 Next 动作都生成了后继；计数和源码/模型/配置 SHA-256 见 `04_evidence/results.json`。
