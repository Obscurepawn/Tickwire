# v2 执行记录

复现命令：`python3 tla/execution_models/binance_private_session/L0_session_lifecycle/04_evidence/run.py`。脚本冻结模型/配置副本，逐个运行固定TLC jar、Java24、单worker、2GiB堆及独立临时目录；每份日志首行记录实际命令。

| 配置 | generated | distinct | depth | 墙钟秒 | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| L0_PrivateSession_v2.cfg | 24,478 | 12,018 | 27 | 2.778 | PASS |
| L0_PrivateSession_v2_buffer.cfg | 2,244 | 1,043 | 20 | 1.304 | PASS |
| L0_PrivateSession_v2_logon.cfg | 3,476 | 1,881 | 28 | 1.371 | PASS |
| L0_PrivateSession_v2_usdm.cfg | 536,304 | 267,643 | 34 | 17.798 | PASS |

检查16项不变量。动作覆盖只取每份日志最后完整块，按产品配置取并集；逐动作计数、源码/model/jar/cfg/log SHA见results.json。旧v1模型和日志保存在runs/pre-final-sample-v2及验证目录的源码归档。

每份末块包含32个入口，跨配置并集为 Init+33 Next，全部至少在一个配置中 generated>0。USD-M 不执行签名预算回调，GateBudgetExpires 和 FinalSampleExpired 在该配置为0，Spot 配置覆盖这两个入口。独立审阅据源码要求为预算动作增加 Signed guard，修前运行单独保存在 runs/pre-signed-review。
