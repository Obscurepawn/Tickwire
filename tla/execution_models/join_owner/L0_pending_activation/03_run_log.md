# TLC 运行记录

从仓库根执行 `python3 tla/execution_models/join_owner/run.py`。runner 固定单 worker、`-coverage 1`、2 GiB Java heap，为每次运行分配独立 java 临时目录和状态目录，完成后删除 scratch。TLC 1.7.4 JAR SHA-256 为 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`。实际完整命令记录在每份原始日志首行。

| 配置 | generated | distinct | depth | 秒 | Next 动作 | 结果 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [L0_JoinOwner.cfg](04_evidence/L0_JoinOwner.log) | 34226 | 2181 | 23 | 1.922 | 22 | PASS |
| [L0_JoinOwner_two.cfg](04_evidence/L0_JoinOwner_two.log) | 14022367 | 321256 | 35 | 79.547 | 22 | PASS |
| [L0_JoinOwner_wrap.cfg](04_evidence/L0_JoinOwner_wrap.log) | 2689 | 172 | 14 | 1.291 | 16 | PASS |
| [L0_JoinOwner_wrap_two.cfg](04_evidence/L0_JoinOwner_wrap_two.log) | 336733 | 7652 | 25 | 3.507 | 16 | PASS |

每个配置最后一段 coverage 中的全部 Next 动作 generated 均大于 0；Init 另计。成功 detach、原 ACK 重试和冲突拒绝各自显示覆盖，不能用汇总 NextBase 计数代替它们。各动作 distinct 为 0 时仍需 generated>0，表示该输入被实际枚举但只产生已有状态。

首轮四配置同样通过，但 TLC 汇总了状态相关量词中的三个 detach 动作，且量词域没有包含 applied 后续推进后的历史 ACK。最终版本把三类输入分成具名动作，补足精确历史重试后重跑以上全部配置。没有属性反例。最终运行后只将源码引用注释改为完整仓库相对路径，未改变动作、配置或行号。

机器可读计数、每动作覆盖、模型/源码哈希位于 [results.json](04_evidence/results.json)。源码行锚点共 1221 条，位于 [anchors.lock](02_models/anchors.lock)；它们是漂移检查，不是测试覆盖率。
