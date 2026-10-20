# L0 校准运行记录

正式模块 SHA256 为 `cc453c8ae056e4eccbcecc362d88f94c6bbac549ae6c2540e3e12a330d475ffd`。TLC 2.19 使用单 worker；每个配置有独立的模型副本和状态目录。

## 数值工具验证

`04_evidence/int-limit.log` 记录实际 `2147483647+1` 溢出，退出码75；这是 TLC 原生整数限制。`word-vectors.py` 使用 Python 任意精度整数生成79个样例，并与已归档的期望逐项比较；另有4条排序和量化关系断言。`word-final-check.log` 的自检实际通过：生成2个状态、2个不同状态、深度2。`word-embedding-check.log` 确认正式模型内联的助手和全部算术断言与已审阅文本逐字一致。

复跑算术自检：

```sh
python3 tla/execution_models/clock/L0_calibration/04_evidence/word-vectors.py \
  --output-dir /tmp/tickwire-clock-words-recheck
bash "$HOME/.codex/skills/vibe-tla/scripts/run_tlc.sh" \
  /tmp/tickwire-clock-words-recheck/ClockWordsCheck.tla \
  /tmp/tickwire-clock-words-recheck/ClockWordsCheck.cfg --det --coverage
```

## 核心模型

四配置的复跑命令；输出目录须尚未包含同名配置子目录：

```sh
python3 tla/execution_models/clock/L0_calibration/04_evidence/run-models.py \
  --output-dir /tmp/tickwire-clock-recheck --parallel 4
```

实际运行使用同一 `run_tlc.sh`，参数为 `--det --coverage=60`。60表示中间覆盖报告的分钟间隔，完成时仍输出完整的最终覆盖块。正式动作覆盖只统计最后一个完整块。

| 配置 | 生成状态 | 不同状态 | 深度 | 秒 | 结果 |
|---|---:|---:|---:|---:|---|
| boundary | 493130 | 64292 | 12 | 92 | PASS |
| primary | — | — | — | — | 运行中 |
| rounding | — | — | — | — | 运行中 |
| usdm | — | — | — | — | 运行中 |

TLC 将直接展开 `Apply` 的部分入口显示为同一助手名，并保留调用点行列范围。对应关系为：296行是 Begin，310行是 Read，313行是 ReadWrongDomain，314行是 Sign，317行是 CheckForeign。其余入口按动作名显示；共 Init 加15个 Next 入口。`CancelStale` 等入口可以只生成已见状态，判断是否执行使用 generated 列。

## 已保留的运行中断

`initial-runs` 保存原模型、配置和原始日志。首次并行调用共用模型目录时，运行脚本的结束清理删除了其他进程的状态文件；同时启动的两份运行也出现状态目录重名。改用不同模型目录后，一次共享 Java 临时模块解析仍发生竞争，随后为每个进程设置独立 `java.io.tmpdir`。这些是运行组织错误，没有产生不变量反例。

边界配置在原模型上已完成并保留 PASS。其他三份运行曾因错误怀疑初态缺少调用历史而主动终止；root 对照 `new(first_serial)` 指出该序号本就是直接构造参数，建议随即撤回，模型逐字恢复原 SHA，三个配置按原文重跑。没有把这些中断日志算入通过结果。
