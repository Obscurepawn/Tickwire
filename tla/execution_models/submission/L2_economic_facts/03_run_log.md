# L2 TLC 运行记录

所有模型使用仓库固定的 TLC 1.7.4 jar，SHA-256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`，单 worker、开启 action coverage。日志中的本机仓库路径和系统临时目录替换为 `$REPO`/`$TMP`，状态、计数、错误及时间未改写。

## 原模型

命令从仓库根目录运行；`SKILL` 指已安装的 `vibe-tla` skill 目录，`LEVEL=tla/execution_models/submission/L2_economic_facts`。

```sh
"$SKILL/scripts/run_tlc.sh" "$LEVEL/02_models/L2_EconomicFacts.tla" --det --coverage
"$SKILL/scripts/run_tlc.sh" "$LEVEL/02_models/L2_EconomicFacts.tla" "$LEVEL/02_models/L2_EconomicFacts_boundary.cfg" --det --coverage
```

| 配置 | 结果 | Generated | Distinct | 深度/反例状态 | 耗时 |
| --- | --- | ---: | ---: | ---: | ---: |
| primary | PASS | 139,711 | 24,036 | 完整深度 8 | TLC 4 秒 |
| boundary | candidate L2-001 | 38,635 | 25,367 | 反例 6 状态 | TLC 3 秒 |

原 primary 的 13 个 Next 动作均生成后继状态。boundary 在违反不变量处提前停止，覆盖仅对应已探索前缀。完整日志见 `04_evidence/pre_fix/primary.log` 和 `boundary-red.log`。

最初一次解析失败是依赖量词写在同一声明中，使 TLC 无法解析 `Facts(i)` 中的 i；改为嵌套量词后通过。最初并行运行 scaling 与 long 时，runner 的默认共享 `states` 清理互相影响，两次都以磁盘目录不存在退出。这两次是运行设施错误，没有验证结论，日志分别归档为 `scaling-infrastructure-error.log`、`long-infrastructure-error.log`。后续每次运行使用独立 `-metadir`，避免共享 scratch。

## 修复后的 v2

最终模型将经济事实按 Fill/Fee/Terminal、Duplicate、Invalid/Conflict/Capacity/Overflow 拆成命名动作，便于逐项验证覆盖；状态迁移与最初的合并动作相同。拆分前 v2 primary 也通过（24,036 distinct），scaling 通过（1,485,288 distinct，14,278,433 generated，5 分 05 秒）；最终结论采用下面重新跑完的命名动作版。

一次并行启动遇到 TLC 解包标准模块时的 `NullPointerException`，未产生模型结论。此后 Java 临时目录也按进程隔离。实际完整配置运行方式如下；这里以用户主目录定位已安装 skill，与运行时绝对路径等价。

```sh
python3 - <<'PY'
import os, pathlib, subprocess, tempfile
level = pathlib.Path('tla/execution_models/submission/L2_economic_facts/02_models')
runner = pathlib.Path.home() / '.codex/skills/vibe-tla/scripts/run_tlc.sh'
for name in ['primary', 'long', 'boundary', 'scaling']:
    cfg = 'L2_EconomicFacts.cfg' if name == 'primary' else f'L2_EconomicFacts_{name}.cfg'
    with tempfile.TemporaryDirectory(prefix=f'tickwire-l2-{name}-') as scratch:
        pathlib.Path(scratch, 'java').mkdir()
        env = dict(os.environ, JAVA_TOOL_OPTIONS=f'-Djava.io.tmpdir={scratch}/java')
        subprocess.run([str(runner), str(level/'L2_EconomicFacts_v2.tla'), str(level/cfg),
                        '--det', '--coverage', '--', '-metadir', f'{scratch}/states'],
                       env=env, check=True)
PY
```

仓库统一复验入口为 `./scripts/check-tla.sh`；它只运行各 `02_models/*.cfg`，历史失败模型放在 `04_evidence/pre_fix`。本级还独立运行了 `cargo test -p tickwire-mock failed_recovery_cannot_leave_the_old_engine_authorized`：1 passed，日志保存在 `04_evidence/v2/rust-recovery-green.log`。

源码漂移检查使用 skill 的 `anchors.sh update/check`，在临时工作区只放入 L1/L2 的模型和输入，再复制生成的 lock；L1 为 348 行，L2 为 444 行，两者均为 0 changed。L1 的固定 00/05 和模型没有修改，重新锚定的原因记在 L1 的 06_notes.md。

最终四配置都穷尽状态队列；结果如下。表中耗时为 TLC 模型检查耗时。

| 配置 | 结果 | Generated | Distinct | 完整深度 | TLC 耗时 |
| --- | --- | ---: | ---: | ---: | --- |
| primary | PASS | 139,711 | 24,036 | 8 | 07s |
| long | PASS | 1,591,624 | 280,458 | 9 | 01min 20s |
| boundary | PASS | 14,397,539 | 1,495,032 | 11 | 06min 54s |
| scaling | PASS | 14,278,433 | 1,485,288 | 11 | 06min 43s |

每个配置的 18 个 Next 动作都生成了后继状态。下面只列 generated 计数；例如 ReceiveUncertain 的 distinct 可以是 0，仍不属于未覆盖动作。完整 distinct:generated 和表达式覆盖保存在 `04_evidence/v2/*.log`。

| 动作 | primary | long | boundary | scaling |
| --- | ---: | ---: | ---: | ---: |
| Admit | 1 | 1 | 2,852 | 2,546 |
| RecordFill | 2,856 | 23,829 | 251,760 | 249,990 |
| RecordFee | 1,728 | 15,717 | 151,716 | 149,724 |
| RecordTerminal | 3,579 | 38,052 | 263,160 | 259,248 |
| RecordDuplicate | 978 | 15,006 | 32,196 | 30,996 |
| RecordInvalid | 14,160 | 169,059 | 879,888 | 873,642 |
| RecordConflict | 972 | 14,856 | 32,124 | 30,948 |
| RecordCapacity | 459 | 198 | 9,504 | 8,784 |
| RecordOverflow | 972 | 12,327 | 123,852 | 122,220 |
| AppendFailure | 24,036 | 280,458 | 1,495,032 | 1,485,288 |
| RestartSuccess | 4,670 | 33,452 | 222,482 | 214,700 |
| RestartFailed | 7,935 | 110,916 | 554,088 | 552,126 |
| ReceiveAck | 12,720 | 144,726 | 1,684,308 | 1,667,436 |
| ReceiveUncertain | 12,720 | 144,726 | 1,684,308 | 1,667,436 |
| RequestCancel | 3,858 | 27,390 | 1,128,600 | 1,119,348 |
| RejectCancel | 24,033 | 280,455 | 2,940,834 | 2,922,000 |
| AcceptNoBytes | 243 | 729 | 333,144 | 332,226 |
| RejectNoBytes | 23,790 | 279,726 | 2,607,690 | 2,589,774 |

最终源码、测试、模型及配置的 SHA-256 位于 `04_evidence/v2/source-and-model.sha256`。历史模型引用的三个源码文件均与提交 `cd8bde270ac257b296100979621c60c64f965e42` 匹配，已逐项核验；记录见 `04_evidence/pre_fix/source-commit.txt`。
