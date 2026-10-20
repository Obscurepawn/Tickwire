# 多进程运行失败记录

这些日志来自同一个 Linux aarch64 容器内的六次独立部署目录，每轮都重新生成了配置、密钥和日志。失败后，启动器终止并回收自己创建的子进程。

| 运行 | 实际失败 | 已定位的原因 |
| --- | --- | --- |
| [run1](run1/exec.stderr.log) | `Bounds` | 复制待发送批次预算配置为 64 MiB，构造器上限为 8 MiB。 |
| [run2](run2/exec.stderr.log) | `strategy startup: Ring(Layout)` | 通道配置缺少必需特性位；SHM 支持掩码同时遗漏 Report 通道的 bit 8。 |
| [run3](run3/md.stderr.log) | MD 查准入文件时返回 `ENOENT` | 服务端使用首次 HELLO 的零 `control_epoch` 查文件，文件却按授权会话代次命名。 |
| [run4](run4/md.stderr.log) | `Ring(Protocol(Cursor))` | 消费者加载快照后未追平持续发布的增量，直接发送 JOIN_READY。 |
| [run5](run5/exec.stderr.log) | 对账完成后 worker 返回 `Identity` | 策略用较新的采样时刻创建命令，发布端却使用循环开始时的旧时刻，触发“发布早于创建”的拒绝条件。 |
| [run6](run6/exec.stderr.log) | `AwaitingCommit` 后执行进程返回 `Strategy` | 发送门禁推进控制面的时间水位后，运行时仍向健康检查提交发送前的旧采样，触发时钟回退拒绝。 |

运行目录分别为 `/lab/run1` 至 `/lab/run6`。这里只保存执行、行情和镜像进程的错误输出；这些失败记录不代表修复后的验证结果。
