# 策略合并输入与 schema3 交接

接口约束固定旧 binary/旧配置从 S 重放至实际 C，随后迁移、应用新配置并安装。实现使用 TWINP002 完整输入顺序、记录时间、timer/RNG 和输出摘要；历史回调没有 IntentWriter。SDK 真实回调结果、输入对象和完成检查点先同步，随后才允许实时命令发布。

`tickwire-release::replay` 提供共享对象校验，SDK 负责实际回调和持久文件，Host 负责固定 executable、真实子进程、认证控制消息和游标；app 重新核对完整账户前缀。来源 S、原 signed Prepare 和完整 Operation/member 贯穿所有阶段。

`CommittedHistory` 核验持久对象内容、输入顺序及切点连接，兼容 schema1 完整前缀接 schema2 增量段。泛型业务状态是否与保存端点一致，仍由旧工厂实际回放并比较完整检查点确定；历史校验结果不授予恢复运行或输出权限。

schema3 完成闭包中的恢复依赖（包括旧策略二进制）沿恢复对象预算校验：单对象最多 64 MiB，完整闭包最多 256 MiB，仍逐个核对长度和摘要。启动、安装、回滚及其嵌套读取使用相同预算；不能用 replay 消息的 16 MiB 上限误拒绝合法二进制依赖。检查点、输入尾部、控制请求及其他类型化 replay 消息继续保留各自的 16 MiB 上限，闭包对象数和递归深度限制不变。

schema3 启动只完成 S 的 restore/migrate，保存 `replay_restore_digest`。Prepare 关闭旧转发后取得 C，启动旧 binary 的 `--source-replay`；目标真实 41/42 安装后保存 `replay_installation_digest`。唯一固定 rollback probe 在 C 的 ConfigApply 输出上执行；Host 观察其真实成功退出后，目标同步 `replay_rollback_digest` 并返回新的 READY。三个根保留在 HandoverSeal v3。schema2 保持原流程。

执行侧依次同步对象与生命周期 State。安装同步返回不确定时，内存不把该阶段当作完成；恢复可以发现完整尾部，但只保留事实并关闭许可。完成回执的精确同会话重试返回原签名；新的消费刷新不再次调用旧回放、迁移、安装或 probe。Commit 必须带协调器采纳的精确 ReplayAdoption，并重新核对实际 READY、完整账户记录、当前订单能力及 gate。

schema3 早期跨进程验收曾因输入累计重写而积压；每次 flush 现以上一持久检查点为锚点写入新增输入，保留固定 S→C 的完整历史。2026-10-28 原生 Linux release 的自定义 V1→V2 场景通过：S=2、C=10，旧 binary 回放、目标安装、独立回滚、Commit/Release、旧 PID 退出及迟到成交均完成。另将两份策略 ELF 副本补齐至 16 MiB＋1 字节后重新生成身份并运行，S=2、C=22 的完整链路同样通过；两轮镜像和目标消费水位均为 12，终态费用闭合。期限与门禁没有放宽，[运行摘要与源码/二进制身份](../evidence/20261028/summary.json)保留实际范围。

`registered::run` 提供静态用户工厂的实际启动、配置解码、旧版本回放和回滚探测入口；`FactoryBinding` 将工厂 ID、配置路径、对象摘要/长度及 revision 绑定到发布资料。内建 worker 拒绝未知工厂，不自动切换策略。跨时钟域转换、目标 Shadow 保留段内的 Config/Model 改动保持拒绝。

独立 `tickwire-strategy-example` 提供两个实际可执行文件，分别使用 V1/V2 状态。其计数和动作状态由真实 SDK 回调改变，C 上迁移保留原计数和已发生动作，新 ConfigApply 后继续处理输入而不重发历史命令。当前已完成 4 项单元测试，以及实际 V1 子进程重放持久 S→C、核对 PID/退出与完整检查点的测试；使用该工厂的完整跨进程交接仍待实际场景验收。

证据：阶段报告。形式化验证按系统统一安排进行，本阶段未新增或运行 TLA。

恢复新增 `ReplayObserved`：新 Query 的当前签名状态携带已保存安装结果。协调器保存历史事实后，只能在全域查询完成和显式 resume 后重新 Prepare，刷新 READY 后再 Commit。旧结果过期不能作为新准入；当前成员期限、gate 或执行会话失效仍拒绝。CLI 和执行侧生命周期 WAL 已接同一显式变体。

## 安装对象与 READY 重复验证修复

第四轮真实 Linux 运行完成旧策略回放后，安装控制对象被 Host 写成 `.bin`，worker 的报告对象读取器按 `object-<摘要>` 查找而失败。Host 现经 `ReportWriter::put_object` 复用既有独占对象存储；单元覆盖实际 writer/reader 一致、重复同步及失败不返回摘要。证据见 对象存储修复。

同轮保留的 34 对象闭包用于工作量回归：相同认证 READY 的三次访问，完整加载从 3 次降到 1 次。Host 仅缓存一份已验证字节，绑定完整 body/目录/进程/会话；身份或期限失效撤销缓存，使用时继续核账户摘要和实际游标。真实文件变化、body/context替换及容量拒绝均有定向用例，见 READY 缓存证据。该阶段尚未代替下一轮 Linux 完整交接结果。

第五轮已读到标准命名的安装对象，但安装控制发送时达到原 30 秒协调器期限。捕获循环中两个不同 body（中间 READY4 和最终 READY5）此前各加载一次完整闭包。实际保留对象的定向测试先得到 2 次加载的 Red，修复后为 1 次；仅最终挑战回执经过完整对象验证后才能成为全局 READY。中间帧保留身份、会话与四类序号单调检查，失败不保留全局水位。证据见 封存中间通知验证；完整 Linux 交接仍未通过。

## 安装后的增量提交链修复

第六轮真实目标安装已同步完成，但旧安装验证器只读取最后一个 schema 2 提交，误把其局部起点与旧策略 C 比较并拒绝。现将有界完整提交链校验下沉到 release，SDK 重开 HEAD、安装验证与 RecoveryWriter 共同调用；保留 schema 1 升级边界、C 后实际输入及完整市场/账户视图比较。实际原对象 Red→Green、恢复写入器入口及负例见验证记录。
