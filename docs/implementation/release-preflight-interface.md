# 策略发布预检接口方案

本方案把协调器签名的发布承诺与执行端实际加载的文件、实际完成的状态恢复相连。规范对象、工厂恢复、认证回执、独立回滚 probe 和执行端 schema2 接线已实现；真实进程场景正在接入 lab。恢复入口默认拒绝不支持的状态格式。设计依据是[策略生命周期 §5、§6、§10](../design/strategy-lifecycle.md)。

## 固定发布资料与实时 READY

一次 operation 固定三份规范对象。对象采用有版本、拒绝未知字段且重新编码字节一致的 JSON，SHA-256 覆盖完整对象字节。三份对象都绑定 operation ID、revision、logical strategy ID 和完整 members digest；成员按风险域排序且与 Operation 一一对应。执行端读取并验证这些对象，摘要相符只是进入内容检查的前提。

| Operation 字段 | 对象 | 固定内容 |
|---|---|---|
| `artifacts_digest` | `ReleaseManifest` | 每域目标实例、binary/HostConfig/Bootstrap/策略配置/模型及实际依赖文件摘要、长度，目标状态 schema、输入/输出协议、产品/仓位模式与可管理订单类型 |
| `checkpoint_digest` | `MigrationSet` | 每域来源实例、固定来源检查点及其 schema、输入封存边界，目标 schema、恢复方法及批准的恢复契约 |
| `rollback_digest` | `RollbackPlan` | 每域回滚版本文件、可读取的检查点/输入、支持的 schema 与订单类型，以及执行回滚恢复探测所需的明确输入 |

全局 `MigrationSet` 的根摘要固定整个操作的迁移起点。`Readiness.checkpoint_digest` 仍指向新实例在当前账户 cut 上产生的检查点，随真实回报推进而改变。其封存对象新增固定的 `preflight_receipt_digest`，指向 Host 保存的 Completion；Completion 引用上述三根、目标和回滚回执，再由目标回执绑定固定迁移起点；Prepare/Commit 同时验证固定承诺及实时 cut。执行端沿这个引用关系验证来源与当前状态。

`FileRef { role, digest, bytes }` 只命名内容；本地域的受信路径表负责找到文件。每域至多 64 个文件、整个 operation 至多 64 域，清单至多 1 MiB；恢复资料继续受已有单对象及闭包总量上限约束。可执行文件沿用 Registry 的 512 MiB 上限，独立流式校验；其恢复引用仍遵循现有文件依赖契约。文件角色和摘要禁止重复或冲突，实际读取长度超限即拒绝。

## 文件证明、权限与公共接口

协调器公钥授权固定承诺；本机 ShadowLoader 持有启动权限；策略子进程只拥有行情/账户读取、意图写入及自己的恢复资料目录。策略进程无交易所凭据。文件字节来自受信私有部署目录，经 regular/no-symlink、长度与 SHA-256 校验后固定在本次启动使用的不可变目录中；后续用已验证的内容，不重新从可变配置补字段。同权限管理者仍负责禁止修改正在使用的部署目录。

ShadowLoader 的 Request 绑定 HostConfig 与 Bootstrap，Bootstrap 中的 `preflight_job` 指向完整 Operation、三根对象位置和本域文件表。`Role::RecoveryTemplate` 绑定 Bootstrap 引用的恢复模板；模板每个依赖的实际路径和摘要必须由某个已读取的角色覆盖，同一文件可同时作为 HostConfig/策略配置和恢复依赖，无需重复条目。它在创建子进程前校验 HostConfig、Bootstrap、策略配置、模型和依赖文件；Registry 用同一个 binary hash 启动并认证真实 PID/UID/GID/实例/代次。Host 私有 Proof 保存实际完成结果；静态配置的 Shadow 同样校验实际文件，并在 Prepare 前核对旧 Host 保留的来源。来源保留最多 64 份已确认封存，淘汰后重新选取来源。

实现入口分为共享资料、加载器和实际策略恢复三处：

```rust,ignore
// tickwire-release：规范对象与恢复闭包验证，无进程启动权限。
ReleaseBundle::decode(manifest, migration_set, rollback_plan) -> Result<Self>;
ReleaseBundle::verify_operation(&Operation, &Member) -> Result<MemberRelease>;

// tickwire-strategy-sdk::preflight：真实执行并同步输出。
start(&Job, &Bootstrap, actual_bootstrap_bytes) -> Result<(Running, Completed)>;
run_probe(&ProbeJob) -> Result<()>;
// runtime Strategy 只从当前 Host 的私有证明读取，默认 None。
release_preflight(instance, generation, roots) -> Option<Vec<Vec<u8>>>;

// tickwire-strategy-sdk：由被校验 binary 内编译的实现处理。
trait RestoreFactory {
    type Running: Strategy;
    fn capabilities(&self) -> Capabilities;
    fn restore(&self, input: &VerifiedRestoreInput) -> Result<Self::Running>;
    fn migrate(&self, input: &VerifiedRestoreInput) -> Result<Self::Running>;
    fn rebuild(&self, input: &VerifiedRestoreInput) -> Result<Self::Running>;
}
```

三个恢复方法均默认返回 `Unsupported`；`VerifiedRestoreInput` 由 SDK 持有全部已验证字节，仅提供只读访问。`capabilities()` 描述实际编译进程序的 schema/方法和订单处理能力，Bootstrap 的声明只用于匹配。worker 将清单能力与实际工厂能力逐项核对。执行端根据真实目录、账户和持久命令，检查产品、仓位模式、仍需管理的订单及在途动作；不认识的类型拒绝。Commit 前再次按当前账户检查，避免 Shadow 启动后出现的新订单越过兼容性边界。

## 恢复执行与完成证据

固定来源检查点由旧实例实际 checkpoint 回调产生，来源 Host 将认证身份、文件摘要、原始 schema 与已消费输入边界封存为证明；加载器验证该证明和旧实例绑定后才接受为迁移起点。协调器指定路径或检查点头部自报身份不足以替代这份来源证明。

worker 启动时先处于 `Restoring`，意图输出关闭。`VerifiedRestoreInput` 由 SDK 验证器构造，包含固定来源检查点、来源身份/schema、来源完整输入及其顺序边界、目标配置和已验证的账户回调历史。该回调在新 JOIN 之前完成；当前内置工厂不使用新 JOIN 行情重建状态。它不包含可复用的旧授权或旧 FD。

| 方法 | 实际执行 | 拒绝条件 |
|---|---|---|
| Restore | 调用目标 binary 的 `restore`，从受支持的同 schema 状态构造运行对象 | schema 不支持、来源身份不符、缺失输入或恢复函数报错 |
| Migrate | 调用目标 binary 内固定方法 ID 的 `migrate`，解码来源 schema、转换为目标状态，再对结果执行目标 schema 校验 | 只有声明没有注册函数、输出格式不符、转换失败 |
| DeclaredRebuild | 接口要求注册契约 ID 对应的实际 `rebuild` 函数；当前两个内置工厂均拒绝此方法 | 契约未批准、实际输入不足、未完成预热；不会在 Restore 失败后自动切换 |

首轮进程接线限定 ObserveOnly/FixedDemo：Prepare 实际 quiesce 后重新获取旧 Host 当前检查点，完整业务 state 字节与固定来源必须相等；发生变化便拒绝本 operation，使用新来源创建下一 operation。泛型策略在固定起点后的精确尾部重放仍是后续实现缺口，不能用只追账户回报替代完整输入。

精确重放使用来源策略保存的完整合并输入顺序及原始逻辑时间，输出只供核对。来源检查点后的行情、定时器和配置输入有任意缺口，发布即拒绝精确重放；新实例 JOIN 快照只描述当前市场状态。声明重建模式也要执行专用函数，并在批准契约中写清哪些状态可以从当前输入重建。

首个实现只接 `ObserveOnly` 和 `FixedDemo` 的明确 schema。`ObserveOnly` 验证 `TWOBS001` 后构造对象。`FixedDemo` 使用新的独立业务状态 schema 保存单次动作是否已发生，目标 command 从新实例绑定配置读取；迁移旧 `TWFD0001` 时解析并保留已消耗标志，不把旧 command 的实例、授权或 intent 身份带入新实例。当前不提供账户重建；以后实现时还需要从真实 Admit/订单记录判断对应动作是否已发生，并拒绝无法唯一识别的动作键。阈值更新保留动作已消耗状态。

恢复回调在受监督进程中执行，超时或退出会终止本次预检。恢复返回后，SDK 调用目标对象的 `checkpoint()`，验证输出 schema，将实际输出检查点、恢复输入和执行记录同步落盘。只有这些步骤成功，才通过认证控制通道发送 `RestoreApplied`。实际恢复函数成功后，worker 才建立新行情、意图和账户连接。等待 Host 回滚确认时，行情和账户保留在现有有界环中；收到 Completion 并同步完整对象后才启动策略回调。后续输入沿正常有界消费逻辑推进至实时 READY。

`RestoreApplied` 消息携带实例、代次、逻辑策略、回执摘要/长度/序号和发布根；它引用的完整 Receipt 保存三根、成员与真实进程身份、方法、来源、输出和完成 cut。它用新的版本化控制消息传输，不复用 READY 的预留字节。Host 核对真实已登记子进程、`LoadedRelease` 和同步对象后创建私有 `VerifiedPreflight`，保留它与所有后续 READY 的对应关系。

回滚预检也要执行：`RollbackPlan.probe_input` 固定选择本次目标恢复的实际输出检查点，Host 把上述 RestoreApplied 对应字节及同一账户 cut 交给受认证的回滚 probe 子进程。probe 没有意图环和交易权限，由回滚版本实际调用 restore/migrate/rebuild、验证并封存输出后返回探测结果。Host 验证结果和文件并等待真实成功退出，再同步 Completion；它向原 RestoreApplied 返回绑定原请求/控制代次的 STATUS Applied。worker 校验 Completion 和完整来源闭包后才开始后续回调。这证明固定输入在该回滚版本上可恢复；以后真正回滚使用新的 operation/实例/更高代次，并对当时新增订单、状态和最新输入重新验证，不恢复旧授权。

## 执行端接线与失败处理

运行时 Strategy 的 `release_preflight(instance, generation, roots)` 由 Host/Multi 按精确身份委派。返回的对象只从当前 Host 已验证的私有 Proof 读取，执行端再次解码并核对完整 Operation 和成员。执行端 Prepare 先核三根与实际加载/恢复/回滚探测，再关闭旧授权并进入原有 quiesce/capture；预检失败时可保持尚未撤销的旧实例继续运行。进入 Prepare 后发生失败，沿现有 gate 关闭规则处理。

Commit 再核同一证明、目标进程存活、当前账户能力和实时 READY。它持久化发布根、恢复回执摘要及实时封存对象后才写账户 `StrategyHandover`。`HandoverSeal` 增加版本化的 preflight 引用，由本地恢复与复制端共用验证器检查完整闭包。任一校验失败都终止本次提交。

operation schema 升级后，执行端对旧 schema 的生产交接返回明确 `PreflightRequired`；旧日志保持可读，历史版本不获得新运行权限。协调器恢复只复用已签名固定清单，仍按原有新 QueryApplied 与显式 resume 规则继续。exec 重启不恢复 `VerifiedPreflight` 的活进程资格：持久对象可用于审计和重建，新 child 需要重新认证并实际执行恢复。

## 针对性测试

先用最小纯单元暴露以下行为，再实现相应入口；真实子进程场景接在现有 handover lab 后，不新增形式验证任务。

- 三根、members digest 或单个文件的内容/长度不符；未知字段、非规范编码、重复条目及容量越界，均在启动前拒绝。
- 实际 binary、Bootstrap 或配置与签名目标不符；即使 loader 请求合法，也拒绝另一实例的 RestoreApplied/READY。
- 默认 `Unsupported`、未注册迁移 ID、坏 schema、函数失败、输出 schema 错误、同步失败均拒绝产生 `VerifiedPreflight`；测试计数证明真实回调已执行。
- FixedDemo 已消耗状态迁移后保持已消耗；新配置身份不能把旧 intent 复活。缺失精确输入时拒绝 Restore；批准重建仍必须完成实际输入和预热判定。
- 固定迁移根保持不变，READY 随新账户记录刷新，能通过同一恢复证明；拿旧 cut 提交或新订单类型超出能力则拒绝。
- 回滚文件缺失/被替换、回滚函数不支持当前固定状态、probe 退出或未同步，都阻止 Prepare；后续回滚重新核最新状态。
- 真实进程测试验证 callback 完成、输出检查点同步、RestoreApplied、READY、Prepared 的顺序；在各完成点中断，Prepared 仅在全部完成点满足后产生。已有 MayHaveSent 订单仍归原账户记录管理。
