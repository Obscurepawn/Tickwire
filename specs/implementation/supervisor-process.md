# 受管进程的执行镜像、身份与资源约束

监督器启动的程序必须来自已经核对内容摘要的固定镜像。原始路径只在准备阶段打开一次；它随后被改名、删除、覆盖或指向另一个文件时，实际执行字节仍取自准备时的封印内容。该能力用于实际运行实例采集，完整全局配置应用仍需各运行时 owner 接入。

Linux 的 `ExecutableImage` 从单个以 `O_NOFOLLOW | O_NONBLOCK` 打开的普通文件描述符有界复制 ELF，复制过程检查长度上限。复制完成后，memfd 加入 WRITE、GROW、SHRINK、SEAL 封印，按系统支持显式申请可执行属性；系统拒绝可执行内存文件时返回错误。监督器对封印后镜像计算 SHA-256，与批准的 `Identity.build_hash` 相同才启动。启动使用固定描述符路径及标准 `Command`；配置回调提供参数、环境、工作目录、UID/GID 与标准流；回调后再次核对执行程序为原固定描述符路径，改变则在 spawn 前拒绝。镜像由 Registry 条目持有，描述符使用 CLOEXEC，避免向子进程泄漏可复用句柄。每个条目保留一份镜像，额外内存按实际长度计入后续托管资源核验；目前没有跨实例镜像缓存，条目随 Registry 释放。仅在内核以 EINVAL 表示不支持新标志时，才使用旧内核 memfd 标志或省略可选 EXEC 模式封印；内容的四种不可变封印始终强制，EACCES/EPERM 等策略拒绝不会回退。非 Linux 平台明确返回 Unsupported。

`Registry::accept` 在握手前后检查仍持有的活跃 Child，以及 `/proc/<pid>/exe` 实际对象的设备、inode、长度和封印与固定镜像一致。实际程序再次 exec 到不同镜像时拒绝认证。`process()` 维持原有轻量退出检查，不在正常轮询中新增文件读取。

`Registry::observe` 是显式冷路径。它读取真实内核主机 boot ID、PID 的 starttime 和当前 UID/GID，在读取前后检查同一个 owned Child、固定镜像和内核启动身份。UID/GID 的 real、effective、saved 和 filesystem 四个值均须与批准启动身份一致；缺字段、畸形值、超限、权限不足、退出或镜像变化均拒绝。`/proc/<pid>/stat` 的 comm 可包含空格、括号与换行，解析按右括号定位后读取规定的 starttime 字段。所有文件读取有固定上限。

输出 `LiveChildObservation` 字段私有，仅由成功采集构造：批准的 instance/generation/role 等 Identity、实际 PID/UID/GID、实际 host boot、内核 start ticks、固定镜像大小与摘要、最后仍标记连接的控制 epoch，以及本地单调采样起始时刻（供保守的新鲜度检查）。批准的逻辑身份与独立读取的内核事实并列保留。控制 epoch 表示既有连接状态，不证明当次 challenge 已应答；全局 revision、projection、Applied、槽位与 locator 身份仍待真实运行时应用后产生。

SDK 的当前程序核验直接打开 `/proc/self/exe`，按既有预算读取实际镜像并与批准的 Binary 内容对象比较。恢复清单中的 Binary 仍保留独立、完整、内容固定的依赖；启动入口路径的存在与归档依赖的存在分别检查。缺少真实恢复对象时沿原规则拒绝，不用当前镜像悄悄替代历史依赖。

动态 ELF 的解释器与系统共享库由受信宿主负责加载和资格核验。使用 `$ORIGIN` 私有库的程序还需验证其在描述符执行路径下的加载行为。本层镜像摘要的覆盖范围限于主 ELF，入口拒绝解释器脚本。退出观察覆盖直接子进程，后代共享内存的释放另需实际回收证据。

验证使用真实 Linux 子进程：原路径替换/删除、固定后源文件改写、错误构建摘要、脚本和超限文件、退出及再次 exec、实际身份读取、恶意 comm、现有认证握手与独立策略重放/回退 probe。生产规则保持固定；最终全仓检查和真实策略进程场景分别记录源码清单与可执行文件摘要。新的形式化验证按项目约定留到功能开发结束。

内核接口依据：[memfd 可执行策略](https://docs.kernel.org/userspace-api/mfd_noexec.html)、[proc_pid_exe](https://man7.org/linux/man-pages/man5/proc_pid_exe.5.html)、[proc_pid_stat](https://man7.org/linux/man-pages/man5/proc_pid_stat.5.html)。

## 托管配置中的资源计划

App 的 `HostConfig.resources` 可显式给出 worker、source_replay、rollback 三个用途的完整资源组。每组固定绝对 cgroup 路径、CPU 集合、NUMA 节点和内存上限；三个路径互异。整份计划先做纯值校验，每次启动再对所用组取得 lease、检查真实内核配置和空成员。未设置时保留原启动模式，设置后所有相关启动、轮换、采集、安装与独立探针都沿对应限制启动，任何资源错误都终止本次启动。声明重建沿同一 PID 保留 worker 组。资源组由部署方预先配置，启动器只核验和使用这些组。

该入口落实已批准启动配置中的资源约束，完整全局投影的 OS 身份映射、角色配置消费者与应用回执仍待接入。

## 预配置进程资源

`ResourceDomain::acquire` 接受已经由部署 owner 配好的 Linux cgroup v2 专用 leaf 与 `ResourceLimits { cpus, numa_node, memory_max }`。CPU 为升序、不重复、非空的 OS CPU ID，最多 4096，ID 不超过 4095；NUMA 节点 0..31；内存上限为 1 MiB..2^53-1 的有限字节数。原语不解释全局授权、配置修订或资源签名，不修改 controller 设置，不创建或删除 cgroup。

按绝对路径逐级 `openat(O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC)` 固定目录，拒绝符号链接、`.`/`..` 和非 cgroup2 文件系统。在实际目录 inode 上取得非阻塞独占 `flock` 后核对：`cgroup.type=domain`，没有子组或存活进程，`cpuset.cpus.effective` 精确等于批准 CPU 集合，`cpuset.mems.effective` 精确为单个批准节点，`memory.max` 精确为有限批准上限，`memory.swap.max=0`。所有读取和列表均有固定上限，同名普通文件会被文件系统类型检查拒绝。锁是协作式排他，部署 owner 必须阻止未持 lease 的外部管理者修改或迁入这个专用组；每次启动和观察仍重新核对内核设置。别名路径取得同一 inode 时也受同一 lease 排他约束。

`ResourceDomain` 不可 Clone，一份 lease 只允许一次启动尝试。取得目录锁后立即由私有 guard 持有，后续构造失败或最终 owner 销毁都会显式解锁；并发启动的无关子进程临时继承的 FD 不延长原 owner 的 lease，其后关闭也不能解除替代 owner 的锁。父进程先打开该组的 `cgroup.procs`，仍从已封印、已核摘要的 `ExecutableImage` 执行。私有 `pre_exec` 在目标 ELF 执行前写入 `0`，将当前子进程移入目标组，再设置并读回完整 CPU affinity；内核静默裁剪、设置变化、权限拒绝或任何系统调用失败均阻止目标执行。`pre_exec` 只使用预先准备的描述符、固定数组和系统调用，不分配、不加锁、不调用用户代码。Rust 的 UID/GID 设置早于此 hook；cgroup 迁移仍受内核实际权限检查，不声称更换 UID 一定可行。Linux 的迁移权限使用打开 `cgroup.procs` 时的 file credentials；它同时要求来源与目标的共同祖先写权限，已打开目标文件不等于迁移必然成功。

启动后和显式冷观察核对实际 `cgroup.procs`、`cgroup.threads` 与 `/proc/<pid>/task`，要求只有该直接启动进程，线程列表两次一致，全部线程 affinity 和允许内存节点满足原限制，并在观察前后复核组设置。结果字段私有，包含 PID、组设备/inode、CPU/NUMA/内存限制、线程 ID 和单调采样起点。持有真实 Child 的上层继续核对 PID 生命周期、镜像与 starttime；采样不是对未来设置不变的保证。组内后代或不相关进程使观察失败，不被自动终止。Drop 仅关闭描述符和释放 lease，不删除组、不清空组、不写 `cgroup.kill`；上层应持有 lease 到受管 Child 退出。启动后的验证失败会终止并回收本次直接创建的 Child；此时目标 ELF 可能已执行，因此 `spawn` 返回错误并不一概意味着从未执行，只有执行前 hook 失败提供该保证。

`memory.max` 约束该组的 charged memory，包括该组产生的内核、文件及匿名内存收费。RSS 与 `RLIMIT_AS` 的统计口径各异，接口只接受实际 controller 限制。父 owner 创建的共享内存与封印 ELF 仍属于父 owner 的保留预算；进入组不会迁移已存在的内存收费。NUMA 限制来自实际单节点 cpuset，额外页迁移和锁页另行处理。CPU 集合限定运行范围，物理核独占或 isolated partition 需单独配置；内核 memory.max 允许短暂超限和分配失败，因此验收读取设置值，不断言瞬时 memory.current 小于等于上限。

本机缺少可写委派 cgroup，验证分为普通测试中的严格解析、真实 cgroupfs/路径/lease 拒绝和执行前失败，以及显式 ignored 的真实委派组进程测试。后者必须提供预配置专用组后实际执行，跳过不算资源启动验收。非 Linux 返回 Unsupported。 共享实际 cgroup 根 inode 的拒绝与 lease 排他断言放在同一个测试内：先无本测试 lease 地核对 InvalidResources，或核对所需 controller 文件实际缺失的 ENOENT，再用生产 DirectoryLease guard 核对独立打开互斥与析构后立即重获；不得让并行夹具的锁冲突充当根目录配置无效的证据，其余测试保持并行。

接口依据：[cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html)、[Linux cgroup 迁移权限实现](https://github.com/torvalds/linux/blob/v6.18/kernel/cgroup/cgroup.c)、[Rust Unix Command 启动顺序](https://github.com/rust-lang/rust/blob/master/library/std/src/sys/process/unix/unix.rs)、[CPU affinity](https://man7.org/linux/man-pages/man2/sched_setaffinity.2.html)。

## 监督器资源启动与观察

`Registry::spawn_with_resources(spec, ResourcePlan { cgroup, limits })` 是显式的受信资源启动入口；`spawn(spec)` 保持原启动语义，不隐式赋予资源资格。两者共用原参数、实例唯一性、容量和封印 ELF 摘要检查。资源入口在执行目标前取得专用组 lease，并只经 `ResourceDomain::spawn` 执行；任何验证或启动失败不登记成功条目，也不回退到普通 spawn。调用方负责计划授权与预配置目录，本层不接受配置字节有效性作为授权。

成功条目同时持有真实 Child、封印镜像和不可克隆的 ResourceDomain。`Registry::observe_resources(instance)` 返回字段私有的 `LiveResourceObservation`，包含真实进程观察与资源观察：以两次 `Registry::observe` 包围域内核读取，并要求进程身份、主机 boot、start ticks、镜像与 PID 相同。域内检查完整线程集合和实际限制；未知实例、退出、镜像更换、成员变化、设置漂移或读取失败均返回错误。普通 spawn 的实例明确返回 `ResourcesRequired`。这个调用是冷路径，普通 `process()` 轮询保持原有开销。

`terminate` 和 `observe_exits` 只更新真实直接子进程退出状态，Entry 继续持有域句柄。Registry 释放时沿原规则处理直接子进程，再关闭资源句柄；后代清理与资源释放证明另由 owner 负责。下一次 acquire 重新核对真实空组和 lease，再决定是否允许复用。

回归保留普通启动与认证测试，并覆盖普通文件系统伪组在目标 ELF 执行前拒绝、失败不占 Registry 槽位、legacy 观察不产生资源证明。真实委派 cgroup 测试显式 ignored，核对实际多线程进程的全部限制、跨 Registry 复用拒绝、直接子进程退出后 lease 仍被原 Entry 保留；未执行该测试时不得宣称资源启动已验收。以上观察不包含全局配置 Applied、运行时发布或交易授权。

## Host 资源用途与持续托管

Host 的 `ProcessResources` 为严格的启动配置值，包含必填 `worker`、`source_replay`、`rollback` 三组 `GroupResources { cgroup, cpus, numa_node, memory_max }`；未知或重复 JSON 字段拒绝。三组路径必须绝对、互异、有界，根目录、空路径分量、末尾斜杠和 `.`/`..` 都会被拒绝；CPU、NUMA 和内存值遵守上述 ResourceLimits 边界。完整计划在启动任何 worker 前做纯值校验，即使本次尚未使用某个 helper 组。监督器仅对本次使用的用途组实际 acquire、核对内核配置与空成员；未使用组只有纯值校验结果。启动授权由调用方提供。

`Launch`、`RemoteLaunch` 和 `capture::Launch` 的 `resources: Option<ProcessResources>` 缺省为未启用；App 的可选 JSON 字段默认 None。启用后普通及采集 worker 使用 worker 组，旧来源重放使用 source_replay 组，restore-probe、replay-rollback 和 rebuild-rollback 使用 rollback 组。ReadyProof 和 preflight 保留原计划，重放与探针不得另选普通启动入口。独立公开 rebuild probe 同时提供显式资源入口。任一组配置、lease、内核或启动失败即返回错误，不能回退 legacy spawn。历史完成件内容复核本身不启动新进程，也不伪造当前资源观察。

同 PID Capture→安装→Live 持续使用原 Registry Entry 及 worker 资源 lease；提升不重新启动或迁移进程。轮换的下一实例使用它自己批准的完整计划。Host 暴露显式冷 `observe_resources`，结果由 Registry 的真实进程与内核采样产生；正常行情、账户与控制轮询不新增资源文件读取。直接子进程退出不构成组释放证明，后续用途复用必须重新取得 lease 并核对真实空组。

回归覆盖三用途的真实 ELF 启动拒绝、任一未使用组非法时整份计划拒绝、严格 JSON 和边界、未配置时真实 legacy 启动。

## 全局投影中的策略启动字段

App 的 `HostConfig.projection_launch` 可显式启用策略首次启动约束。输入固定完整 ProjectionObject 文件及其 ObjectBinding，另由受信启动配置指定 cluster、namespace、host、process 和 OS 身份名称到 UID/GID 的映射。投影自身不能增加这张映射的权限。文件按 4 MiB 上限读取并经过配置库的完整结构、内容摘要和目标绑定校验；保留原对象的版本及摘要作为输入身份，不把它称为已经提交或已经应用的全局配置。

首版仅接受普通 registered strategy 首次启动：role=strategy、trust_profile=qualified_sdk、restart_policy=never，Active/Shadow 必须与实际启动模式一致，standby 拒绝。capture、预恢复任务、轮换目录及轮换入口不接受此配置。缺省不启用，原配置序列化和运行行为保持兼容。使用者应通过新的启动配置明确开启；不从已有 READY 或历史文件推断已开启。

在创建子进程及启动资源之前，核对投影的 process/host/namespace、strategy process/logical ID、批准角色、制品摘要，以及 Bootstrap 全部本地通道身份。制品摘要必须等于实际启动身份的 build_hash，最终仍由 Registry 核对封印 ELF。OS 映射最多 128 项，名称唯一且有界，未知名称拒绝；解析出的 UID/GID 必须同时等于 HostConfig 和当前 App 的实际内核身份。首版不扩展跨 UID 共享内存的权限。当前进程身份由 supervisor 的安全冷接口读取 /proc，核对 UID/GID 的四种值一致，非 Linux 明确拒绝。

worker 资源组必填，CPU 集合、NUMA 和内存上限必须与投影逐项一致；CPU 按集合比较，重复项拒绝。cgroup 路径及 source_replay/rollback 用途组仍来自单独批准的 ProcessResources，不由投影自行选择。所有用途先做原有纯值检查，实际 worker 沿现有资源启动路径执行。任何映射、内容、资源或内核错误都终止本次启动，不转入无约束模式。

实际启动和认证完成后，App 用同一 owned Child 的 LiveResourceObservation 核对进程、镜像、主机 boot、连接与资源，才保留字段私有的 LaunchBindingProof。它描述这一时刻的启动字段绑定，包括输入对象身份、实际 PID/start ticks/host boot/UID/GID/image 及实际组和线程限制；它不证明策略参数、风险、IPC、日志、local_config、timeouts、observability 已消费，不写 RuntimeObserved/effective，也不授予交易权限。后续查询必须重新观察当前实例；保留的首次启动证明不可作为持续新鲜的运行事实。首次证明失败时返回启动错误，由原 owner 清理直接子进程。

回归覆盖完整合法投影的启动字段解析、每类绑定冲突、未知/重复 OS 名称、非法当前身份、缺失资源、受限模式拒绝和旧配置字节兼容。真实 Linux 启动覆盖资源拒绝时不执行目标和不回退；完整成功路径的资源证明仍要求实际委派 cgroup，未执行时单独标明，不能把纯值通过当作内核约束通过。本阶段不更新历史 TLA/TLC 证据。

本地 `HostedStrategy` 保留 lazy 启动：`prepare_start(now)` 顺序执行原错误门禁、维护和行情已 Live 后的启动，错误按原规则锁存，返回当前是否已经启动；它不读取或交付 intent。返回 false 时调用方通过 `failed()` 区分等待与失败。冷 `observe_resources` 只观察已启动的真实 Child，尚未启动返回 NotReady。App 在首次 intent 交付前先完成启动与资源证明；证明失败由 owner 终止该 Child 并保持失败状态，成功后才继续原策略轮询。
