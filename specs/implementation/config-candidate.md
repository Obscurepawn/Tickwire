# 配置候选与静态校验

`tickwire-config` 将 TOML 和 JSON 转换到同一受限值树，产生按 RFC 8785 键序编码的规范字节与 SHA-256。入口接受至多 16 MiB、至多 64 层容器的部署上限；默认深度 32，值节点总量上限 262,144。策略参数另限 16 层。

JSON 在构造对象时检查重复键，包含转义后相同的键。所有浮点语法、越过 ±(2^53−1) 的整数、TOML 日期时间及无效编码均拒绝。整数 `-0` 与 `0` 产生相同内容身份；`-0.0` 和 `-0e0` 属浮点语法，仍拒绝。数组顺序保留，对象键按 UTF-16 单元排序；字符串与其转义后的内容保持一致。

`validate_structural()` 使用编译时固定的 `system-config.schema.json`，执行 Draft 2020-12 与格式校验。依赖关闭网络和文件引用提取，固定 schema 只使用本地引用。参考 TOML 通过，未知字段和带未解决内容的生产模板拒绝。`normalized_configuration()` 将当前唯一缺省字段 `RoleBinding.cluster_scope=false` 显式补齐后重新检查预算并计算摘要；通用 `parse()` 保持原输入值。

`validate_static()` 在结构通过后检查唯一 ID、引用、进程角色、策略实例、逐账户预算币种和上限、CPU 分配冲突、内存、主备和三个 voter 的故障域、ring/arena 几何、共享内存预算与期限顺序。Schema 限制 CPU/NUMA 编号范围；物理核和 NUMA 数量不作为操作系统编号的连续上界，实际存在性由主机清单核对。所有字节预算使用 checked arithmetic。精确十进制预算用数字字符串比较，不经浮点数或有损缩放。

返回的 `Resources` 只记录静态分配总量。后续参数 schema 与容量核验使用下述独立入口；设备、账户模式、秘密引用授权、元数据与资格证据仍需部署验证。该入口没有创建交易或配置发布权限的 API。

`ArtifactCatalog` 验证完整 artifact 引用语法和字节 SHA-256，最多保留 4096 项、单项 16 MiB、总量 512 MiB，同内容重试不重复计费。依赖扫描按完整候选收集引用和 JSON Pointer 使用位置，凭据引用单独列出。`validate_parameters()` 使用实际内容固定的参数 schema 验证每个策略，结果绑定候选与 schema 哈希。它限制 schema 为 1 MiB、32 层、Draft 2020-12，只接受文档内引用并关闭外部获取；正则使用线性引擎，编译和 DFA 缓存各限 1 MiB。此项校验不改变参数的 Quiesce 分类。

`PinnedPolicy::decode` 只接受严格 JSON 政策，并校验完整文件 SHA-256。每个进程的账户、venue、topic 与凭据用途都有显式集合。`project` 同时核对候选内政策引用/revision、实际进程分配和代码固定的角色用途上限；策略没有凭据用途，exec 与 guardian 使用各自的引用。视图按独立 schema 字段生成，保留必要运行参数，隐藏未授权对象。输出再次通过 `ProjectionPayload` schema，并拒绝任何不在实际用途绑定中的 `secret://` 引用，包括参数值中夹带的引用。完整候选摘要与权限 payload 摘要独立保存。

`ProjectionSet` 要求政策 target 与完整 process 集合相等，逐项生成并校验全部投影。`ProjectionDelivery` 核对原始 base 摘要，保留新旧 process 并集、双侧投影和 Added/Retired/Changed/RevisionOnly 关系；不接受请求裁剪目标。投影相同且政策 revision 相同的目标仍保留为 RevisionOnly。目标数和新旧规范 payload 总量共同受 4096 项、512 MiB 硬上限约束；完整计划持有的 payload 预算与运行时 SHM 并存预算分别核验。旧 desired 投影不代表当前 effective。

`CapacityAssessment` 读取候选固定的主机清单，核对完整 host 集合、故障域、CPU/NUMA 映射、package/core 物理核、系统保留内存和 SHM 容量。热循环与 housekeeping 不能占同一物理核；supervisor 使用 housekeeping CPU。额外保留资源登记经过严格解码，计入共享/私有内存及 CPU 冲突，缺失主机和 CPU 拒绝。结果绑定候选哈希、清单文件哈希和登记快照规范哈希，不授予部署权限；登记来源、新鲜度和真实设备验收仍由服务端核验。具体格式见部署资源接口。

`ProjectionObject` 按固定 schema 包装权限 payload，完整交付字节最多 4 MiB。解码先核对实际完整对象 SHA-256，再检查严格 JSON/schema、全局版本/hash、政策版本、目标进程和重新计算的 payload hash；两个版本均在 `1..2^63` 内。`for_delivery` 从完整交付集合取投影，退休目标保留原权限 payload 和政策版本，同时绑定新的全局配置摘要。退休动作由计划 target 指定，目标仍须完成最后一次应用和退出；对象的预期绑定从受信提交取得。具体接口见投影交付对象。

`materialized::ProjectionFile` 按独立 `ProcessProjection` schema 生成 TOML，将权限 payload 与 `_materialization` 分开保存。全局版本、目标、政策和 payload 摘要来自原交付对象；其余元数据逐项核对预期 Stamp。desired 的应用位置为 0，effective 要求非零 boot 和应用位置；operation 与 slot 为非零 UUID。文件上限 8 MiB，最终 TOML 字节摘要保存在外部收据，恢复同时核对该摘要、严格 TOML、完整元数据与原 payload。六项测试覆盖实际 TOML 往返、全部绑定、字节和内容漂移、长度边界、嵌套参数、混合数组及 ±(2^53−1) 精确整数。文件内容对象不包含持久化或运行时应用凭据，具体完成顺序由物化 agent 接口规定。

`ChangeSet::between` 根据完整结构路径计算变化分类和影响账户。预算收紧可热更；保护放宽需静默边界及同一账户范围内的 operator、risk-admin 联合授权。角色的账户范围独立保存，不能用另一账户的 risk-admin 身份满足当前变更。没有账户使用的政策按集群范围授权。策略自由参数一律按 Quiesce 处理，参数名与内置字段同名也不会获得 Hot 分类。所有原有和新增进程均进入交付集合。

新增预算、整个策略的增删及逐合约覆盖变化也要求 risk-admin。按实体 ID 对齐数组时，公共实体的相对顺序单独比较，因此重排与字段修改同时发生仍保留完整影响范围。每个声明为 strategy 的进程对应唯一策略 payload。投影字节数同时受固定 4 MiB 上限和候选 `config_projection_max_bytes` 限制，避免输出超过已经计算的共享内存预算。live 占位检查只解释部署引用、地址、路径和 venue 传输字段；自由参数交给固定参数 schema，凭据引用仍由投影授权检查。

可交易标的范围变化要求账户风险授权；磁盘告警/冻结百分比和请求接收窗口增大也按保护放宽处理。管理角色继承对应作用域的读取权限，但提交仍单独检查 operator。合约覆盖的 Spot/衍生品类别与账户产品一致，再核对交割到期时间。

早期 41 项测试的真实失败与修复保存在候选验证证据。作用域补丁、显式权限范围、候选准备、依赖/schema、完整投影集合、部署容量、交付对象及进程 TOML 文件核验加入后，配置模块累计 90 项全部通过，Clippy 通过；新增记录保存在控制配置验证。开发期间未运行新的 TLA+。

## 纯值计划计算

`plan::ComputedPlan::compute(base_revision, base, prepared, delivery, options)` 生成确定性计划草案。revision 是调用方提供的值；服务端负责将它绑定实际多数安全读取得的配置头。计算核对 base、候选和完整交付集合的身份，固定 base revision/hash/ETag、后继 revision、候选 hash、选项及逐目标计划，规范字节至多 1 MiB，并以独立域 SHA-256 计算摘要。它不读取时钟、进程或资格资料，没有 Ready 或提交授权状态。

`PlanOptions` 固定应用上限、曝光处理策略和请求目标提示；默认上限 RollingRestart、策略 RejectIfExceeded、提示为空。提示中的未知目标拒绝，合法子集不会裁剪新旧 process 并集。整个候选和每个目标的必要类别均不得超过上限；ForbiddenRuntime 拒绝。退休目标保留旧投影，新增目标保留新投影，未改动目标仍参与版本推进。

逐目标类别取其实际依赖变化的最高类别，不取全局最高类别。策略私有字段影响自身；策略账户、topic 和风险预算还影响对应账户的执行/保护消费者；topic 集合变化另影响原有及新增 topic 的发布/消费目标，单个账户风险预算变化不会传播至无关行情发布者。账户、venue、风险政策及 topic 按新旧投影中声明的实体集合传播。进程变化影响自身、同主机 supervisor，以及引用它的账户和 topic 消费者；主机变化影响该主机进程。IPC 通用几何、control、replication、qualification 影响全部目标，因为没有相应字段的进程仍依赖会话、授权和资源协议；journal、local_config、timeouts、observability 依其投影字段传播。实体重排按集合传播。旧新两侧依赖取并集，投影字节相同不能免除这些依赖动作。未能分类的投影变化拒绝。

目标记录 action、必要类别、旧新投影摘要/政策版本、旧新主机、影响账户与变化路径，附带尚待验证的运行身份、当前 effective 和应用能力前提。风险预算变化附带曝光检查，Quiesce/Retire/新增与换代分别附带静默、退休或继任要求；目录和凭据变化另外标记迁移及凭据交接。草案全局保留当前权限、内容资格和当前保留资源头的待验前提。服务端只能在验证这些事实并持久受理后推进；纯值类型不会把未知运行能力当作已支持。
