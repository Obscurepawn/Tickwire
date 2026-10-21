# 控制配置基础规格

实现位于 `tickwire-config-service`，本阶段完成复制对象、初始 desired、真实 mTLS、当前角色授权、HTTP 请求解析及 schema/validate 的安全读执行。计划器、常规配置提交、操作状态机、watch 输出和文件物化继续按业务接口实现。

## 复制与恢复

`Replica<S>` 独占实际 `tickwire-control-raft::Node<S>`。一次 drive 先完成 Raft 的持久提交，再按日志顺序执行全部非空业务命令；成功处理后才释放本轮网络输出。结构损坏、集群或初始化身份不符使实例停止，业务拒绝保留确定错误并继续扫描。安全读同时等待实际 Safe ReadIndex 和完整业务扫描水位；空 no-op 的 index 计入水位。

对象 Begin 固定 key、owner、purpose、总长度和 SHA-256，并预留总长度预算。Chunk 以 64 KiB 边界顺序存储，最后一片可短；精确重复不重复消费容量，同位置异内容和缺片拒绝。Seal 校验全长和摘要后发布不可变对象。所有内容经 Raft 日志复制，恢复逐条重建；只有本地文件或哈希的引用无法成为已封口对象。对象最多 4096 个、同时未封口最多 64 个，累计声明内容硬上限 512 MiB。

TWCF0001 命令使用小端整数，1–4 为初始化及对象 Begin/Chunk/Seal，5 为附带引导信任摘要的初始化，6 为签名首次配置。单命令最多 1 MiB；候选/资格对象最多 16 MiB、投影 4 MiB、收据/计划元数据 1 MiB。解码拒绝截断、尾字节和未知 tag。固定 Python/OpenSSL 样本独立验证引导消息、签名和命令字节。

初始配置授权绑定离线公钥、集群、namespace、集群名称、完整候选摘要、对象键和 nonce。初始化先持久绑定信任摘要，恢复时变更或删除信任即失败。多数提交后的 reducer 重新验证签名、规范内容、静态语义、角色数据，以及实际 Raft voter ID 集合，创建 revision 1。精确重试保留第一次的 term/index；其他引导请求不能覆盖 head。该记录只建立 desired，没有目标 runtime 或交易权收据。

签名对象已经显式包含 schema 默认值；缺少 `cluster_scope=false` 的对象拒绝，不在验签后补写。普通候选准备则先展开默认值，再重新计算内容摘要与真实 diff。全量入口先要求全局 Editor，补丁先检查全部 selector 授权，之后才检查完整 base 与 ETag。

配置业务快照 schema 1 使用 `TWCS0001` 根对象，绑定集群、Raft group、实际 cut/index/term、membership 摘要、预算及引导信任。根对象最多 1 MiB；对象表按 key 排序，内容按顺序打包成至多 16 MiB 的块。恢复重新检查每块摘要、已封口对象摘要、未完成上传边界和累计预留，并以原签名、原 voter 集合及原 index/term 重新验证首次配置。

快照 prepare 在隔离状态中完成。只有实际 Node 发布 CURRENT 后产生、并与本机当前代相符的凭据能安装；业务水位超过 cut 后，旧凭据也不能回退状态。`recover_generation` 先恢复业务快照，再应用已提交日志尾部；普通构造器拒绝已压缩日志。具体格式和恢复顺序见快照接口。

## 认证、作用域和输入

TLS 1.3 要求有效 CA 链、固定叶摘要和唯一 SPIFFE URI SAN，CRL 的 thisUpdate/nextUpdate 均按当前时间校验。连接握手、加密字节及请求读取共享总预算和绝对期限；会话恢复与 0-RTT 关闭。证书主体通过私有 `AuthenticatedPrincipal` 进入授权；每次授权重新验证证书链、撤销状态与当前候选角色。

`cluster_scope` 缺省 false。空账户列表不授予全局权限；显式 true 时列表必须为空，且只扩大本行角色范围。策略共享预算和账户引用按实际依赖集合计算权限。ScopedPatch 在读取 selector、ETag 或旧值前检查全部选择器的作用域，按稳定 ID 合并，再对完整候选执行静态校验和最终 diff 授权。

HTTP/1.1 使用单请求连接，头部最多 16 KiB/64 项，正文硬上限 16 MiB。固定 16 个端点拒绝未知 query、路径逃逸、重复头、TE/CL 混用、压缩正文和非法 ETag/幂等键。正文先严格 JSON 解析，再用程序固定的 OpenAPI 与离线 system-schema registry 校验。请求摘要绑定方法、规范路由和规范正文；If-Match 作为独立幂等条件保存。解析结果不产生业务授权或提交结果。

## 验证

原始日志与清单保留新增能力的 Red/Green、真实 TLS 连接、三个 FileStore voter 的多数提交与重启、独立编码样本，以及 Future CRL 的真实失败与修复。16 MiB 对象经过 256 个真实复制分片后，在三个 voter 各自重启和当选后均返回完整同字节内容。作用域补丁与角色语义另有 `tickwire-config` 测试。

## 认证请求执行

`request::PendingRequest` 由实际 TLS 主体和 owner 的 ReadIndex ticket 构造。`poll_request` 在实际已提交头上执行当前角色检查，`schema` 返回固定 schema，`validate` 返回结构/静态语义结果及实际尚未完成的部署项。完整候选需要全局读取作用域；作用域补丁使用正文 ETag 并沿原 selector/依赖授权。收到的原始正文长度受当前已提交预算限制。响应绑定原主体和叶证书，一个请求只完成一次，取消保持底层未完成 ReadIndex 的有界积压。

配置 Replica 同时委托成员提案与 session 进度。实际四节点文件测试覆盖旧对象复制、learner 加入、joint 替换、退出 joint、重开 receipt、4 号当选后的安全读；成员日志不进入对象解码，业务扫描包含成员日志的持久 applied 水位。

`https::Server` 以有界连接线程完成 TLS 和单次 HTTP I/O，由原控制 owner 执行 begin/poll/cancel。每个线程持有实际 socket，握手、正文、等待结果和响应写入共用从接收连接起算的绝对期限。慢正文和未完成握手不阻塞 Raft；容量耗尽停止接收，超时回收连接。显式 shutdown 取消请求并关闭 socket，Drop 关闭 socket 且释放请求 ticket 的私有 lease。新信任在下一次 begin/poll 生效；正常 leader 切换返回 503，owner 的致命错误继续上报。

## 部署资料来源

`evidence::Trust` 将 cluster、namespace、信任 revision 和最多 32 个 issuer 的公钥及可签资料种类固定为摘要。`verify` 校验严格 JSON envelope 的 Ed25519 签名、实际 ArtifactCatalog 内容、预期 kind/reference 以及有效时间窗口；资料有效期最多七天。返回的私有 `VerifiedEvidence` 保存完整原 envelope、资料摘要、信任摘要和此次核验时间，再次检查沿当前 Trust 执行。候选不能选择验证公钥。签名目录最多 4096 项、总量 64 MiB，只保存严格解析后的原 envelope；目录中存在一项不代表其签名有效。

`ReportClosure` 对规范候选检查六份资格资料的来源和完整引用关系。basis 只去除六个资格引用，保留 SLO、权限、参数、进程和其余全部配置。manifest 的完整进程数组及非资格依赖集合与候选相等；主机清单和设备集合覆盖全部主机。协议、掉电、隔离报告绑定同一 basis、manifest 和设备集合，明确通过且原始资料齐备。原始资料按内容摘要排除六个报告，换名称不能绕过独立内容要求。风险批准精确绑定其他五份报告；替换任意一份后，旧批准失效。结果记录完整候选摘要和最早失效时间，供后续内容校验与计划器使用。

来源六项及报告完整性九项测试通过；内容别名问题保留实际失败与修复记录。报告结果与下述账户、凭据和保留资源结果共同进入完整内容核验。

`account_facts::AccountModes` 已核验候选中的全部账户身份和模式快照。`AccountMode` 签名绑定实际内容，再核对 cluster、namespace、账户、venue、环境、产品、完整身份引用、观察到的交易所账户和资金池身份、三种模式以及交易状态。相同 venue/environment 下的同一资金池不能配置为不同风险域。观察时间不晚于签发时间，另受默认五秒、最高六十秒的新鲜度上限约束；有效期取签名期限与观察期限较早者。六项合成资料测试及 all-targets Clippy 通过；该模块没有调用交易所或修改账户模式，实际运行时激活核对继续使用当前只读查询。

`resource_facts::RetainedCapacity` 验证 `RetainedResources` 来源、外层快照与内层登记作用域及独立观察期限，再调用实际 `CapacityAssessment`。结果同时保留登记 revision、规范内容摘要、签名对象摘要、主机清单摘要与候选绑定；容量计算包含额外共享/私有内存和 CPU 责任。当前登记头仍需由控制 owner 在计划、提交及应用时比较。四项组合测试和原容量八项回归通过。

凭据内容核验见[独立规格](config-credential-facts.md)。来源、资格报告、账户、凭据和保留资源共 33 项定向测试通过。

## 资料包、异步核验与 HTTPS

`deployment::ContentValidated::check` 对同一规范候选、实际内容目录与当前信任依次执行全部内容检查，内部生成完整权限投影；接口只接受原始资料与检查预算。私有结果绑定完整候选摘要、检查时间、最早失效时间，以及全部实际使用对象、规范签名、信任和预算的统一摘要；目录顺序不影响该摘要。

`source_bundle::Bundle` 从一个普通文件加载 `TWDSRC01`、严格 JSON 索引及连续原始对象。索引只有内容引用、长度、签名及辅助引用；路径、公钥和时钟由受信服务配置提供。加载检查总量、逐对象摘要、重复键、签名依赖和精确尾部；全部字节成为内存中的不可变目录。默认整包 64 MiB、索引 4 MiB，硬上限分别为 528 MiB 加固定头、16 MiB；仍执行目录的 4096 对象、单对象 16 MiB、累计 512 MiB 限额。

`deployment_owner::Owner` 持有不可分割的包、Trust 和预算，用最多八个有界后台任务执行实际内容检查。任务在计算结束且结果已消费或丢弃后才释放容量；取消仍在运行的任务不会提前腾出槽位。工作线程与 owner 使用本机系统时间，开始、结束及交付时均检查时钟和资料期限。时钟倒退、来源替换、跨 owner 任务或结果、重复消费均拒绝。

HTTPS 的部署核验先取得实际 ReadIndex，校验当前 TLS、角色、请求预算和候选，再开始后台任务。计算完成后发起新的 ReadIndex；第二次多数读完成后重新检查证书、当前角色、base ETag、来源上下文和有效期。计算期间控制 owner 继续处理 Raft。当前成功内容结果列出已完成的八项内容检查，`checks_pending` 精确保留 `retained_registry_head` 与 `runtime_targets`，整体 `valid=false`；控制服务尚未据此生成可提交计划。

实际文件加载、后台线程、来源替换、取消与真实 TLS/三 voter 测试验证这些边界。源码和原始 Red/Green 见本阶段记录。部署资料 owner 属于本机受信服务配置，业务快照不保存其来源包或信任状态。
