# 运行时控制门禁

`tickwire-runtime-policy::ControlPlane` 保存 supervisor 授权、执行所有权租约、行情发布水位和私有输入对账资格。克隆句柄共享同一个单线程状态；`PolicyGate<SystemClock>` 每次读取实际单调时钟并实现 `runtime::authority::GateProvider`。初始状态为 Stopped，没有默认 owner、策略 grant 或对账资格。

`install_owner_lease` 核对执行实例、owner 代次、writer 代次、时间域和递增 lease 代次。`isolation_record` 指向 supervisor 已取得的隔离记录，本模块不执行进程隔离。策略 grant 精确绑定实例、策略代次、授权代次、配置版本、允许动作和全部必要行情流。撤销保留最高授权代次，旧 grant 无法重新安装。`replace_writer(revision,new_lease)` 原子更换 writer 与租约，同时停止交易、撤销旧策略和管理授权、清除对账资格。

每条 `FeedUpdate` 来自真实 ring 发布，携带独立的 feed generation、ring epoch、sequence 和发布时间。门禁按命令 TriggerRef 精确查找保留的发布记录，核对目录版本、代次、序号距离及实际年龄；交易所 update ID 不充当 ring sequence。缺记录、缺依赖、断档或旧代次均拒绝。行情有效截止也进入返回 Gate，供首次接受字节前最后采样重查。

NodeObserver 接收真实 GapToken，source gap generation 不由连接阶段或本地冻结次数生成。Receiving 只表示连接可接收；撤销、断流、输入冲突或账户冻结会清除对账资格。`input_applied` 接收输入 WAL 的连续已处理物理前缀，其中包含登记、原始响应和子结果，因此允许批次跨多个序号推进。对账请求绑定该前缀、源连接与缺口、账户代次、writer、持久基线、账户历史、查询覆盖摘要和当前时间域截止。完成核验前后都重查当前状态。实际适配由 `RestOwner::reactivate` 返回的私有构造 `ActivationProof` 实现；`ReconciliationRequest::from(&proof)` 使用激活同步后的账户序号和历史摘要，核验逐字段比对该能力。

显式 ManagementGrant 只允许命名策略的撤单和查询，可以在策略撤销、行情陈旧或私有流缺口期间使用，仍受 owner、配置、请求期限和动作检查。新单和改单还要求 Trading、有效行情、当前账户与目录版本，以及已完成的私有对账。

`bootstrap::Config` 由 JSON 显式构造配置、OwnerLease、StrategyGrant 和 ManagementGrant；`build()` 返回共享 ControlPlane，`native_provider(&plane)` 建立同一 ClockDomain 的实际 SystemClock。配置使用绝对单调截止，重新读取配置不会延长租约或生成对账资格。
