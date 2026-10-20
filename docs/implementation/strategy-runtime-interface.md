# 策略进程与执行回报接口

`tickwire-strategy-sdk` 实现策略进程的行情接收、规范命令发布和账户回报消费。`tickwire-strategy-host` 持有实际子进程句柄，以三个受控 Unix socket 完成行情、意图、回报通道握手；每条连接都核对同一个子进程的 PID、UID、GID 和部署身份。策略进程只接收通道资源与恢复对象目录，不接收执行器的交易凭证。

## 数据通道

- 行情使用 JOIN 快照与广播 ring。策略从共同序号的完整快照恢复状态，再消费连续增量；Arena 引用随实际借用生命周期释放。
- 意图使用每实例独立的 SPSC ring。完整 `CanonicalCommand` 先保存为不可变对象，再发布包含完整 SHA-256 与长度的固定记录。执行器读取原对象，重新校验命令及恢复依赖后进入账户准入。
- 回报使用每实例独立的 512 字节 SPSC ring。`CanonicalAccountBaseline`（消息 780）引用完整账户基线；`CanonicalAccountRecord`（消息 781）引用完整账户日志记录。两者要求协商 `CanonicalAccountReference`（feature bit 8）。

回报的 `ReportBody.aux_id` 保存摘要前 16 字节，`value_0`、`value_1` 的原始小端字节保存其余 16 字节，`aux_version` 保存对象长度。`exec_journal_seq` 是账户 WAL 位置，`report_seq` 是回报 ring 序号；两者各自连续，不能互换。基线的 WAL 位置为 0。对象通过 `FileBlobStore` 同步后才发布 ring 引用，接收器使用只读 `FileBlobReader` 核对完整摘要、长度、账户域、基线摘要与记录顺序。

回报保留 Admit、发送前中止、完整交易所事件、费用结清、归属变化和对账激活等实际日志记录。多资产费用与原始成交身份包含在规范对象内，不受固定回报字段数量限制。策略可以从基线和顺序日志重建账户视图。

## 回报与恢复

执行循环把不可变 `AccountHistory` 交给策略适配器。每个子策略独立保存报告位置，每轮最多推进 32 条记录；满 ring 时保留该位置，继续处理账户事实。新策略进程先收到基线，再从位置 1 重放完整账户日志。策略进程的回报消费顺序和实际调用时间同时进入 `TWINP001` 输入日志，产生的新意图仍须在其完整恢复描述同步后发布。

`ReportWriter` 对重复基线和最近一次记录发布幂等；同一位置内容冲突或跳号会被拒绝。`ReportReceiver` 拒绝第二份基线、跨基线记录和不连续的日志位置。

## 换代与多策略

策略换代使用控制面提供的新身份、资源头与恢复配置。发布者冻结旧 bank，取得实际退出或 quiesce 结果，并保留旧映射；新 bank 的起始序号来自旧 bank 的真实发布水位。旧资源的保留数量有上限，耗尽后等待管理操作，不复用仍可能被读取的存储。

`strategy.mode="multi"` 在一个账户 owner 上公平轮询最多 32 个策略实例，禁止嵌套。同一逻辑策略可同时有一个 Active 和一个更高代次的 Shadow；每个实例拥有独立意图和回报通道。多个实例可以直接 JOIN 同一 MD topic；各自创建本地行情发布者时，topic ID 才须不同。一个成员失效时仅撤销其精确实例和代次的授权，其他成员继续处理。

## 动态加载与交接

配置 `shadow_loader` 后，控制端向私有 inbox 原子发布 `<sequence:020>.json`。`tickwire_app::launch::Request` 绑定执行器身份、writer epoch、逻辑策略、新实例和代次，以及 HostConfig、Bootstrap 的完整 SHA-256。HostConfig 必须是 `shadow=true`。加载器先同步 Decision，再构造真实子进程，最后同步注册结果；注册成功不授予交易权，也不代表 READY。重启只恢复已使用的请求、实例和最高代次，不重新启动历史注册的子进程。

Shadow 与 Active 独立消费同一账户基线及顺序记录。Host 只有在认证 READY、实际报告 cursor、完整检查点依赖和账户 InputSeal 一致时才确认追平。交接使用[生命周期执行端](lifecycle-executor-interface.md)的 Prepare、Commit、Release：Prepare 暂停两实例的意图转发，Commit 同步账户的 `StrategyHandover`，Release 验证另行安装的新实例授权后切换转发。

新实例授权在 Prepare 已应用后安装；提前安装会改变最高策略代次，使 `begin_handover` 拒绝该操作。`install_strategy_grant` 不改变交接闸门版本，安装本身也不打开尚未 Release 的闸门。新授权的有效期不能超过交接期限。

当前 Host 在 Release 完成前终止旧实例并等待持有的 `Child` 实际退出。只有 `Registry::terminate` 返回退出观察后才记录 `strategy_retired`（精确 PID、实例、代次和退出状态），并允许新实例激活。终止失败时返回失败，两者保持暂停，由生命周期执行端转入 Halted；已有订单和可能已发送请求的责任不因此释放。这是本地实例退休的完成条件，不是全域 Finalized 凭据。

协调器重启先发出本轮 QueryApplied，收齐回复且显式配置 `resume_after_queries=true` 后才能恢复操作。执行器重启则恢复为 Halted，旧 Release 不重新授予权限；下一次发布或回滚需要新实例、更高代次及新的生命周期操作。
