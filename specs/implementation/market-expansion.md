# 运行时行情目录扩展

Status: implementation-in-progress

目标是在 MD、执行和策略进程保持运行时追加交易标的。固定目录继续使用冷建索引；运行时目录追加保留已有本地编号，冷路径完整准备候选，通知热路径仍使用预绑定槽位。首个接线范围是同一已批准 catalog 内向现有 topic 追加 feed；更换 catalog 则同时校验经济规格、执行授权和全部来源重绑定。

## 受信计划与候选

MD 的扩展计划由已经持有真实子进程身份的 launcher 写入私有控制目录。计划绑定非零 operation ID、当前 revision/规范配置摘要、当前精确 bank 描述、目标 revision 和新 bank 模板、完整目标配置、批准消费者的旧/新 Admission、预热期限、新鲜度及峰值存储预算。读取文件和校验摘要不替代 launcher 授权；运行时只处理启动配置明确批准的私有控制路径。

`tickwire_md::expansion::Plan` 最多 4 MiB，预热期限距准备时刻最多 60 秒。`prepare` 核对全部旧 feed 的规范字节与实际参与者；新请求、游标、快照和会话 nonce 不得复用旧身份。`Prepared` 只提供不可变访问，`finalize` 在真实 cut 上填写新 bank 的创建时刻、cutover、first_seq 与描述摘要，再把同一精确 bank 字节交给全部新 Admission。模板中的创建时刻/cutover 为零、first_seq 为一；descriptor_hash 在 finalize 才确定。bank 与会话复用原有语义校验，63 位序号保留边界和倒退时钟在准备/封口阶段拒绝；消费者深度容量保持原值并覆盖目标 topic 的上限。当前实现保持旧 bank 几何布局不变，目标 topic 只允许增加缓存/快照预算与集合版本；退休加活动加候选 SHM 同时计入峰值。

目标配置保留全部旧 feed、事件来源、网络配置、产品及价格数量规格；新 feed 只加入目标 topic，owner 和完整本地 BookKey 不冲突。目标集合严格增加，instrument_set_version、ring_epoch、feed_generation 递增；catalog/version/hash、进程身份、channel 和 host boot 保持不变。相同 operation 重试读取原结果，旧 revision、不同内容复用 operation、遗漏旧成员和更改旧规格均拒绝。活动配置仅在候选完成后替换。

`FeedIndex::expanded` 在独立候选中按原编号顺序复制映射，再按目标配置的首次出现顺序追加新 ID；完整旧集合必须保留。失败不改变活动索引。新深度和事件 owner 通过真实 WSS、REST 与同步逻辑预热，旧 owner 在预热期间继续发布。只创建 socket 或收到订阅 ACK 不算预热完成，候选的每个必需类别及深度均须可用且未过期。

## bank 切换

`TopicPublisher::prepare_expansion` 冷建候选，保留旧行的完整身份、来源绑定、状态和预留容量。新增行只通过实际通知更新；断线、缺口和不合法输入撤销就绪。候选预算计入旧/新缓存同时存在的峰值；MD 另外核对活动、候选和退休 SHM 的合计预算。

提交前停止新的旧 bank JOIN，撤销旧消费资格并等待所有批准消费者的真实 QUIESCED。`JoinOwner::all_detached` 只在所有注册条目都已完成真实退出时成立，Failed、Expired、Pending 和连接 EOF 均不能代替证明。新 bank 的 cutover_seq 在提交点取旧 published，first_seq 为下一序号；候选刷新旧行的最新状态，再核验所有状态和深度新鲜度。完整新基线写入未公开的新 bank 成功后，冻结旧 owner 并交换；旧映射保留至合法回收。

预热、配置或容量错误发生在撤销旧资格前，旧链继续运行；撤销之后失败则保持关闭，不能复用旧代次。普通 `replace_bank` 仍要求集合完全一致，目录扩展使用独立入口。

### MD 控制连接门禁

`Server::active_admissions(topic)` 返回当前真实 Active peer 的完整 Admission，任一同 topic 未完成握手/退出时拒绝；尚未收到 HELLO、无法归属的连接也保守拒绝计划确认。`begin_expansion(topic, operation, expected, owner, now)` 原子复核完整参与者集合及实际旧 bank，再暂停该 topic 新 JOIN；操作 ID 非零，每个 QUIESCE 请求 ID 由 operation 和完整订阅身份派生。进入撤销阶段后任何发送、EOF、租约或期限错误都保留暂停并标记操作失败。

QUIESCE 待发整包由 peer 的有界 outbox 保留，poll 只调用一次 try_send；背压/EINTR 不推进，成功后才等待 QUIESCED。只有 `JoinOwner::quiesced` 核验已注册实际 cursor 的退出状态后才移除 peer。`expansion_quiesced(topic, operation, owner)` 核验原 bank 身份、操作未失败及真实 all_detached，并锁存成功。

`resume_expanded_topic(topic, operation, owner)` 还核对 owner 已换为同 topic/channel/producer 的真实后继 bank，resource 不同、predecessor_ring_epoch 指向旧 ring、ring/feed 代次递增；仅完成旧 quiescence 不能开放旧 bank。暂停期间匹配的 HELLO/Request/Waiting 被拒绝，尚未创建 owner gate 的拒绝不触发其他健康 topic 重建。相同操作和相同参与者重试保持原状态，不重复生成撤销请求；不同内容复用操作拒绝。

恢复 JOIN 时后继还须 `needs_bank == false` 且完整状态为 live；提交后故障或尚未预热的后继不能记录为恢复成功。重复恢复也检查当前可用性：后继已故障则返回错误并保持操作关闭，已记录的后继身份只保留历史完成事实，不清除当前故障门禁。

控制服务最多保留 128 项扩容历史，每项最多 128 个参与者；历史只存完整规范 Admission 的 SHA-256 摘要及 bank 身份，避免按操作重复持有全部配置。活动 Admission 仍按原完整对象返回给计划校验。

HELLO 只为计划识别连接归属，尚未为 owner 注册游标；该阶段及等待 JOIN 请求阶段断连不触发行情重建。进入实际 prepare 后的失败继续保留既有关闭规则，owner gate 仍受追踪。

## 消费者与可重放基线

消费者从计划取得精确批准的新 ConnectConfig，并使用有界 `PendingJoin` 驱动认证、快照、增量追平和激活。每次 poll 限制协议步骤或记录数，不等待 socket 可读写；快照映射、校验和副本构造仍是按容量上限约束的冷步骤。旧阻塞 `Client::connect` 兼容入口驱动同一状态机，执行轮询不得调用该入口。

策略切换只适用于已注册完整输入重放的 owner。`MarketTransition` 输入绑定 operation/approval 摘要、精确旧消费游标、新流身份、真实 JOIN 基线、消费时刻及实际追平记录；整体遵守既有 16 MiB 类型化输入预算。新基线和追平作为一次结构转换，不执行用户回调、不产生命令。运行时先核对重建副本与新 Client 的实际副本和游标一致，持久保存输入及 HEAD 后才替换旧 Client，并允许后续回调。

来源重放、schema3 continuation 和安装沿同一市场流谱系处理转换；旧格式数据保持可读。重放恢复到相同盘口及策略状态，只证明确定性状态重建，运行资格仍由当前 JOIN 和执行授权决定。执行进程也需自己的真实重新 JOIN，新增行情不会创建账户资产、资金或交易权限。

## 验收

- 同一 MD、执行与策略 PID，从一个标的追加高位 `u64` 标的，经过真实 TLS/WSS、REST、SHM 与认证 JOIN；旧本地编号稳定，新编号追加。
- 新快照包含完整旧/新成员，增量从其快照切点后连续消费；真实持久输入可重放得到同一盘口和策略状态。
- 预热超时、失效事件、目录冲突、容量不足或伪造 admission 不产生成功切换；QUIESCED 超时保持关闭。
- 重复操作幂等，旧 revision/旧 admission 拒绝；操作成功要求运行时与同 revision/hash 的 effective 收据均完成。
- 运行时全链仍在实现中，当前证据覆盖候选、控制门禁、真实重新 JOIN 和持久重放接口；持续进程装配另行验收。
