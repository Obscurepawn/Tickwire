# 状态和抽象

每个 entry 保存阶段、固定 baseline S、登记时 membership V、请求是否已完成、资源是否已送达、实际 cursor 是否登记、双 READ_READY、激活时水位及最终 Quiesce 水位。全局 membership 只在真实 join/detach 增加；READY 比对每个 entry 原有 V，其他 JOIN 不改变这个绑定。

发布门禁是所有未 detach 成员的最小保护值：pending 使用 S，已激活成员使用实际连续 released。底层缓存只会更保守，L0 在调用发布时使用精确最小值以减少缓存状态；既有 ring L0 检查了缓存刷新规则。本级保留实际槽位序号，所有活借用和未读记录必须对应原始序号。因此覆盖错误会产生具体被覆盖的记录见证。

只允许一个活借用，released/applied 独立推进；这对应合法 `Subscriber<1>`，不是缩减 ring 容量。两个水位都推进后才能继续借用，覆盖边界允许先应用后释放以及先释放后应用。较大借用窗口的缺口扫描由 ring L0 覆盖。

prepare 分为登记和构造结果：`busy` 锁住 owner，已有消费者可独立推进。完成交付归约为单个动作，因为消费者取得两个文件前不能 attach；第二次 send 成功至 deliver 返回间的消费者操作，与返回后的操作交换不影响 owner 状态。交付失败保留 entry 和门禁；没有模拟已经失败的发送仍秘密交付完整文件。

每个 owner 控制调用是同步动作；发布的槽位写与 release 发布在本级归约为原子批次，其引用保护前提来自 ring L0。READY 采样当前 published，在同一 owner 调用内固定；消费者可并行推进 cursor，但只会让当前采样更保守。模型不把身份认证等同于远端遵守内存协议。

主配置从发布 1..4、无读者的实际可达状态开始。wrap 初态前缀详见 PLAN：每个读者 birth V 依次为 2、3，双方 cursor 仍 pending，即使应用/释放到 67 也保持 S=4。配置只继续枚举 68 与 69，容量保持 64。两个读者配置中，只激活一个仍不足以复用 69 的槽位。

本组有限配置为所有读者选择同一个 JOIN 截止时刻，`clockExpired` 是这一共同期限的谓词；尚未枚举独立读者截止时刻，也不覆盖持续接纳新注册。期限动作表示该 JOIN deadline 到期或 owner 观察到时钟倒退；未激活 entry 进入 Expired，门禁保留。Active 的相同 READY 回首次水位，仅表示历史成功；Quiesce 完成后的重复 ACK 必须匹配保存的完整证据。Reader 在 Quiesced/Detached 后仍可推进 applied；模型保留这一动作，历史重试独立使用 closedRel/closedApp，不受当前水位变化限制。`quiesced_seq` 在合格 SDK 的 Quiesced 操作中写成 read，之后停止新借用，因此本级把 qseq==read 与 life5 一起抽象；实际 mapped detach 仍逐字段读取验证该值。认证会话仍有效是本级输入；永久会话关闭和续租由 control L1 检查。

每个有限身份只登记一次，与当前 M 个累计历史条目一致。错误身份/hash/水位和旧 membership 回包保留为拒绝动作；本级不枚举哈希碰撞、恶意 SDK 写者或尚不存在的死亡回收入口。没有公平性假设，也不宣称持续发布下消费者必然追平。
