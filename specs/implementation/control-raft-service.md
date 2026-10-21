# 控制 Raft 网络服务

阶段 1/2 已实现于 `tickwire-control-peer`：真实非阻塞 TCP、TLS 1.3 双向认证、会话绑定帧和 `PeerService<Replica<FileStore>>`。网络服务通过 `control-raft::driver::PeerDriver` 驱动唯一业务所有者。`ReplicaControl` 只允许类型明确的提案、引导和安全读，不提供第二个 Node 或任意驱动入口。

`Node::drive_bounded` 按 1..128 个完整 Ready 调度；预算用尽返回 `more_ready`，持久失败仍毒化。`pending_applied_bound` 合计实际未应用的持久与 unstable 后缀；二者重叠时保守计两次。Replica 在该上限中加入业务结果的持有空间。服务预留后才调用 drive，核对实际输出数量、字节和两种 applied 水位；结果保留至调用方 `take_applied`。队列不足暂停 Node 输入、tick 和 drive，网络队列不承担业务结果存储。

帧头固定 128 字节，包含版本、长度、cluster/group、from/to、进程 incarnation、会话 challenge 和连续序号。TLS 完成后双方交换 Hello/HelloAck，两份随机 challenge、双方身份和 incarnation 共同派生会话摘要。Raft 帧只接受这一会话的下一序号。protobuf 解码前检查固定字段、唯一性、嵌套 entry 数量和长度；未知字段、旧 ConfChange 与本地 Raft 控制消息拒绝；ConfChangeV2 另按规范成员请求校验；Snapshot 使用独立闭包传输，不从普通 Raft 帧注入。

TLS 固定受信 CA、CRL、正常 server-name 校验、叶摘要及唯一节点 URI；关闭早数据与会话恢复。每次连接调度重新核验证书与 CRL，有界握手最多 2 MiB 加密输入/输出。`replace_trust` 在下一次 I/O 前撤销不再合格的已认证连接。绝对握手、半帧和空闲期限按每轮传入的单调时间检查；部分输入不会重置期限。每个节点对由较小 ID 拨号，仅允许一条有效双向连接。同一身份的重复连接或新 incarnation 重连只关闭新 lane；激活瞬间先检测既有会话再向 Node 交付任何消息。既有连接关闭或超时后才允许重连，不按随机 incarnation 大小覆盖会话。

收发和 TLS 工作受字节、帧、accept、tick、Ready 预算限制。断线丢弃未完成网络帧并向 Raft 报告不可达；完整 Append 的恢复和重试由原 Raft term/index 处理。应用结果不随断线丢弃。实际原生回归覆盖三 FileStore 节点经 mTLS 选举、对象多数提交、reducer 应用、安全读、全体重开、旧 leader 隔离与重连、业务结果背压、网络队列容量、畸形身份和半帧关闭，见执行证据。

阶段 3 已实现本地快照与文件换代。规范 `TWRS0001` manifest 保存 group、index/term、完整成员描述、应用 schema/root 和排序唯一的对象引用；解析检查长度、保留字段、集合关系和累计预算。对象 SHA256/长度逐一核验，支持连续 64 KiB 块写入。`GenerationStore` 在父锁保护下为每代使用独立真实 FileStore；compact 同步并重开新代 WAL 后才原子替换 CURRENT、同步父目录、替换内存 storage，最后产生私有安装凭据。恢复只使用 CURRENT 指定的完整代，缺对象或损坏即失败；业务先恢复 snapshot，再处理 base 之后的实际已应用 history。

`InstalledGeneration` 绑定 owner/node/generation/cut/root。应用安装先与 Node 当前持久代匹配，再核对隔离准备的状态；它不授予调用者跳过应用 schema 验证的能力。CURRENT v2 在同一原子指针中绑定当前和上一完整代；GC 固定保留两代与活读取/WAL/staging 引用。旧 v1 CURRENT 保守保留未知前驱的完整快照。目录达到 16 代时先回收无 lease 的中断 staging 和更旧代，再决定是否有新 staging 容量；清理失败毒化目录。实际文件回归覆盖 7 个对象/manifest 切点与 19 个 WAL/CURRENT 切点、旧 receipt 在重开后失效、缺对象与损坏 CURRENT 不回退，以及压缩后继续提交和只回放 tail。

阶段 4 已连接实际 mTLS 流式快照。Begin/Chunk/End 精确绑定同一 manifest，Chunk 数据最多 64 KiB，按规范对象顺序和连续 offset 写入 staging。完成后 Node 的私有恢复适配器先解析隔离业务候选，再同步并重开 seed WAL、切换 CURRENT，最后核验自身安装凭据并安装业务状态。准备失败保留旧 CURRENT；切换后安装失败毒化且没有肯定响应。所有业务 tail 仍由唯一 Replica 扫描，应用水位覆盖 cut 后才发送 SnapshotApplied。

断线关闭会话并丢弃传输内存状态；Raft 重试从新 staging 完整重传，没有跨连接缓存复用。传输期限 300 秒，单节点接收一个闭包，每 peer 发送一个。每次对象读取最多 16 MiB；单个 Ready 的业务解析/完整闭包重验和文件同步在单所有者调用内完成，网络帧预算不表示这段同步磁盘工作的墙钟上界。

实际三节点网络回归在第三节点离线时保存未完成上传的快照，之后通过 TLS 安装并追赶剩余 Chunk/Seal；安全读和第三节点再重开均得到完整 131,073 字节对象。`current_membership()` 读取持久 ConfState 及其生效 index，快照恢复保留该元数据。阶段 5 已实现显式 JOIN、learner 追赶、joint 替换和 leave-joint，以及独立服务 CLI。本次没有新增 TLA，也不将旧固定成员模型的结果扩展到新网络实现。

保留规则的实际 Red 为上一快照被删除及第16次中断后容量耗尽；修复后的真实文件回归覆盖40次中断、新旧CURRENT编码、上一代无reader仍保留、第三代发布后才删除更旧代，以及清理中途失败保持CURRENT不变。

目录 clone 共用 namespace mutex，串行化容量计数、GC、创建与 lease 登记，并覆盖 CURRENT 发布和恢复选择到 WAL 打开。独立 Node lease 禁止同目录两个活跃所有者；每次构造的新 owner ID 使上一 Node 的 InstalledGeneration 在同一目录 clone 重开后失效。实际 barrier 回归先暂停在目录已建、lease 未登记处，再并发触发 GC；另有旧 receipt 复用的真实失败与压缩前后双 Node 拒绝回归。


成员操作 `TWMC0001` 固定 88 字节，包含操作 ID、期望 membership digest 和 AddLearner/ReplaceVoter/LeaveJoint 参数。操作 ID 先于 membership ETag 查重，精确重试返回原 term/index/完整 ConfState。当前会话的真实 AppendResponse 与 raft-rs 匹配水位都覆盖 committed cut 后，learner 才能被提为 voter。ConfState、操作结果及 applied index 同步后才发消息；失败毒化，恢复根据实际完整持久尾重建。

`TWRS0002` 快照包含完整 membership 历史、原始引导集合、墓碑及各操作稳定结果；解码从原三 voter 重放规范变更并核对每个结果与最终配置。旧 `TWRS0001` 仅接受未发生变更的固定基线，原字节不变。生命周期上限为 16 个节点 ID、64 个成员操作；退休 ID 不复用，容量满时保留历史并拒绝新增。

持久 JOIN 的未入组身份只接受真实认证会话中的复制消息；即使领导者已不是原始 voter，也可完成新节点追赶。完整快照必须同时证明 sender 为该 cut 的 voter、本机在该 cut 成员集合中。首次实际入组或出现退休墓碑后关闭 JOIN 豁免；被移除身份的连接关闭，不再拨号或投票。Hello 不依赖成员摘要相同，实际 Raft 资格由已应用 ConfState 判定。

独立 `tickwire-control-peer --config /absolute/node.json` 只组装一个真实配置 Replica 所有者；`runner::assemble` 可供 HTTPS 共用。不会自动写初始化命令。配置严格解析且有界，私钥权限、证书和 CRL 在网络启动前校验；快照每 256 个新应用 index 尝试压缩。

读取消按实际 context+term 撤销，未完成项继续占用 1024 配额。票据的私有 Arc/Weak lease 让调用方 Drop 等效于取消；实际 ReadState/任期切换后回收底层工作对应的容量。隔离时不能靠反复 begin/cancel 或 Drop 绕过上限。

`api_runner::ApiOwner` 已把真实 HTTPS worker 接到同一个 `Replica<GenerationStore>`。peer 完成 Ready 后才由窄 facade 处理 HTTP，业务 Applied 仍在原队列中保留。实际三个 mTLS 文件节点的回归经过 HTTPS ReadIndex 请求；尚未引导业务 head 时返回 503，Raft 所有者继续运行。随后提交对象命令并触发停止，未领取的 Applied 仍能精确取出一次。

文件加载在打开时固定 fd，再做类型、长度和私钥权限校验。实际路径替换 Red 曾读出未通过权限检查的替代文件；修复后读取原 fd。真实 FIFO、符号链接、非私有密钥、多硬链接、空文件及超限输入均拒绝。此次增量的日志和独立哈希曾存于已删除的 `docs/validation/control-raft-service/api-owner-results.json`，原阶段 5 证据保持不变。
