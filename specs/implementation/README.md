# 模块实现规格导航（`implementation/`）

本目录 75 篇模块实现规格，按功能域分组。每篇先于对应 crate 源码确定接口、不变量与失败边界；全局索引、架构总览与契约说明见 [`../INDEX.md`](../INDEX.md)。

状态图例：**已实现**＝源码有实质实现且文档/实现总览明确记录测试通过；**部分实现**＝核心已落地，文档明确列出待接线/剩余范围；**状态待确认**＝规格存在但总览无明确完成结论。

## Binance 接入与网络命令

- [instrument-index.md](instrument-index.md) — 固定集合位掩码映射、动态只追加索引、本地盘口身份与行情连续存储。`tickwire-instrument-index` / `tickwire-md` / `tickwire-market-publisher` / `tickwire-market-source`。
- [market-expansion.md](market-expansion.md) — 受信目录追加、来源预热、bank 切换、消费者重新 JOIN 和可重放市场基线（同 catalog、同进程链已验收；重启恢复与 catalog 变更待续）。`tickwire-md` / `tickwire-market-publisher` / `tickwire-market-source` / `tickwire-strategy-sdk` / `tickwire-app`。
- [binance-protocol.md](binance-protocol.md) — HMAC/RSA/Ed25519 签名、订单参数编码与类型化错误分类。`tickwire-binance`。已实现。
- [binance-streams.md](binance-streams.md) — exchangeInfo/REST depth/depthUpdate/executionReport 类型化解码。`tickwire-binance`（feed/events）。已实现。
- [binance-sbe.md](binance-sbe.md) — Spot SBE 固定 schema 解码、精确换算及 MD owner 接线。`tickwire-binance-sbe` / `tickwire-binance-public` / `tickwire-md`。已实现（diff、Trade/BBA owner；depth20 仅解码）。
- [binance-public-session.md](binance-public-session.md) — 公共 WSS 订阅 owner、HTTPS 快照、退避重连与 24h 主动轮换。`tickwire-binance-public`。已实现。
- [binance-depth-sync.md](binance-depth-sync.md) — DepthSync 状态机（Disconnected/Buffering/Bridging/Live/Exhausted）与序列桥接。`tickwire-binance-public`（depth_sync）。已实现。
- [binance-market-events.md](binance-market-events.md) — 精确行情事件（Trade/BBA/MarkFunding）规范发布与 MarketReplica 应用。`tickwire-market-source` / `tickwire-market-publisher`。已实现（七类输入至 SHM/JOIN；生产压力待验收）。
- [binance-private-session.md](binance-private-session.md) — 私有流会话（Spot logon/subscribe、USD-M key 轮换、缺口撤销）。`tickwire-binance-private`。部分实现。
- [binance-private-clock-adapter.md](binance-private-clock-adapter.md) — 私有流共享校准时钟适配与原签名窗口检查。`tickwire-binance-session`。已实现。
- [binance-quota.md](binance-quota.md) — 时间/私有/公共/交易命令到 ExecLimiter 的费用表适配。`tickwire-binance-quota`。已实现。
- [binance-request-extensions.md](binance-request-extensions.md) — Spot OCO/OTO/OTOCO/peg/SOR 高级请求类型化编码（schema2 tag 11）。`tickwire-binance`（命令规范见 `tickwire-venue`）。已实现。
- [binance-execution.md](binance-execution.md) — Binance 执行适配：原签名/租约/期限/本地配额门禁追加。`tickwire-binance-execution`。部分实现。
- [binance-ws-trading.md](binance-ws-trading.md) — WS 交易 owner、DurableWsBinding、响应入账、恢复与复制。`tickwire-binance-account`。部分实现（持久发送链已接；logon、无冻结换线及高级单组合待续）。
- [transport.md](transport.md) — mio/rustls 非阻塞 TCP/TLS、文本分帧与首次接受前门禁。`tickwire-transport`。已实现。
- [venue.md](venue.md) — 不可变规范命令字节/摘要与交易所事件事实规范化。`tickwire-venue`。已实现。
- [gateway.md](gateway.md) — JournaledSender 持久命令发送协调、授权 WAL、NoBytes 中止与冻结。`tickwire-gateway`。已实现。
- [usdm-account-facts.md](usdm-account-facts.md) — USD-M 收入去重关联、批准基线、收入核对与观察关联。`tickwire-account` / `tickwire-binance-account`。部分实现（上述已接；外部成交、强平/ADL/交割及资金费归属待续）。

## IPC / 共享内存传输

- [ipc-codec.md](ipc-codec.md) — ABI 1.0 固定布局 Wire 编解码、ControlFrame 与语义/畸形输入校验。`tickwire-ipc`。已实现。
- [shm-mapping.md](shm-mapping.md) — 单写 memfd 映射、seals、RO 只读切片与原子访问原语。`tickwire-shm-sys`。已实现。
- [shm-ring.md](shm-ring.md) — 固定记录共享广播 ring、bank 借用/释放/容量门禁。`tickwire-shm`。部分实现（implemented-partial）。
- [immutable-resource.md](immutable-resource.md) — 0x0f 封印不可变资源创建/接收与只读 FD 交付。`tickwire-shm-sys`。已实现。
- [topic-snapshot-builder.md](topic-snapshot-builder.md) — NormalizedMarket TopicSnapshotManifest 构造与 JOIN 绑定。`tickwire-snapshot`。已实现。
- [topic-snapshot.md](topic-snapshot.md) — TopicSnapshotManifest 接收校验为 ValidatedTopic。`tickwire-snapshot`。已实现（接收器已用于真实 JOIN）。
- [join.md](join.md) — JOIN owner：prepare/deliver/receive_cursor/read_ready/追平激活。`tickwire-join`。部分实现（真实进程链已接；持久 owner 恢复、代次重用与死亡回收待续）。

## 行情数据与订单簿

- [book-core.md](book-core.md) — 单所有者有界订单簿 reducer 与 Binance SequencePolicy。`tickwire-book`。已实现。
- [book-state-model.md](book-state-model.md) — Book 生命周期/覆盖知识 TLA+ L0/L1 有限验证契约与结果。`tickwire-book`（形式验证记录）。已完成（模型）。

## 策略生命周期与条件单

- [strategy-handover.md](strategy-handover.md) — 实际策略进程交接、Shadow 加载、管理权单次移交。`tickwire-lab`（集成）/ `tickwire-strategy-host`。部分实现。
- [strategy-graceful-retirement.md](strategy-graceful-retirement.md) — 共享 MD 订阅正常退休 QUIESCE/DETACH 证据链。`tickwire-strategy-sdk` / `tickwire-strategy-host`。部分实现。
- [strategy-rebuild.md](strategy-rebuild.md) — 声明窗口、同进程持久安装、独立回退与 schema4 发布。`tickwire-release` / `tickwire-strategy-sdk` / `tickwire-strategy-host` / `tickwire-app`。单域真实链已验收；跨域故障、重启与冷步骤异步化待续。
- [strategy-replay.md](strategy-replay.md) — 旧 binary S→C 合并输入重放、schema3 迁移安装链。`tickwire-release` / `tickwire-strategy-sdk`。部分实现。
- [conditional-orders.md](conditional-orders.md) — SpotList/Algo 父子关联、父查询与未创建 Algo 闭合。`tickwire-account` / `tickwire-binance-account`。部分实现（已至第三阶段；完整订单风险与生命周期组合待续）。
- [spot-amend.md](spot-amend.md) — Spot 同 client ID 减量修改、REST 结果/历史与私有 REPLACED。`tickwire-account` / `tickwire-binance-account`。部分实现（普通 LIMIT/GTC 路径已接；完整修改范围与组合验收待续）。

## 执行内核与账户

- [execution-core.md](execution-core.md) — 执行内核 reducer、订单/资金更新与 MockService 边界。`tickwire-execution` / `tickwire-mock`。部分实现。
- [mock-abort.md](mock-abort.md) — Mock 未发送中止事实、顺序重放与旧引擎冻结。`tickwire-mock`。已实现。
- [account.md](account.md) — 多资产账户状态机：预留/成交/费用/归属/冻结/确定性恢复。`tickwire-account`。已实现。
- [account-codec.md](account-codec.md) — 账户基线与八类恢复记录有界 CBOR 编码摘要。`tickwire-account-codec`。已实现。
- [account-journal.md](account-journal.md) — 账户有序日志固定绑定、有序追加与输入索引。`tickwire-account-journal`。已实现。
- [account-gateway.md](account-gateway.md) — 账户发送协调：准入同步、排队、中止与冻结恢复。`tickwire-account-gateway`。已实现。
- [account-recovery-binding.md](account-recovery-binding.md) — kind 14/15 账户恢复依赖与 ClosureBinding 校验。`tickwire-recovery` / `tickwire-account-gateway`。已实现。
- [reconciliation-controller.md](reconciliation-controller.md) — 对账控制器与终态费用维护调度。`tickwire-app`。已实现。
- [handover-inputs.md](handover-inputs.md) — 交接账户输入完整性校验（TWINP001/InputSeal/恢复核对）。`tickwire-handover`。已实现。

## 风险、额度与时钟

- [risk-core.md](risk-core.md) — 无依赖十进制安全计算库与提交边界（RISK-1…）。`tickwire-risk`。已实现。
- [quota.md](quota.md) — 共享请求额度协调器、窗口/比较交换发放与租约续期。`tickwire-quota`。部分实现。
- [clock.md](clock.md) — 产品/环境/单调域校准核心（anchor/pending/last_seen/fault）。`tickwire-clock`。已实现。
- [system-clock.md](system-clock.md) — OS 单调读取绑定 ClockDomain 的 SystemClock。`tickwire-clock-sys`。已实现。

## 日志、恢复闭包与快照

- [journal.md](journal.md) — 物理 WAL v1 帧/批次/sync_data/分段/manifest 恢复。`tickwire-journal`。已实现。
- [journal-codec.md](journal-codec.md) — 业务日志 codec schema2（PreparedIntent/Fill/Fee/Terminal/Aborted）。`tickwire-journal-codec`。已实现。
- [business-journal.md](business-journal.md) — 业务日志运行时、commit_intent 物理同步收据与恢复。`tickwire-business-journal`。已实现。
- [command-binding.md](command-binding.md) — command_digest 绑定、完整命令对象持久与恢复核对。`tickwire-business-journal` / `tickwire-journal-codec`。已实现。
- [recovery-closure.md](recovery-closure.md) — BlobStore 恢复依赖 publish/verify 与 VerifiedClosure。`tickwire-recovery`。已实现。

## 控制面、Raft 与复制接管

- [runtime-observation.md](runtime-observation.md) — 实际托管实例的新鲜挑战、消费进度与内核事实采集；SDK、Host 与 App 已接线；全局配置事实聚合仍待接入。
- [supervisor-process.md](supervisor-process.md) — 固定执行镜像、内核身份与进程资源约束。`tickwire-control-sys` / `tickwire-supervisor` / `tickwire-strategy-sdk` / `tickwire-strategy-host` / `tickwire-app`。部分实现（资源启动与观察已接线；委派组正向验收、全局配置应用仍待完成）。
- [control-session.md](control-session.md) — 控制会话协商、内核身份核对与请求登记票据。`tickwire-control`。已实现。
- [control-transport.md](control-transport.md) — Linux SOCK_SEQPACKET/SCM_RIGHTS 控制 FD 传输。`tickwire-control-sys`。已实现。
- [control-raft.md](control-raft.md) — 持久 Raft、SafeReadIndex、Chunk 大更新、快照与成员变更。`tickwire-control-raft`。已实现（快照与成员扩展见网络服务规格；生产故障组合待验收）。
- [control-raft-service.md](control-raft-service.md) — Raft 网络服务 TLS1.3 双向/会话帧/快照换代/Joint 成员。`tickwire-control-peer`。已实现（含 learner 追赶与 joint 替换；生产故障组合待验收）。
- [replica.md](replica.md) — 连续前缀异步/同步复制、ACK、SourceRecoverySeal 冻结接管。`tickwire-replica`。部分实现。

## 配置控制面

- [config-candidate.md](config-candidate.md) — TOML/JSON→规范值树、RFC 8785 摘要与结构/静态校验。`tickwire-config`。已实现。
- [config-locator.md](config-locator.md) — ConfigLocator generation 链/checksum/迁移 successor 校验。`tickwire-config`。已实现。
- [config-control-state.md](config-control-state.md) — Replica 复制对象 Chunk/Seal、安全读与部署核验。`tickwire-config-service`。部分实现。
- [config-credential-facts.md](config-credential-facts.md) — 凭据元数据 ProjectionSet 验签与使用集合。`tickwire-config-service`。已实现。
- [config-planning.md](config-planning.md) — 两次真实安全读、完整目标计划内容与后台来源复核。`tickwire-config-service`。部分实现（草案与内容归档已接线，发布准入待接）。
- [config-plan-archive.md](config-plan-archive.md) — 完整计划依赖归档、逐条复制收据与幂等恢复。`tickwire-config-service`。部分实现（内容持久化；发布资格与应用另行接入）。
- [config-archive-sources.md](config-archive-sources.md) — 信任配置严格解码与资料包确定性导出。`tickwire-config-service`。已实现（历史内容归档边界）。
- [config-runtime-facts.md](config-runtime-facts.md) — 运行实例签名观测、host/process 签发范围与基线投影核验。`tickwire-config-service`。已实现（事实内核；采集与持久水位待接）。
- [config-desired-read.md](config-desired-read.md) — 已提交配置安全读、ETag 与 BootstrapHead。`tickwire-config-service`。部分实现。
- [config-file-store.md](config-file-store.md) — Linux 私有根 durable 文件存储（0700/0600/原子 rename）。`tickwire-config-fs`。已实现。
- [config-journal-store.md](config-journal-store.md) — 配置 journal 物理文件后端（分段/hash chain/脏尾截断）。`tickwire-config-fs`。已实现。
- [config-local-journal.md](config-local-journal.md) — 本地配置业务日志 Prepared→Desired→RuntimeObserved→Effective。`tickwire-config-journal`。已实现。
- [config-materializer.md](config-materializer.md) — 本地物化存储、投影对象/last-good/四阶段推进。`tickwire-config-materializer`。已实现。
- [config-server.md](config-server.md) — 配置服务启动组合：schema1 启动配置、ApiOwner、TLS/bootstrap/紧凑快照（binary `tickwire-config-server` 与 `config_runner` 位于 `tickwire-control-peer`，业务 Replica 在 `tickwire-config-service`）。`tickwire-control-peer` / `tickwire-config-service`。部分实现。

## 运行时门禁、发布与监督

- [lifecycle-coordinator.md](lifecycle-coordinator.md) — 多域发布协调器 Prepare/Commit/Release 签名与 GroupRelease。`tickwire-lifecycle`。部分实现。
- [release-preflight.md](release-preflight.md) — 发布预检 Operation/member/回滚 probe/closure 递归验证。`tickwire-release`。已实现。
- [runtime-policy.md](runtime-policy.md) — 运行时控制门禁：策略授权/行情依赖/租约/GapToken。`tickwire-runtime-policy`。部分实现。
- [guardian.md](guardian.md) — 独立 guardian 隔离证明验签、紧急取消与分块审计。`tickwire-guardian` / `tickwire-fencing`。部分实现。
