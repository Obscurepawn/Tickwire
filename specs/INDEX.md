# Tickwire 规格索引（接续开发权威入口）

本目录是 Tickwire 生产目标、设计决策、机器契约与模块实现规格的唯一入口。开发新模块或修改既有模块前，先读本索引，再按「建议阅读顺序」定位对应规格；文档语言为简体中文，技术名词与代码保留英文。

- 项目根说明：[`../README.md`](../README.md)
- 形式验证工作区：[`../tla/`](../tla/)（入口见 [`../tla/README.md`](../tla/README.md) 与 [`../tla/ROADMAP.md`](../tla/ROADMAP.md)）
- 契约校验脚本说明：[`../scripts/README.md`](../scripts/README.md)
- 模块实现规格导航：[`implementation/README.md`](implementation/README.md)
- 机器契约说明：[`contracts/README.md`](contracts/README.md)

## 1. 项目架构总览

Tickwire 是面向 Binance Spot 与 USDⓈ-M 的 Rust 超低延迟生产交易系统，采用多进程隔离主架构：独立行情进程、每策略实例独立进程、按账户风险域独立的执行进程，以及控制监督（guardian）与紧急执行进程；策略代码不进入 exec 进程，多策略可独立加入、升级、回滚与退出，OMS/交易连接不因策略更新重启。行情经 Disruptor 式固定记录共享广播 ring（单写、受保护借用、协作退出、最多 32 消费者、bank 轮换）分发给策略；意图/回报走独立 SPSC 通道，exec 单写资金与订单。数据面默认本地 durable_intent 加后台异步复制，逐单不经过 Raft；控制 Raft 只协调低频配置、发布与执行权。恢复依赖完整证据闭包，缺尾部进入 EvidenceLost/Frozen，跨主机接管要求实际隔离与对账。全系统统一配置 schema，控制 API 更改后运行时与本地 effective 文件达到同一 revision/hash 才算成功。

进程与数据流要点：

1. **行情（md）**：公共 WSS/HTTPS 取得深度并桥接为本地 Book，精确行情事件经共享内存 ring 广播；JOIN 完成快照 S 移交与追平激活。
2. **策略（strategy）**：独立进程收发行情与规范意图、账户回报；换代走 Shadow 加载、签名 Prepare/Commit/Release 与原位移交，不重复发单或释放资金。
3. **执行/账户（exec/account）**：准入检查风险预算后持久意图到 WAL，授权与依赖同步完成才排队发送；成交、费用、终态按原顺序入账，冻结后仍允许有效 Cancel/Query。
4. **控制面（control/raft/config）**：三 voter 持久 Raft 承载配置对象、发布操作与安全读；本地四阶段业务日志把 Prepared/Desired/RuntimeObserved/Effective 落到文件。
5. **恢复与复制（journal/replica/guardian）**：物理 WAL 同步收据、恢复依赖闭包、异步/同步镜像复制与隔离证据共同约束接管；guardian 持独立密钥做紧急取消与审计。

设计依据见 [总体设计 v0.3](20261020-binance-ultra-low-latency.md)，实现阶段与已完成边界见 [Rust 实现与验证](20261020-implementation.md)。

## 2. 建议阅读顺序

1. **总体设计**：[`20261020-binance-ultra-low-latency.md`](20261020-binance-ultra-low-latency.md)（目标、约束、验收 AC、关键决策与风险）。
2. **机器契约**：[`contracts/`](contracts/) 七个 JSON（先读 [`contracts/README.md`](contracts/README.md)），它们是 IPC ABI、风险数值、配置 schema、控制 API、复制与部署的可核验事实来源。
3. **实现总览**：[`20261020-implementation.md`](20261020-implementation.md)（分阶段实施状态、已验证模块与后续验收）。
4. **模块实现规格**：[`implementation/`](implementation/)，按下文第 3 节分组索引定位；每篇先于对应 crate 源码确定接口与不变量。
5. **形式验证**：[`../tla/`](../tla/) 中与改动模块对应的 `execution_models/<model>/ROADMAP.md`；源码变更后先核对锚点哈希再复跑。

## 3. 实现规格索引（按功能域分组，共 76 篇）

状态图例：**已实现**＝源码有实质实现且文档/实现总览明确记录测试通过；**部分实现**＝核心已落地，文档明确列出待接线/剩余范围；**状态待确认**＝规格存在但总览无明确完成结论，接手时先核对源码。

### 3.1 Binance 接入与网络命令

| 文件 | 职责一句话 | 对应 crate | 实现状态 |
|---|---|---|---|
| [instrument-index.md](implementation/instrument-index.md) | 固定集合位掩码映射、动态只追加索引与行情连续存储 | `tickwire-instrument-index` / `tickwire-md` / `tickwire-market-publisher` / `tickwire-market-source` | 固定/动态索引、本地盘口身份及行情接线；性能边界见规格 |
| [market-expansion.md](implementation/market-expansion.md) | 受信目录追加、来源预热、bank 切换、消费者重新 JOIN 与可重放市场基线 | `tickwire-md` / `tickwire-market-publisher` / `tickwire-market-source` / `tickwire-strategy-sdk` / `tickwire-app` | 部分实现（同 catalog、同进程扩容及跨 bank 交接已验收；重启恢复与 catalog 变更待续） |
| [binance-protocol.md](implementation/binance-protocol.md) | HMAC/RSA/Ed25519 签名、订单参数编码与类型化错误分类 | `tickwire-binance` | 已实现 |
| [binance-streams.md](implementation/binance-streams.md) | exchangeInfo/REST depth/depthUpdate/executionReport 类型化解码 | `tickwire-binance`（feed/events） | 已实现 |
| [binance-sbe.md](implementation/binance-sbe.md) | Spot SBE 固定 schema 解码、精确换算及 MD owner 接线 | `tickwire-binance-sbe` / `tickwire-binance-public` / `tickwire-md` | 已实现（diff、Trade/BBA owner；depth20 仅解码） |
| [binance-public-session.md](implementation/binance-public-session.md) | 公共 WSS 订阅 owner、HTTPS 快照、退避重连与 24h 主动轮换 | `tickwire-binance-public` | 已实现 |
| [binance-depth-sync.md](implementation/binance-depth-sync.md) | DepthSync 状态机（Disconnected/Buffering/Bridging/Live/Exhausted）与序列桥接 | `tickwire-binance-public`（depth_sync） | 已实现 |
| [binance-market-events.md](implementation/binance-market-events.md) | 精确行情事件（Trade/BBA/MarkFunding）规范发布与 MarketReplica 应用 | `tickwire-market-source` / `tickwire-market-publisher` | 已实现（七类输入至 SHM/JOIN；生产压力待验收） |
| [binance-private-session.md](implementation/binance-private-session.md) | 私有流会话（Spot logon/subscribe、USD-M key 轮换、缺口撤销） | `tickwire-binance-private` | 部分实现 |
| [binance-private-clock-adapter.md](implementation/binance-private-clock-adapter.md) | 私有流共享校准时钟适配与原签名窗口检查 | `tickwire-binance-session` | 已实现 |
| [binance-quota.md](implementation/binance-quota.md) | 时间/私有/公共/交易命令到 ExecLimiter 的费用表适配 | `tickwire-binance-quota` | 已实现 |
| [binance-request-extensions.md](implementation/binance-request-extensions.md) | Spot OCO/OTO/OTOCO/peg/SOR 高级请求类型化编码（schema2 tag 11） | `tickwire-binance`（命令规范见 `tickwire-venue`） | 已实现 |
| [binance-execution.md](implementation/binance-execution.md) | Binance 执行适配：原签名/租约/期限/本地配额门禁追加 | `tickwire-binance-execution` | 部分实现 |
| [binance-ws-trading.md](implementation/binance-ws-trading.md) | WS 交易 owner、DurableWsBinding、响应入账、恢复与复制 | `tickwire-binance-account` | 部分实现（持久发送链已接；logon、无冻结换线及高级单组合待续） |
| [transport.md](implementation/transport.md) | mio/rustls 非阻塞 TCP/TLS、文本分帧与首次接受前门禁 | `tickwire-transport` | 已实现 |
| [venue.md](implementation/venue.md) | 不可变规范命令字节/摘要与交易所事件事实规范化 | `tickwire-venue` | 已实现 |
| [gateway.md](implementation/gateway.md) | JournaledSender 持久命令发送协调、授权 WAL、NoBytes 中止与冻结 | `tickwire-gateway` | 已实现 |
| [usdm-account-facts.md](implementation/usdm-account-facts.md) | USD-M 收入去重关联、批准基线、收入核对与观察关联 | `tickwire-account` / `tickwire-binance-account` | 部分实现（上述已接；外部成交、强平/ADL/交割及资金费归属待续） |

### 3.2 IPC / 共享内存传输

| 文件 | 职责一句话 | 对应 crate | 实现状态 |
|---|---|---|---|
| [ipc-codec.md](implementation/ipc-codec.md) | ABI 1.0 固定布局 Wire 编解码、ControlFrame 与语义/畸形输入校验 | `tickwire-ipc` | 已实现 |
| [shm-mapping.md](implementation/shm-mapping.md) | 单写 memfd 映射、seals、RO 只读切片与原子访问原语 | `tickwire-shm-sys` | 已实现 |
| [shm-ring.md](implementation/shm-ring.md) | 固定记录共享广播 ring、bank 借用/释放/容量门禁（implemented-partial） | `tickwire-shm` | 部分实现 |
| [immutable-resource.md](implementation/immutable-resource.md) | 0x0f 封印不可变资源创建/接收与只读 FD 交付 | `tickwire-shm-sys` | 已实现 |
| [topic-snapshot-builder.md](implementation/topic-snapshot-builder.md) | NormalizedMarket TopicSnapshotManifest 构造与 JOIN 绑定 | `tickwire-snapshot` | 已实现 |
| [topic-snapshot.md](implementation/topic-snapshot.md) | TopicSnapshotManifest 接收校验为 ValidatedTopic | `tickwire-snapshot` | 已实现（接收器已用于真实 JOIN） |
| [join.md](implementation/join.md) | JOIN owner：prepare/deliver/receive_cursor/read_ready/追平激活 | `tickwire-join` | 部分实现（真实进程链已接；持久 owner 恢复、代次重用与死亡回收待续） |

### 3.3 行情数据与订单簿

| 文件 | 职责一句话 | 对应 crate | 实现状态 |
|---|---|---|---|
| [book-core.md](implementation/book-core.md) | 单所有者有界订单簿 reducer 与 Binance SequencePolicy | `tickwire-book` | 已实现 |
| [book-state-model.md](implementation/book-state-model.md) | Book 生命周期/覆盖知识 TLA+ L0/L1 有限验证契约与结果 | `tickwire-book`（形式验证记录） | 已完成（模型） |

### 3.4 策略生命周期与条件单

| 文件 | 职责一句话 | 对应 crate | 实现状态 |
|---|---|---|---|
| [strategy-handover.md](implementation/strategy-handover.md) | 实际策略进程交接、Shadow 加载、管理权单次移交 | `tickwire-lab`（集成）/ `tickwire-strategy-host` | 部分实现 |
| [strategy-graceful-retirement.md](implementation/strategy-graceful-retirement.md) | 共享 MD 订阅正常退休 QUIESCE/DETACH 证据链 | `tickwire-strategy-sdk` / `tickwire-strategy-host` | 部分实现 |
| [strategy-rebuild.md](implementation/strategy-rebuild.md) | 声明窗口、同进程持久安装、独立回退与发布 | `tickwire-release` / `tickwire-strategy-sdk` / `tickwire-strategy-host` / `tickwire-app` | 部分实现（单域真实链已验收；跨域故障、重启与冷步骤异步化待续） |
| [strategy-replay.md](implementation/strategy-replay.md) | 旧 binary S→C 合并输入重放、schema3 迁移安装链 | `tickwire-release` / `tickwire-strategy-sdk` | 部分实现 |
| [conditional-orders.md](implementation/conditional-orders.md) | SpotList/Algo 父子关联、父查询与未创建 Algo 闭合 | `tickwire-account` / `tickwire-binance-account` | 部分实现（已至第三阶段；完整订单风险与生命周期组合待续） |
| [spot-amend.md](implementation/spot-amend.md) | Spot 同 client ID 减量修改、REST 结果/历史与私有 REPLACED | `tickwire-account` / `tickwire-binance-account` | 部分实现（普通 LIMIT/GTC 路径已接；完整修改范围与组合验收待续） |

### 3.5 执行内核与账户

| 文件 | 职责一句话 | 对应 crate | 实现状态 |
|---|---|---|---|
| [execution-core.md](implementation/execution-core.md) | 执行内核 reducer、订单/资金更新与 MockService 边界 | `tickwire-execution` / `tickwire-mock` | 部分实现 |
| [mock-abort.md](implementation/mock-abort.md) | Mock 未发送中止事实、顺序重放与旧引擎冻结 | `tickwire-mock` | 已实现 |
| [account.md](implementation/account.md) | 多资产账户状态机：预留/成交/费用/归属/冻结/确定性恢复 | `tickwire-account` | 已实现 |
| [account-codec.md](implementation/account-codec.md) | 账户基线与八类恢复记录有界 CBOR 编码摘要 | `tickwire-account-codec` | 已实现 |
| [account-journal.md](implementation/account-journal.md) | 账户有序日志固定绑定、有序追加与输入索引 | `tickwire-account-journal` | 已实现 |
| [account-gateway.md](implementation/account-gateway.md) | 账户发送协调：准入同步、排队、中止与冻结恢复 | `tickwire-account-gateway` | 已实现 |
| [account-recovery-binding.md](implementation/account-recovery-binding.md) | kind 14/15 账户恢复依赖与 ClosureBinding 校验 | `tickwire-recovery` / `tickwire-account-gateway` | 已实现 |
| [reconciliation-controller.md](implementation/reconciliation-controller.md) | 对账控制器与终态费用维护调度 | `tickwire-app` | 已实现 |
| [handover-inputs.md](implementation/handover-inputs.md) | 交接账户输入完整性校验（TWINP001/InputSeal/恢复核对） | `tickwire-handover` | 已实现 |

### 3.6 风险、额度与时钟

| 文件 | 职责一句话 | 对应 crate | 实现状态 |
|---|---|---|---|
| [risk-core.md](implementation/risk-core.md) | 无依赖十进制安全计算库与提交边界（RISK-1…） | `tickwire-risk` | 已实现 |
| [quota.md](implementation/quota.md) | 共享请求额度协调器、窗口/比较交换发放与租约续期 | `tickwire-quota` | 部分实现 |
| [clock.md](implementation/clock.md) | 产品/环境/单调域校准核心（anchor/pending/last_seen/fault） | `tickwire-clock` | 已实现 |
| [system-clock.md](implementation/system-clock.md) | OS 单调读取绑定 ClockDomain 的 SystemClock | `tickwire-clock-sys` | 已实现 |

### 3.7 日志、恢复闭包与快照

| 文件 | 职责一句话 | 对应 crate | 实现状态 |
|---|---|---|---|
| [journal.md](implementation/journal.md) | 物理 WAL v1 帧/批次/sync_data/分段/manifest 恢复 | `tickwire-journal` | 已实现 |
| [journal-codec.md](implementation/journal-codec.md) | 业务日志 codec schema2（PreparedIntent/Fill/Fee/Terminal/Aborted） | `tickwire-journal-codec` | 已实现 |
| [business-journal.md](implementation/business-journal.md) | 业务日志运行时、commit_intent 物理同步收据与恢复 | `tickwire-business-journal` | 已实现 |
| [command-binding.md](implementation/command-binding.md) | command_digest 绑定、完整命令对象持久与恢复核对 | `tickwire-business-journal` / `tickwire-journal-codec` | 已实现 |
| [recovery-closure.md](implementation/recovery-closure.md) | BlobStore 恢复依赖 publish/verify 与 VerifiedClosure | `tickwire-recovery` | 已实现 |

### 3.8 控制面、Raft 与复制接管

| 文件 | 职责一句话 | 对应 crate | 实现状态 |
|---|---|---|---|
| [runtime-observation.md](implementation/runtime-observation.md) | 新鲜检查点挑战、真实托管实例与消费进度的冷采集 | `tickwire-strategy-sdk` / `tickwire-strategy-host` / `tickwire-app` | 已实现（本机实际实例查询；全局配置事实聚合仍待接入） |
| [supervisor-process.md](implementation/supervisor-process.md) | 固定执行镜像、内核身份、进程资源约束与投影启动字段 | `tickwire-control-sys` / `tickwire-supervisor` / `tickwire-strategy-sdk` / `tickwire-strategy-host` / `tickwire-app` | 部分实现（策略首次启动接入投影约束；委派组正向验收、完整全局配置应用仍待完成） |
| [control-session.md](implementation/control-session.md) | 控制会话协商、内核身份核对与请求登记票据 | `tickwire-control` | 已实现 |
| [control-transport.md](implementation/control-transport.md) | Linux SOCK_SEQPACKET/SCM_RIGHTS 控制 FD 传输 | `tickwire-control-sys` | 已实现 |
| [control-raft.md](implementation/control-raft.md) | 持久 Raft、SafeReadIndex、Chunk 大更新、快照与成员变更 | `tickwire-control-raft` | 已实现（快照与成员扩展见网络服务规格；生产故障组合待验收） |
| [control-raft-service.md](implementation/control-raft-service.md) | Raft 网络服务 TLS1.3 双向/会话帧/快照换代/Joint 成员 | `tickwire-control-peer` | 已实现（含 learner 追赶与 joint 替换；生产故障组合待验收） |
| [replica.md](implementation/replica.md) | 连续前缀异步/同步复制、ACK、SourceRecoverySeal 冻结接管 | `tickwire-replica` | 部分实现 |

### 3.9 配置控制面

| 文件 | 职责一句话 | 对应 crate | 实现状态 |
|---|---|---|---|
| [config-candidate.md](implementation/config-candidate.md) | TOML/JSON→规范值树、RFC 8785 摘要与结构/静态校验 | `tickwire-config` | 已实现 |
| [config-locator.md](implementation/config-locator.md) | ConfigLocator generation 链/checksum/迁移 successor 校验 | `tickwire-config` | 已实现 |
| [config-control-state.md](implementation/config-control-state.md) | Replica 复制对象 Chunk/Seal、安全读与部署核验 | `tickwire-config-service` | 部分实现 |
| [config-credential-facts.md](implementation/config-credential-facts.md) | 凭据元数据 ProjectionSet 验签与使用集合 | `tickwire-config-service` | 已实现 |
| [config-planning.md](implementation/config-planning.md) | 两次真实安全读、完整目标计划内容与后台来源复核 | `tickwire-config-service` | 部分实现（草案与内容归档已接线，发布准入待接） |
| [config-plan-archive.md](implementation/config-plan-archive.md) | 完整计划依赖归档、逐条复制收据与幂等恢复 | `tickwire-config-service` | 部分实现（内容持久化；发布资格与应用另行接入） |
| [config-archive-sources.md](implementation/config-archive-sources.md) | 信任配置严格解码与资料包确定性导出 | `tickwire-config-service` | 已实现（历史内容归档边界） |
| [config-runtime-facts.md](implementation/config-runtime-facts.md) | 运行实例签名观测、host/process 签发范围与基线投影核验 | `tickwire-config-service` | 已实现（事实内核；采集与持久水位待接） |
| [config-desired-read.md](implementation/config-desired-read.md) | 已提交配置安全读、ETag 与 BootstrapHead | `tickwire-config-service` | 部分实现 |
| [config-file-store.md](implementation/config-file-store.md) | Linux 私有根 durable 文件存储（0700/0600/原子 rename） | `tickwire-config-fs` | 已实现 |
| [config-journal-store.md](implementation/config-journal-store.md) | 配置 journal 物理文件后端（分段/hash chain/脏尾截断） | `tickwire-config-fs` | 已实现 |
| [config-local-journal.md](implementation/config-local-journal.md) | 本地配置业务日志 Prepared→Desired→RuntimeObserved→Effective | `tickwire-config-journal` | 已实现 |
| [config-materializer.md](implementation/config-materializer.md) | 本地物化存储、投影对象/last-good/四阶段推进 | `tickwire-config-materializer` | 已实现 |
| [config-server.md](implementation/config-server.md) | 配置服务启动组合：schema1 启动配置、ApiOwner、TLS/bootstrap/紧凑快照（binary `tickwire-config-server` 与 `config_runner` 位于 `tickwire-control-peer`，业务 Replica 在 `tickwire-config-service`） | `tickwire-control-peer` / `tickwire-config-service` | 部分实现 |

### 3.10 运行时门禁、发布与监督

| 文件 | 职责一句话 | 对应 crate | 实现状态 |
|---|---|---|---|
| [lifecycle-coordinator.md](implementation/lifecycle-coordinator.md) | 多域发布协调器 Prepare/Commit/Release 签名与 GroupRelease | `tickwire-lifecycle` | 部分实现 |
| [release-preflight.md](implementation/release-preflight.md) | 发布预检 Operation/member/回滚 probe/closure 递归验证 | `tickwire-release` | 已实现 |
| [runtime-policy.md](implementation/runtime-policy.md) | 运行时控制门禁：策略授权/行情依赖/租约/GapToken | `tickwire-runtime-policy` | 部分实现 |
| [guardian.md](implementation/guardian.md) | 独立 guardian 隔离证明验签、紧急取消与分块审计 | `tickwire-guardian` / `tickwire-fencing` | 部分实现 |

## 4. 机器契约（`contracts/`）

逐字段说明见 [`contracts/README.md`](contracts/README.md)。七个 JSON 与消费方对照：

| 契约 | 用途 | 消费脚本 / 测试 |
|---|---|---|
| [`control-api.openapi.json`](contracts/control-api.openapi.json) | OpenAPI 3.1.1（v1.0.0-design）配置控制 API：validate/preview/plans/commits/rollback/head/desired/revisions/effective/operations/watch/schema，mTLS 与幂等 | `scripts/verify-design-contracts.py --openapi`（标准 OpenAPI 验证器）；`scripts/verify-config-contract.py`（接口引用交叉核对） |
| [`deployment-reference-v1.json`](contracts/deployment-reference-v1.json) | 格式 `tickwire-deployment-reference-1`，design 0.3，profile `TW-LD-ASYNC-16x2-v1`：参考部署容量/CPU/内存/存储/延迟 SLO 合同 | `scripts/verify-design-contracts.py`（部署内存/CPU/存储/延迟跨文件一致性）；`scripts/verify-config-contract.py`（dep 对照） |
| [`ipc-vectors-v1.json`](contracts/ipc-vectors-v1.json) | 格式 `tickwire-ipc-vectors-1`，ABI 1.0，CRC32C：编码 golden 向量、语义案例与兼容基线 | `scripts/verify-ipc-contract.py`（向量哈希/帧校验/语义案例）；`tickwire-ipc` Rust 回归 |
| [`ipc-wire-v1.json`](contracts/ipc-wire-v1.json) | 格式 `tickwire-ipc-contract-1`，ABI 1.0：46 个布局、734 个字段、枚举/bitsets/31 种消息 ID/control_opcodes、production_reference 容量、资源约束、配置控制与精确行情事件规则 | `scripts/verify-ipc-contract.py`（布局覆盖/对齐/保留字节/类型宽度）；`verify-design-contracts.py` 与 `verify-config-contract.py` 读取 `profiles.production_reference` 做容量一致性 |
| [`replication-profile-v1.json`](contracts/replication-profile-v1.json) | `tickwire-replication-profile` v1，design 0.3：LocalDurable+AsyncReplica 默认与 SyncReplica、拓扑、Raft 引擎(bin raft 0.7.0)、batching、WAL 帧、异步复制、依赖、快照、成员资格 | `scripts/verify-design-contracts.py`（与部署/延迟合同对照） |
| [`risk-vectors-v1.json`](contracts/risk-vectors-v1.json) | schema_version 1：21 个风险案例/131 项预期结果、Decimal 舍入与算术 | `scripts/verify-risk-vectors.py`（Decimal 重算逐项比对）；`tickwire-risk` 测试读取合成向量 |
| [`system-config.schema.json`](contracts/system-config.schema.json) | Draft 2020-12 全局配置 schema：identity/hosts/processes/venues/ipc/strategies/accounts/risk_policies/journal/replication/control/local_config/observability/timeouts/qualification | `scripts/verify-config-contract.py`（全量/投影 schema、参考 TOML、跨字段约束、负例）；`tickwire-config` 的 `validate_structural()` |

## 5. 如何接续开发

### 5.1 新增或修改模块

1. **先改规格**：在 `implementation/` 新增或更新对应模块规格（接口先行、列出不变量与失败边界），同步更新本索引表与 `implementation/README.md`；涉及对外字节格式、数值或配置项时，先改 `contracts/` 对应 JSON 契约。
2. **TDD 实现**：按接口写可编译失败测试（Red），再实现到测试通过（Green），保持 `unsafe` 仅在许可的低层 crate 边界。
3. **源码完成后**再按最终源码快照执行/更新对应 TLA+ 模型（`../tla/execution_models/`），开发中不启动新的形式化检查。

### 5.2 校验命令与推荐顺序

设计契约校验（Python 3.11+，不连接 Binance、不读凭据）与 Rust 测试是两条独立证据链，先各自通过再组合：

```bash
# 一次性设计契约环境
python3 -m venv /tmp/tickwire-design-validation-env
/tmp/tickwire-design-validation-env/bin/python -m pip install -r scripts/requirements-design.txt

# 设计契约（汇总 IPC/风险/配置/部署/OpenAPI）
/tmp/tickwire-design-validation-env/bin/python scripts/verify-design-contracts.py --openapi
/tmp/tickwire-design-validation-env/bin/python scripts/verify-documentation.py
# 单项契约
python3 scripts/verify-ipc-contract.py
python3 scripts/verify-risk-vectors.py
python3 scripts/verify-config-contract.py
```

```bash
# Rust：格式 + 全 workspace 回归（含 example/bench smoke/编译失败 doctest）+ Clippy -D warnings
./scripts/check-rust.sh
# 本地 Mock 完整业务流程
cargo run -p tickwire-mock --example spot-roundtrip
# Linux 固定容器：memfd/seals/映射权限与 1/2/4/8/16/32 消费者（需 Docker daemon）
./scripts/check-linux.sh
# 固定 TLC 1.7.4 全部模型配置（需 Java）
./scripts/check-tla.sh
# 仅性能基准（勿把调试 smoke 当性能数据）
cargo bench -p tickwire-book --bench depth_apply
```

推荐顺序：模块改动后先 `cargo test -p <crate>` 与 `cargo clippy` 局部验证 → `./scripts/check-rust.sh` 全仓格式/回归/Clippy → 涉及字节/数值/配置时跑对应 `verify-*.py` → 涉及模型时 `./scripts/check-tla.sh` → 需要 Linux 多进程证据时 `./scripts/check-linux.sh`。契约脚本核验的是设计事实（布局、向量、schema、算术、部署合同），不替代 Rust 运行时或真实进程通信测试；脚本细节与 Raft protobuf 依赖（`raft 0.7.0` / protoc 3.x）见 [`../scripts/README.md`](../scripts/README.md)。

## 6. 其他工作区导航

- 根 README：[`../README.md`](../README.md)（项目概述与当前进展）。
- TLA+：[`../tla/README.md`](../tla/README.md)、总览 [`../tla/ROADMAP.md`](../tla/ROADMAP.md)；模型位于 `../tla/execution_models/<model>/`，各层保存输入、源码锚点、配置、运行记录与结论。
- 校验脚本：[`../scripts/README.md`](../scripts/README.md)。
