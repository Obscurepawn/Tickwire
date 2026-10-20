# 生产设计 v0.3 交付核验

核验日期：2026-10-20。本轮完成详细设计、配置/API 契约、数值与编码向量、参考部署和核验脚本。交易引擎与配置服务尚未实现，当前证据不包含真实进程通信、生产交易、延迟实测或 TLC 运行结果。

## 用户要求与当前交付

| 要求 | 当前文件与可审阅内容 |
|---|---|
| Rust 同时对接现货与合约，说明各进程职责 | [主设计](design/binance-ultra-low-latency.md)：独立 md、每策略实例、账户风险域 exec、supervisor、guardian、recorder/replay |
| 参考本地文章、官方协议和开源实现 | [本地调研](research/local-notes.md)、[官方协议](research/binance-protocols.md)、[开源源码](research/opensource-rust.md)、[共享内存调研](research/shared-memory-ipc.md)；保留来源和固定提交 |
| 多策略独立新增、发布与回滚 | [策略生命周期](design/strategy-lifecycle.md)：Shadow、存量订单/持仓原位移交、跨域协调、授权代次与回滚 |
| Disruptor 共享广播与低延迟 IPC | [共享内存机制](design/shared-memory-ipc.md)及[精确 ABI](design/ipc-abi.md)：发布/释放水位、bank 轮换、封印、权限、布局与快照 |
| 生产设计保留完整故障语义，避免逐单重共识 | [复制与接管](design/replication-and-failover.md)：默认本地 durable_intent＋后台异步复制；控制 Raft 只协调低频状态，副本缺尾部冻结 |
| 补齐风险公式与可核算案例 | [风险计算](design/risk-calculations.md)：现货预留/费用、合约压力资本/保证金、减险、条件结构与交割结算 |
| 补齐具体部署和完整链路目标 | [部署与 SLO](design/deployment-and-slo.md)：两交易主机＋控制第三节点、16 Active＋16 Shadow、CPU/内存/磁盘和恢复预算 |
| 全系统文件配置、控制面修改接口、本地文件同步 | [配置契约](design/configuration.md)、[schema](../specs/contracts/system-config.schema.json)、[OpenAPI](../specs/contracts/control-api.openapi.json)、[参考 TOML](../config/reference.toml)和[生产模板](../config/production.template.toml) |
| 单元、集成和 vibe-tla 纳入开发 | [验证计划](verification-plan.md)：30 项 U、65 项 I，关联 RF-01～16、CFG-01～17 和拟建模型；这些引擎用例尚未运行 |
| 本地 Markdown、适量图片及可接续规格 | 八份设计文档、六张 PNG/SVG 与图源、交互式 HTML、[规格索引](../specs/INDEX.md)和[校验命令](../scripts/README.md) |

## 已执行的设计产物校验

机器结果见[设计契约核验记录](validation/design-contract-check.json)与[文档核验记录](validation/documentation-check.json)。前者记录输入文件哈希，后者核对当前图资产和链接。复现命令：

```bash
/tmp/tickwire-design-validation-env/bin/python scripts/verify-design-contracts.py --openapi
/tmp/tickwire-design-validation-env/bin/python scripts/verify-documentation.py
```

依赖安装步骤与固定版本见[脚本说明](../scripts/README.md)。本轮命令均成功退出，具体结果如下。

| 范围 | 已观察到的结果 |
|---|---|
| 二进制 ABI | 36 个布局、624 个字段完整覆盖且对齐；63 个黄金编码向量通过解码/重编码；26 个数据消息 ID、31 个控制操作码；25 个语义案例、19 项 CRC 检查通过 |
| 兼容性样例 | 原 37 个向量字节/哈希保持不变；新增配置控制帧、投影对象和混合行情快照另加向量 |
| 风险算术 | Decimal 重算 21 个案例、131 项预期结果，失败数 0 |
| 配置 schema 与语义 | 参考配置 0 错；17 项负例和 3 种候选输入通过；ProcessProjection、ProjectionObject、ConfigLocator 与 RollingRestart Target 示例通过 |
| 生产模板 | 未填写真实输入的模板出现 413 项预期拒绝，不能用于 live 准入；这不是参考配置的校验失败 |
| 控制 API | openapi-spec-validator 0.9.0 完整校验通过；16 个操作、251 个引用解析通过 |
| 部署一致性 | CPU 与独立内存预算、复制默认值、三类期限、存储保留公式及配置/ABI 参数一致；含 bank、cursor、快照、投影和 relay 的 SHM 为 6,449,856,512 B，低于 8 GiB 上限 |
| 文档与图片 | 本地链接目标、参考定义、代码块、95 个计划用例编号、PNG/SVG 格式、26 项图资产哈希及交付回执通过；完整计数见机器记录 |

参考配置使用模拟账户、fixture 工件及秘密引用。此处验证其结构、公式和相互约束，没有部署这些依赖或运行配置接口。

## 交叉审阅落实的修正

复制、IPC、风险和配置分别进行独立审阅，主设计再核对跨文档约束。当前文件已包含：

- 明确本地提交和异步尾部的证据边界；控制组多数派不能替代订单日志或策略恢复闭包。SyncReplica 单独定义，不能自动退回异步。
- 分开读取、业务应用与最后释放水位；旧 bank 在完整事件边界冻结，活引用及独立 arena 不被覆盖。只读原子访问、写者映射/封印顺序和不受信任策略适配路径分别约束。
- 多标的快照绑定统一边界和集合；混合行情 topic 的快照保存 depth、报价、成交、标记价格与 feed 状态。配置消息区分运行时和文件收据的签发身份。
- 风控空头损失按有符号持仓正确计算；退出费用覆盖潜在成交后的最大持仓，增加针对性数值案例。交割结算依赖真实账户事实，不按定时器直接记账。
- 配置滚动更新预先绑定 predecessor→successor，分别保存实例文件并持久切换稳定文件所有权。
- 配置修改的业务权限与系统分发范围分开；窄账户使用 scoped-patch，完整目标集合仍决定成功。
- 普通进程只接收授权投影，不能通过全量只读文件读取其他账户的配置或秘密引用。
- 配置根目录迁移使用固定引导 locator、唯一写者屏障、完整前缀复制和持久切换；重启不按目录 mtime 选版本。

配置最后四项已由原审阅者逐项复核闭合。以上是设计契约的修正，尚不是已修复的 Rust 运行时缺陷。

## 图片交付

最终配置发布图已实际打开，检查中文、箭头和裁切。其余 PNG 与交互 HTML 保持此前审阅的字节；架构 SVG 仅清理 CSS 行尾空白，解析检查通过。当前重新核对 manifest 与交付记录。

| 图 | 文件与尺寸 |
|---|---|
| 进程与职责 | [PNG](assets/process-topology.png)，3568 × 1074；[SVG](assets/process-topology.svg) |
| 共享广播与执行通道 | [PNG](assets/tickwire-architecture.png)，4060 × 2550；[SVG](assets/tickwire-architecture.svg) |
| 订单状态 | [PNG](assets/order-state.png)，1244 × 1376；[SVG](assets/order-state.svg) |
| exec 恢复 | [PNG](assets/recovery-sequence.png)，2198 × 2146；[SVG](assets/recovery-sequence.svg) |
| 策略原位移交 | [PNG](assets/strategy-rollout.png)，2430 × 2666；[SVG](assets/strategy-rollout.svg) |
| 配置发布与持久化 | [PNG](assets/config-release.png)，2328 × 2498；[SVG](assets/config-release.svg) |

图源和复现方式见 [assets 说明](assets/README.md)，全部图资产绑定见 [manifest](assets/diagram-assets.manifest.json)。Mermaid 使用 11.17.0，Archify 使用 2.17。

### 交互架构图交付记录

```text
diagram_type: architecture
output: /home/obscure/Tickwire/docs/assets/tickwire-architecture.html
specification_sha256: a40edf6a6066d29069eac3569bc21b1d6c309b26320b6a7f0f5c21aad152bd0c
artifact_sha256: 462c18a0250623426052f6ec8b8af53270b75989e6f12c7d183b32fea7a4710d
validation: 9/9 showcase, 0 errors, 0 warnings
browser_evidence: passed
visual_review: passed
correction_rounds: 0
```

[交付 receipt](assets/tickwire-architecture.delivery.json)、[浏览器 receipt](assets/tickwire-architecture.visual-check.json)和[独立视觉复核](assets/tickwire-architecture.review.json)分别保存不同证据。浏览器覆盖 1440×900、1600×1000、1920×1080、2048×1320，页面均无横向/纵向溢出，并生成端点尺寸的明暗主题截图。主审复看了最终的浅色/深色截图、进程图、恢复图和策略移交图。自动 receipt 的 visualReview 保持原始 pending，视觉结论另行记录。

## 后续实现与资格验证

详细设计已交付。下一阶段从真实协议/部署能力验证和 Rust workspace、IPC 状态机开始，按[验证计划](verification-plan.md)实现配置、风控、发布与恢复，并建立有真实源码锚点的 TLA+ 模型。当前没有运行 Rust 单元/多进程测试、TLC 或性能实验；300µs/1000µs 等完整路径数字仍是资格目标。
