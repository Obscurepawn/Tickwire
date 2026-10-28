# 2026-10-28 阶段证据

本目录按源码快照保存验证结果。每份报告只覆盖其引用的源码清单与二进制摘要；后续改动保留较早记录。

| 阶段（按执行顺序） | 报告 | 范围 |
|---|---|---|
| 标的索引、连续盘口、恢复修复 | [summary.json](summary.json) | 五轮独立索引基准；普通及 16 MiB＋1 字节策略 ELF 的 schema3 进程交接；有界输入采集库 |
| 完整外部 ID 与本地盘口 key 接线 | [dense-book.json](dense-book.json) | 完整 `u64` 经真实 SBE 网络与共享内存发布；高位 ID 与本地槽隔离；重新执行全仓与进程验收 |
| 发布热路径 | [publisher-hot.json](publisher-hot.json) | 共享 JSON/SBE 解码工作区、真实发布零分配、2 MiB 栈、缓存预算与失败边界；再次执行全仓与进程验收 |
| 行情扩容基础接口 | [market-expansion-core.json](market-expansion-core.json) | 计划核验、候选预热、真实 QUIESCED/重新 JOIN、跨 bank 持久重放；全仓与现有 Spot 进程兼容性，持续循环装配尚未完成 |
| 运行时行情扩容 | [market-expansion-runtime.json](market-expansion-runtime.json) | 同 catalog 同进程追加高位 ID；三个消费者实际激活；S=2→扩容观察点161→C=167 完整前缀、schema3 交接及费用闭合；全仓与契约校验通过 |
| 声明重建采集 | [capture-runtime.json](capture-runtime.json) | SDK/Host 实际采集子进程、完整窗口与九对象闭包、真实退订；两个 release worker 验收及旧 schema3 交接兼容性通过 |
| 声明重建安装与发布 | [rebuild-runtime.json](rebuild-runtime.json) | 同 PID 实际工厂安装、窗口后接续、独立回退与 schema4 协调发布；全仓与 schema3 兼容场景通过 |
| 配置计划内容与运行实例验签 | [config-planning.json](config-planning.json) | 双安全读内部草案、完整目标动作与有界后台任务、运行实例签发范围；持久发布及实际应用仍待接 |
| 配置计划内容持久化（10 月 29 日） | [config-archive.json](config-archive.json) | 完整依赖归档、并发幂等、真实三 voter 提交和换主读取、GenerationStore 快照安装及尾日志恢复；当前 desired 保持不变 |

`summary.json` 中的 `final` / `verified` 文件名指第一阶段结束时的快照。`dense-book.json` 记录的 JSON 栈溢出和 8 MiB 测试绕行属于第二阶段，已在发布热路径阶段修复；对应旧报告保持原样。

运行时扩容阶段的 [源码清单](market-expansion-runtime-source-sha256.json) 绑定 794 个构建输入，[二进制清单](market-expansion-runtime-binaries.json)绑定八个实际部署文件。报告中的 `/tmp/` 路径记录本次运行位置，原始日志摘要用于校验保留的本地输出。全链路使用本机 TLS 交易所 fixture；耗时是场景运行时间，基准 smoke 不作为性能数据。本轮没有新增 TLC 结果。

运行时场景的累计输入统计包含一次首次账户准入前的 `Clock` Gap；后续完整快照与首条 `Reactivated` 已将其关闭。额外账户记录是周期查询得到的 NEW 状态，成交量为零。[只读审计结果](market-expansion-runtime-input-audit.json)与[留存场景复现脚本](market-expansion-runtime-input-audit.py)保存物理 WAL 校验和精确输入/账户绑定；脚本使用本次留存的本机路径，日志未提供更细的时钟采样原因。

扩容基础接口改到了 JOIN L0 的锚点文件。该锁在本阶段之前已有行号失配，本次从 552 行增加到 558 行；旧模型覆盖固定身份和单 bank，后续形式化验收还需加入扩容与跨 bank 行为。[只读审计](market-expansion-core-anchor-audit.json)及其[复现脚本](market-expansion-core-anchor-audit.py)保存了原始基线与逐行结果，原锁和历史 TLC 输出保持不变。

运行时装配阶段相对 `e20ce05` 没有改动现有锚点覆盖的源码，也没有修改模型、锁或历史 TLC 输出。[本阶段审计](market-expansion-runtime-anchor-audit.json)及其[复现脚本](market-expansion-runtime-anchor-audit.py)扫描了 25 份锁；新增扩容控制、持久化与跨 bank 前沿行为仍待最终形式化验证。

声明重建采集阶段的 [808 项构建输入清单](capture-runtime-source-sha256.json) 同时绑定全仓检查、独立 release 采集 worker 和旧交接场景。采集验收使用固定可执行文件副本，避免 Cargo 并行构建替换路径；真实 MD JOIN/共享内存发布器运行在测试父线程，策略运行在独立子进程。完整九对象导出与两个显式进程测试的原始路径、SHA-256 记录在主报告中。

[兼容场景结果](capture-runtime-compat-result.json)与[八程序构建/部署摘要](capture-runtime-compat-binaries.json)保存旧 schema3 链路验证；启动时累计的一条 Clock Gap 已由首条账户激活覆盖。[采集阶段锚点审计](capture-runtime-anchor-audit.json)及其[复现脚本](capture-runtime-anchor-audit.py)仍为只读检查：25 份锁无本阶段源码重叠，未运行 TLC。目标安装与完整 schema4 发布继续保持待办。

配置规划阶段的 [855 项构建输入](config-planning-source-sha256.json) 绑定本次全仓格式、测试、doctest、bench smoke 与 Clippy 检查。真实 TLS/请求回归共 40 项，其中 5 项覆盖双安全读草案与作用域补丁；Owner 回归另包含两份分别通过资格核验的旧/新资料。该阶段只生成内部草案，HTTP 持久发布仍待接入。[锚点审计](config-planning-anchor-audit.json)与[复现脚本](config-planning-anchor-audit.py)核对 25 份历史锁，没有本阶段源码重叠，也未执行 TLC。

计划归档阶段的 [868 项构建输入](config-archive-source-sha256.json)绑定全仓检查。归档流程使用真实 mTLS 主体、三份文件日志和实际提交回执；并发请求必须再经安全读取得首次记录。原 10 秒测试资料在逐条落盘时正确过期，归档专用 fixture 改用明确签名的 60 秒有效期和 30 秒 TLS 会话；生产期限与旧测试默认值保留。该阶段继续只读检查[历史锚点](config-archive-anchor-audit.json)，没有新增 TLC 结果。
