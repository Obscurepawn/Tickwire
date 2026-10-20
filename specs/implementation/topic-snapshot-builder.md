# TopicSnapshot 构造规格

`tickwire-snapshot::build_topic` 已实现完整 NormalizedMarket TopicSnapshotManifest 构造，返回拥有全部字节、资源头、快照头与最终 JOIN 绑定的 `BuiltTopic`。接口见[构造接口](../../docs/implementation/topic-snapshot-builder-interface.md)，实际 Red/Green 和摘要见[验证报告](../../docs/validation/topic-snapshot-builder/report.md)。

构造遵守以下行为：

1. 登记集合为 1–4096 项，成员与登记的 instrument/version 一一相同、严格升序；snapshot ID 非零且全对象唯一。成员必须声明同一 S、feed generation、catalog version/hash，最后实际事件序号不超过 S。S 为 0 可用于初始非 Live 基线；S 最大为 2^63−2，保证 J=S+1 仍未耗尽。
2. 先计算清单行、清单页尾、全部子对象和子页尾的完整大小，拒绝任何溢出或超出 `byte_limit`/64 MiB 的输入；不把每个成员分别当作独立 64 MiB 预算。输入身份、时间、状态、深度都检查完后才分配完整输出。
3. 每个成员保存 1–4 条唯一状态，FeedState 必需，Quote/Trade/MarkFunding 按输入保留。按 message_type 排序，重新分配快照行号并重算启用的 record CRC，保留观测时间与业务状态。原始行序号不得超过成员最后事件序号。缺必需态、Bootstrapping/Invalid 等成员保持存在，构造成功也可以 `check_readiness` 为 NotReady。
4. 规范深度中的价位和数量均为正，买降卖升，不自动排序、去重或修正输入。Live 状态拒绝交叉簿；coverage_depth/flags 保留调用者知识声明，非法位拒绝，不扩大已知范围。
5. 子对象为 kind=6：256 字节头、两个 64 字节 section、状态行、深度层和零页尾。section offset 为文件绝对位置，hash 顺序包含数据、描述符、完整子页、清单行、置零摘要字段的资源头和完整文件。整个对象与独立 12 KiB 基准样本逐字节相同。
6. 只计算公开约定的布局字段和 JOIN 派生绑定；channel/feed/catalog/membership/subscription/bank/cursor 身份冲突拒绝。已有非零 snapshot/set 绑定必须与结果一致。`ResourceHeader.first_seq` 保留输入模板的 bank F，builder 不把它改成 JOIN 的 J。
7. 返回前通过既有 `validate`，不调用 `check_readiness` 拒绝非 Live 基线。输入均为共享借用，失败没有可交付半成品，也不修改 ring、pending gate、cursor 或原 JOIN。

本机验证为 1 个算术边界单元测试、17 个构造集成测试和 12 个既有接收测试，共 30 项；格式、Clippy 和文档测试入口通过。初始 Red 是编译成功后的真实断言失败，展开后的 14 项测试也全部失败，再实现得到 Green；其后三项补查覆盖 4096 成员、累计预算及资源配置边界。Linux aarch64 上同样的 30 项测试、既有跨进程快照接收的 2 个入口（含 helper）及 Clippy 通过。

owner 必须在同一发布边界冻结全部成员输入并先保留 pending gate。构造器验证这些声明相同，实际缓存捕获的一致性由 owner 的独占调用保证。预算以外的 owner 存活对象配额、memfd 封印、交付、ReadReady/JOIN_READY 及交易准入由相应组件承担。
