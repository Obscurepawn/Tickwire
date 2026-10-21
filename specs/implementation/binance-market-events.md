# 精确行情事件实现规格

实现入口见 接口。本阶段已接通以下七种输入，各自经过真实 TLS/WebSocket、类型解码、规范发布、共享内存、MarketReplica 和异构 JOIN 快照：Spot JSON Trade/BBA、Spot SBE Trade/BBA、USD-M JSON 聚合成交/BBA/MarkFunding。

## 协议和边界

- 价格与数量按可信 InstrumentBinding 的 tick/step 精确换算；非整倍数和整数溢出拒绝。源 symbol、product、catalog、source/connection 身份全部保留。
- 参考价格和资金费率使用 DecimalWire：`coefficient: [u8; 16]`、`exponent: i8`、七字节零填充，24 字节、8 字节对齐。尾数是有符号小端整数，指数 −38…38；零与可约去的尾零采用唯一表示。
- 市场消息 264/265/266 使用公共 128 字节头及 384 字节 MarketExactBody。旧消息 257–263 不变。资源、控制协商、SHM、snapshot builder/validator 和 Source 全部要求 feature bit 9。
- JSON Spot BBA 无交易所时间，保留缺失；SBE 时间是微秒并检查转纳秒乘法。聚合成交保留 aggregate ID、原 trade 范围、总数量和可选 non-RPI 数量。交割资金费空字段与数值零分别表示。
- 接收整帧 SBE 后先验证全部 rows；回调预算不足时保留帧内位置。未知模板、错误 schema/version、截断和 group 边界由固定 SBE codec 拒绝，不以 JSON 中转。
- 单 owner 固定产品/标的/类别。Connect、Upgrade、Subscribe、Pong 使用现有 PublicPermit；零接受重试重新申请。TLS 主机验证开启；SBE 只允许 SpotProduction、使用 Upgrade API key，不使用私钥。
- ExplicitSubscribe 以完整发送后的精确 null ACK 确认。EOF 与完整 ACK/数据同批时先失效，再交付已完整收到但 `usable=false` 的旧数据。I/O、消息解析、SBE rows、控制队列、接收时间片和通知数均有边界。

## 发布、时间与恢复

每标的保存 Quote、Trade、MarkFunding、FeedState 四个类别；旧/新编码同类不能重复。独立 BBA 配置替代深度派生 Quote，但不替换深度 levels 或 coverage。聚合 FeedState 要求配置的类别已可用，仍由真实 DepthSync 确定深度连续性。

每类事件保留自己的接收时间。FeedState 使用最近深度观测时间，其他事件不得刷新深度 age。MarketReplica 按全 topic 连续序号应用，身份/代次/序号或容量错误使视图失效；JOIN 仍从完整快照 S 接收 S+1，SBE depth20 不代替完整快照。

`TWMR0001` 检查点保存实际物化的全标的视图与 blocked 状态，最多 4096 标的、64 MiB，恢复要求同一最大深度容量。恢复校验版本、长度、尾 SHA256、所有状态记录语义、身份、排序、精确 coverage 和容量。快照内 state sequence 是 1 起行号，live state sequence 是原 topic 序号；`last_event` 单独保存最近 topic 事件位置。上层恢复闭包负责对象授权。

MD 默认 Feed.events 为空；显式事件配置绑定独立 owner/source identity，并同步扩展 InstrumentSpec.required_states 与 bank feature。策略实际消费的规范记录和 arena 输入沿既有 SDK 输入 WAL 保存，MD 不新增同步原始网络日志。

## 验证

原始证据包含先失败测试、修复结果、最终源码 SHA 与命令。旧 63 个独立契约向量保持不变，新 46 布局生成检查与独立字节样本覆盖 Exact 消息及发布控制 38–40；独立 256 字节样本覆盖新增 replay 控制 41/42 的身份、挑战、操作、对象和保留字节。

定向真实回归还检查了七类输入的来源时间和精确值、SHM 实际借用、后续完整 JOIN、检查点恢复，以及独立事件不能刷新深度年龄。既有 SBE 深度 mapped 测试继续运行。此阶段未新增 TLA；模块与全局模型在功能代码完成后统一更新。
