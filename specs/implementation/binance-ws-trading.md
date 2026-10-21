# WS 交易 owner 实现规格

当前代码以 `tickwire-binance-account::rest::RestOwner` 统一持有 REST/WS 交易责任。协议接口和配置见WS 接口。

## 持久身份与输出

- 输入 16/17/18/19/20 标签分别使用独立版本化编码，解码校验长度并重新编码核对规范字节。Binding 的原命令、输入位置、writer、会话、账户原收据/封口和 HistoryFrontier 完整核对。
- 57 字节 ID 无损编码输入 stream、writer、连接代次和实际 Binding 序号。新连接先同步会话代次；旧 owner Drop/撤销使旧资格失效。恢复不重新签发资格。
- 原 JSON/掩码帧和 `SignedTime` 在同一有界共享状态保存。首次应用输出需实际 `DurableWsBinding` 与缓存帧摘要相等，且实际账户收据与该绑定相等。
- `AwaitingCommit` 保留账户责任。SyncReplica 的本地 Admit 收据先于 ACK 可读取；Binding 输入同步先于输出，原 Admit 恢复闭包不反向引用后续 Binding。
- Gateway 的本地零接受分支仍先验证候选 Aborted、同步账户 WAL，再释放。断线、错误 ID、重开后的旧意图均不走该本地证明分支。

## 实际连接与恢复

Gateway 的协议控制槽只接受固定 Upgrade 和 Ping/Pong/Close 类型。升级校验成功才开放业务提交；帧头/掩码首次被 TLS writer 接受就计为业务 MayHaveSent。控制槽在业务帧边界使用，Close 无法在该边界发送时关闭连接并保持责任。

响应先同步 `WsResponse + WsEffect[]`，再逐条写账户 `IngressEvent`。完整原文在恢复时重新解析并核对已记录效果。已经匹配的历史 ID 再出现时记 Duplicate；错误 ID、畸形/不完整响应保存原文并隔离。订单累计量仅生成 Order 观察，不推算成交/手续费。429/418 冻结共享额度，鉴权和时钟错误停止会话。

输入 owner 的 `paired` 校验将所有 Binding 关联到实际账户原 Admit/原封口。双 WAL 中断补写使用原输入 key，仅补缺失账户记录。恢复后的业务请求查询可返回 Unknown，但 `CommittedAdmission` 和连接都不存在。运行时按已验证 Reactivated 资格重新创建实际 WS 连接，查询仍使用独立 REST 地址；没有回退重发旧请求的入口。

## 复制与配置

`SourceWorker` 的私有 `LocalPrefixProof.dependencies(sequence)` 来自实际已封存 Entry；app 在公开 Ready 前核对依赖根中的 Binding 输入位置和 SHA256。旧 locator 不得因后续输入增长而改变。镜像 ACK 证明其持久前缀和完整对象闭包，不提供旧执行者隔离资格。

部署的顶层 `trading_websocket` 与 `network.trading_websocket` 同时配置，缺一拒绝；均省略保留 REST。`network.rest` 永远用于只读对账。实际签名时间源、环境、产品、authority 和凭据引用由部署与已加载凭据固定。共享额度规则必须显式提供 WS 应用与会话控制动作。

## 定向验证

验证记录按阶段保留实际 Red/Green，包含真实 FileStore 的输入/账户 WAL、原始响应同步后账户 torn append 的重开补写、本地 Spot/USD-M 双 TLS 端点，以及实际双向证书 TLS 镜像与两种提交模式。策略授权在这些连接测试中使用显式测试 Authority；复制资格使用真实 SourceWorker/FileMirror 私有 Evidence。

完整 List/Algo/Amend 风险与生命周期、session.logon、计划无冻结换线尚未在本实现提供。模型同步和形式验证沿全仓安排，在功能代码完成后进行。
