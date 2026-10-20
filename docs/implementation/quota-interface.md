# 共享请求额度与本地消费

`tickwire-quota` 把共享 IP、UID、账户及连接额度分成 supervisor 分配和 exec 本地消费两步。请求可能同时消耗多个桶；分配和扣减均先完整检查，成功后一起更新。该账本不表示账户资金或订单风险额度。

每个权威命名空间只有一个持久 Store；多个协调器可以竞争同一 Store 的比较交换，不能另造一个独立 Store 重发同一份额度。消费者和请求 producer 身份由 supervisor 唯一分配，重复部署身份不是本地内核能够自行验证的事实。

`BucketId` 明确权威命名空间、共享主体类型、主体 ID 和池 ID。REST 与 WS 的重叠接口使用相同桶；没有重叠的接口使用不同桶。产品名称本身不会创建独立额度。`WindowKey` 在桶内有不可复用的窗口 ID，`WindowConfig` 包含配置版本、限额、恢复保留额和本地有效区间。

窗口配置是可信控制面的输入：其本地时间区间必须保守落在交易所计数窗口内，已扣除时间误差与交付延迟。核心不把“从进程启动经过一分钟”当作交易所分钟窗口，也不硬编码官方示例中的额度。当前实现要求协调器和消费者使用同一 `ClockDomain`，异域输入直接拒绝；跨主机期限映射由后续控制面适配器提供。

```rust
pub trait QuotaStore {
    fn load(&mut self) -> Result<Snapshot>;
    fn compare_exchange(&mut self, expected_revision: u64, next: &Snapshot) -> Result<()>;
}
pub struct Coordinator<S: QuotaStore> { /* 私有状态 */ }
pub struct ExecLimiter { /* 唯一所有者；不可 Clone */ }
pub struct Grant { /* 私有构造；不可 Clone */ }
pub struct StoppedReceipt { /* 私有构造；不可 Clone */ }
```

`Coordinator::open` 用更高的 supervisor 代次持久登记本次所有者。Store 的比较交换必须原子、持久，并且只有完成同步后才返回成功；失败或回执不确定会停止该协调器，调用方重新打开并读取已经保存的占用。`MockQuotaStore` 明确模拟此契约，可注入提交前和提交后失败；本阶段没有 SQL、Raft 或文件持久化实现。

`register_consumer` 在 Store 中登记实例 ID、递增的 exec 代次、单调域和租约期限后，返回唯一 `ExecLimiter`。恢复不能再次取得旧代次的本地消费状态。`issue` 持久占用完整分配量后才返回 Grant；相同发放请求再次调用返回 AlreadyIssued，不创建第二份可用对象。supervisor 或 exec 重启后，旧 Grant 全量占用仍保留。

同域重新打开还须满足保存的单调时间下界；更换域时拒绝沿用旧窗口映射。健康消费者可以通过持久登记后返回的私有 `LeaseRenewal` 延长租约，但旧 Grant 的原到期时间不变。续约到达时旧租约已经过期就拒绝，不能复活失联消费者。

Grant 同时绑定 supervisor 代次、消费者、grant_id、类别、各窗口分配量和到期时间。`ExecLimiter::install` 消耗 Grant 的 Rust 所有权，核对身份并拒绝重复。不可 Clone 只防止本进程 API 复制；跨进程导入还需要已认证的控制连接、唯一消费者登记及持久去重，当前没有把 Rust 所有权称为跨进程证明。

`consume(attempt_id, digest, class, costs, now)` 先检查单调域、回退、租约、窗口、授权到期和冷却，再完整检查各桶余额。成功后一起扣减，并保存请求摘要和确切费用列表；同一次尝试的精确重试不重复扣减，但仍重新检查时间与冷却。相同 attempt_id 的不同内容使消费者冻结。失败、超时、NoBytes 和结果未知都不退回已消费额度。

`AttemptId` 包含 supervisor 分配的 producer ID 和该请求所有者的 sequence。时间查询、私有控制及交易命令不能把各自从一开始的计数直接放到同一个裸整数空间。

Normal 分配只能使用 `limit - recovery_reserve`，Recovery 分配与 Normal 共用总上限。恢复保留额由可信请求分类器用于撤单、查询、心跳及恢复；核心不根据任意字符串猜测类别。一个请求缺少任何必需桶都不能放行，完整费用表由端点适配器负责提供。空表拒绝；零费用接口仍需适配器列出其他适用的连接、原始请求或订单维度。

`stop(self)` 先终止本地消费，再产生最终已消费记录。协调器只凭匹配的 StoppedReceipt 回收已安装 Grant 的明确未用部分；已消费量仍留在该窗口。过期、失联和缺少回执不构成回收证明。尚未安装但已发出的 Grant 保持全额占用。

`cooldown` 单调延长本地禁止消费时间，`freeze` 永久关闭当前消费者；429/418 及权威使用量异常由外层解析后调用。某消费者收到响应不能瞬时停止其他进程，控制面须传播停止信息，未收到的消费者最多使用既有租约内的授权。窗口配置更改要求先停止相关消费者，本模块不原地放宽正在使用的窗口。

容量有硬上限：256 个窗口、256 个消费者身份、4096 次发放、每个请求 16 个桶、本地 4096 条尝试记录。容量不足返回错误，不驱逐仍可能重试的记录。当前先交付有界所有者生命周期；长期压缩、跨进程消息编码和具体 Binance Permit 接线继续单独实现。

协议依据：Spot 的多个计数区间、共享 IP 权重与账户订单计数见[固定版本官方文档](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-api.md#rate-limits)；Futures 的共享池和重叠接口见[官方 WS API 规则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-api-general-info)。实现只接受明确配置，不把这些页面的示例数值作为默认许可。
