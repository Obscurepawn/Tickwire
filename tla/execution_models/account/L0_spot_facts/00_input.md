# 输入与范围

目标是 `tickwire-account` 的 Spot 预留及成交与费用记录顺序，模式为 faithful transcription。源码单所有者 `&mut self`，候选状态通过全部单位与身份校验后整体替换；模型不引入并行读改写。

模型采用完整有限输入实例：两个资产，单标的限价买单，原始数量 2、买价上界 2，接收币费率 1/2，最大成交次数 2，因此舍入尾额 2。每个订单两笔数量为 1 的成交、一个独立费用 ID；费用值包含 0 和超界值。输入域允许终态先到、成交/费用迟到、完全重复、内容冲突和中止后矛盾事实。

源码入口：`crates/tickwire-account/src/admission.rs:4`、`crates/tickwire-account/src/events.rs:3`、`crates/tickwire-account/src/calculation.rs:134`、`crates/tickwire-account/src/lib.rs:54`。账户初始无未导入存活订单，故 coverage 为 0，locked 为已解释非交易锁定。本级选 locked=0。

WAL 同步、目录资格、原始报文规范化、哈希和 USD-M 公式不在本级；调用者只把已持久化且身份已规范化的事实传给 reducer。实际超预算事实允许导致负余额并冻结，性质不假定余额永远非负。
