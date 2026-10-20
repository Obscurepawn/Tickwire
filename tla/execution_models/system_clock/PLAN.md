# 系统时钟形式阶梯

验证对象为 `SystemClock` 的构造、采样和永久故障，以及 `BinanceTimeClient::local` 接收时钟 provider 结果的顺序。一个实例由一个 owner 通过 `&mut self` 串行调用；移入 TimeClient 后，调用方不再直接访问该 SystemClock。

| 阶段 | 范围 | 状态 |
| --- | --- | --- |
| L0_native_owner | 首次探测、固定域、两种时钟回退、永久故障、provider 错误分类 | PASS，独立证据复核完成 |
| 后续：TimeClient 网络 L0 | HTTPS 采样令牌、首次接受、响应关联、超时与校准交付 | 待规划 |
| 后续：私有流时钟 adapter L0 | 原始 SignedTime、额度回调前后采样、预算与私有流首次接受组合 | 待规划 |

L0 使用小整数表示时间读数的先后关系，数值不表示生产纳秒上限。`timespec` 校验与 u64 边界由现有 Rust 测试覆盖；校准区间数学属于 `clock/L0_calibration`。系统挂起是否计时依据已选 OS API 的契约，本级不添加时钟重置、故障恢复或 owner 重建动作。
