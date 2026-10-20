# 输入与范围

目标为冻结的 `crates/tickwire-clock/src/lib.rs`，SHA256 `14cb9c43701fec8ea9339a5009b497bc0e629a9bae8aa3d8f22caa8a5cd0ffb2`；行为测试 `crates/tickwire-clock/tests/calibration.rs` SHA256 `e2a9b1b266acb78d5a68f8ffb68ff45ee818b8006ba701858f88d9f03788ca40`。核心 25 项测试及 Clippy 的实际结果来自 `docs/validation/clock/core-results.json`。

源码入口与完成点：构造与状态 `crates/tickwire-clock/src/lib.rs:144-212`；采样与取消 `crates/tickwire-clock/src/lib.rs:214-293`；读取和签名 `crates/tickwire-clock/src/lib.rs:295-326`；发送检查 `crates/tickwire-clock/src/lib.rs:328-372`；状态及整数公式 `crates/tickwire-clock/src/lib.rs:374-409`。

本级按单 owner 转录方法，没有两个公开调用交错执行。产品、环境、ClockDomain 在实例创建后不变；外部旧令牌或旧签名可以作为参数输入。故障后同一实例无重置入口。已结束的令牌不复用；旧签名可跨正常校准刷新按原时间戳重新检查。

服务端真值在隐藏采样时刻 `t_s∈[m0,m1]` 量化成报文毫秒值；之后按允许的连续速率推进。上下速率与中性速率均可选择。另行注入的跳变或不满足关联契约的输入，只检查拒绝和撤销行为；不对它们继续声称原区间包含真值。

SECOND=PPB=1e9，毫秒=1e6，微秒=1e3，u64 上限保持真实值。有限配置直接选择真实纳秒值集合及边界值；调用次数上限仅限制探索长度，不表示生产容量或触发虚构的溢出。drift 选择 0、1ppb 与1e6ppb 的实际有理比率。

`new` 的 `first_serial` 是调用者提供的非零输入（`crates/tickwire-clock/src/lib.rs:166-190`）。边界配置可直接从 `first_serial=u64::MAX−1`、`last_seen=None` 构造，不依赖未建模的先前调用。

四配置的量化误差均为1毫秒。来源1、2分别代表选定环境的 Spot 和 USD-M；外部来源3及域2只作为身份不相等的见证。枚举的调用序列有明确上限，末状态允许停止，因此本级检查安全性质，不检查请求最终完成或持续可用性。
