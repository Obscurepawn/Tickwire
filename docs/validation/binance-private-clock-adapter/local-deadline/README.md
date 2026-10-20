# 私有请求本地截止修复

`red.log` 记录编译成功后的实际回归：第一次预算回调推进时间的对照通过；第二次回调推进时间时，对端收到219字节，结果为 MayHaveApplied，违背 NoBytes 断言。`green.log` 记录最小修复后两项均为0字节/NoBytes。

校准和订阅通过真实本地 HTTPS/WSS。请求截止为1秒，签名窗口为5秒，校准有效期为10秒；预算回调只将共享 Mono 推进到2秒，Instant 未到传输截止。

修复在最后 check_clock 返回后再次采样，随后仅检查时钟域、顺序、原请求 fresh 和本地 deadline，没有外部回调。原 SignedTime 及原回退分类保持。root 与 risk_core 独立核对源码和实际 Red/Green 后确认，无开放候选；独立审阅未重复运行测试。

`native-final.log`：private41项与adapter12项，共53项通过。`native-clippy.log`：两包所有目标通过。修前、修后哈希分别在 red-source-hashes.json 和 green-source-hashes.json；pre-fix 保存实际修前源码。fixture-count 日志保留最初多写的回调次数断言，清理该测试假设后的 red.log 是最终 Red 依据。

v2有限模型位于 `tla/execution_models/binance_private_session/L0_session_lifecycle`。旧v1完整源码及证据归档在 `docs/validation/binance-private-session/pre-final-gate-v2`。v1没有区分预算回调前后两个时间样本，其 PASS 未覆盖本缺陷。
