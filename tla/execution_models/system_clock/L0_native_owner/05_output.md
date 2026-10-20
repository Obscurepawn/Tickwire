# L0 输出

四个有限配置通过 14 个不变量。结果覆盖 `SystemClock::new/sample`、原生错误到 `MonotonicClock` 的映射，以及无校准 anchor 时 TimeClient 的 provider→local→public sample 顺序。

在本级输入域中，首次探测失败不会发布 owner；成功构造后的域固定；成功采样的两种读数不回退，相等值允许。原生读数或系统调用失败保留上次成功读数，并使后续原生采样拒绝。

移入 TimeClient 后，没有对同一 SystemClock 的直接调用。provider 五类永久时钟错误被外层 fatal 保存，未来调用不再访问 provider。校准器故障单独保留：外层没有 fatal 时仍可能调用 provider并更新 lastInstant，但 core 拒绝后不会返回成功读数。暂时 provider 错误没有被错误归为永久故障。

## 忠实度与覆盖

模型转录原生构造/Guard/sample、provider 映射、local 和 public sample，共 7 个相关函数/方法；另转录校准器无 anchor 子集的 observe/read。timespec 算术只输入实际错误分类，由 Rust 边界测试验证。TimeClient 构造配置固定为有效值，完整校准、HTTP、签名窗口和额度回调不在本级范围。

四个配置分别为 3,021、2,431、18,714、2 个不同状态，深度 7、7、9、2。跨配置覆盖并集包含 20 个 Next 动作，全部 generated 大于 0；实际结果见 [03_run_log](03_run_log.md)和[results](04_evidence/results.json)。读数域只保留有限偏序，调用次数是外部长度限制；本结论没有缩小或替代生产 u64 最大值。

root 已审 00/01 范围，market_core 已独立核对源码与范围，risk_core 已核对模型核心与源码，均无开放阻断项；两位审阅者未重跑 TLC。risk_core 又独立逐项核对了 7 个源码/测试、model/jar/四配置/四日志 SHA、231 个代码锚点及每份末覆盖块；数值与 results 完全一致。

## 后续使用

后续模型可使用固定域构造、成功采样顺序和永久错误传播这三个契约。系统挂起的计时行为来自接口所列 OS API；校准区间数学继续由 clock 阶梯处理。完整 TimeClient 网络 L0 和私有流时钟 adapter L0 已保留在本阶梯 PLAN/ROADMAP，当前结果不替代这两项组合验证。
