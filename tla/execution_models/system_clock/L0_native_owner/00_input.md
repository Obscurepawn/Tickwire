# L0 输入

本级验证系统时钟 owner 在失败后是否仍能交付读数，以及外层 TimeClient 是否保留 provider 报告的永久故障。生产源码已冻结；本级不修改源码。

| 源码 | 核对内容 |
| --- | --- |
| `crates/tickwire-clock-sys/src/lib.rs:28-50` | `Guard` 只保存上次成功的 Instant/native 读数；任一回退使故障永久保留，相等值允许 |
| `crates/tickwire-clock-sys/src/lib.rs:52-85` | 构造器先采样后返回 owner；固定 ClockDomain；故障后直接拒绝；先读 Instant，再读 native |
| `crates/tickwire-clock-sys/src/lib.rs:87-118` | timespec 字段检查、checked 乘加、系统调用失败及平台不支持 |
| `crates/tickwire-binance-clock/src/lib.rs:41-55` | 原生错误映射：Backwards→TimeBackwards，Overflow→Overflow，其余→Faulted |
| `crates/tickwire-binance-clock/src/client.rs:33-107` | 有效配置构造、单 owner、provider 永久错误分类、Instant 回退、校准器读取 |
| `crates/tickwire-binance-clock/src/client.rs:378-388` | 公共 sample 经 local 后获取签名；未校准时仍可返回 local，signed=None |
| `crates/tickwire-clock/src/lib.rs:200-211`、`crates/tickwire-clock/src/lib.rs:295-299` | 本级无校准样本时的域/单调顺序检查与 Uncalibrated 返回 |

调用方提供有效且固定的 `ClockDomain`。原生构造时的首次系统调用失败会使 `new` 返回错误，调用方得不到可采样对象。移入 TimeClient 后，只通过 TimeClient 访问 provider。本级不把这两个可变借用建成并发线程。

配置分为原生 owner 与通用 provider 两种场景。原生场景从真实 `SystemClock::new` 开始，可直接采样，再把对象移入配置有效的 TimeClient。通用场景使用实际 `MonotonicClock` trait 允许的返回值，覆盖 Domain、TimeBackwards、Overflow、ClockJump、Faulted 五类永久错误和暂时错误。两种场景不会让原生 provider 任意返回其实现中不存在的错误类别。

读数取有限非负整数，只验证比较、保存和错误顺序；调用次数是外部有限执行长度。`timespec` 的符号、纳秒字段范围和 u64 最大值转换由现有 7 项 Rust 测试负责。模型不声称穷举该算术，也不使用小整数代替实际 u64 溢出边界。

已验证接口与实际检查材料分别位于 [系统时钟接口](../../../../docs/implementation/system-clock-interface.md)和[系统时钟验证目录](../../../../docs/validation/system-clock/)。系统挂起计时来自接口中已核对的 Linux/macOS API 契约；TLC 不模拟挂起或改变时钟源。
