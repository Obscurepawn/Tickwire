# 固定记录共享 ring

Status: implemented-partial

接口由 `tickwire-shm` 实现；[映射层](shm-mapping.md)负责 Linux memfd、权限、seals 和原子访问。一个发布者可对应 1～32 个消费者，单消费者使用同一套容量与借用规则。

## 状态与数据约束

每个 bank 使用不可变资源身份与代次。JOIN 在当前发布边界登记 pending 消费者，立即将其纳入容量门禁；消费者 Ready 前，发布者保留初始释放基线。成员变更清空最小释放水位缓存，普通 cursor 更新只让缓存保持为保守下界。只有整批最后序号与最小释放水位之差不超过容量时，才允许取得 claim。

`Publisher::publish` 预检整批固定长度及连续序号，取得唯一 claim，写完全部槽位，再 release-store 发布水位。消费者 acquire-load 该水位后开始借用。共享原子字段始终通过对齐的 64 位原子访问；普通字节引用只覆盖已发布、尚未释放的记录。

`ReadLease` 和 `RecordBorrow` 不可复制。guard 的字节引用无法逃逸其生命周期；析构时记录该序号释放完成，再推进连续 `released` 前缀。`applied` 记录业务处理的独立连续前缀。容量 B 限制同时尚未完成这两件事的记录数量，防止乱序完成表提前复用。`mem::forget` 保留占用，Quiesce 在仍有借用时拒绝完成。

guard 内部保存 `NonNull<[u8]>`，仅在 `bytes(&self)` 中生成与本次借用同寿命的引用。若字段直接保存长寿命 `&[u8]`，将 guard 按值传入函数后，在函数内先 drop、再允许发布者复用槽位，会违背该引用覆盖整个调用期的约束。这一点由独立源码审阅与 [Rust 引用规则](https://doc.rust-lang.org/reference/behavior-considered-undefined.html#undefined.alias)确认，修法与[标准库读锁 guard](https://doc.rust-lang.org/stable/src/std/sync/nonpoison/rwlock.rs.html#75-84)相同；没有把它记作 TLC、Miri 或实际误编译发现。

退出时先关闭新借用，再完成所有 guard，最后写入实际 cursor 的 Quiesced 状态。发布者必须观察到该状态，且 read、applied、released、quiesced_seq 与收到的证明精确匹配，才能删除成员。纯状态内核的证明可由其他 `Reader` 构造，因此映射层不会仅凭此值释放实际消费者的容量门禁。

Prepare 固定 successor/cutoff；Freeze 要求没有 claim 且恰好发布到 cutoff。冻结后的 bank 保持只读数据，现有 guard 仍可读取，后续发布被拒绝。跨 bank 切换还需后续控制模块建立 successor 及快照关系。

## 验证记录

按接口先编写协议测试，再实现状态内核和实际映射 SDK。实际失败记录保存在 shm 验证目录：

| 回归 | 修复前现象 | 当前行为 |
|---|---|---|
| 独立纯 `Reader` 构造退出证明 | 实际 guard 存活时也能 detach | 必须验证登记的实际 cursor 已完成 Quiesce |
| 必选特性与代次边界 | 接受当前 SDK 不支持的必选特性或越界代次 | 建立映射前拒绝 |
| 重复 `start_quiesce` | Quiesced 被降回 Quiescing，detach 失败 | 保持 Quiesced，重复调用幂等 |

Rust 测试覆盖满/空/环绕、pending JOIN、最慢消费者、缓存失效、批次原子发布、乱序释放/应用、身份拒绝、冻结与序号上限。编译失败 doctest 验证共享引用不能逃逸 guard。Linux 子进程测试分别运行 1/2/4/8/16/32 个消费者，每个读取 1,024 条记录，逐条检查序号及全部 512 字节内容，完成 Quiesce 后退出。实际 `detach` 的成功与拒绝由同进程映射测试验证。

多进程测试使用 `/proc/<pid>/fd` 传递测试资源，以管道确认登记与 Ready。测试完成点是所有消费者校验、释放、应用、Quiesce 并退出，未采集性能分位数。`./scripts/check-linux.sh` 固定容器镜像摘要、Rust 版本并归档内核信息；macOS 执行状态测试与不支持平台的拒绝测试。

TLA+ 模型与源码锚点保存于 [broadcast_ring](../../tla/execution_models/broadcast_ring/PLAN.md)，对有限消费者、序号和借用窗口检查发布、释放、回收及 Freeze 交错。它依赖映射层提供的 release/acquire 顺序，不建模 CPU 的弱内存行为。

## 当前接入范围

SDK 验证资源头、descriptor hash、精确大小、架构与固定记录布局；必选特性固定为 `0x1c`。完整业务消息仍需 `tickwire-ipc` 校验，生产连接还需控制模块完成对端认证、FD 交付、namespace/host/topic/catalog 身份准入与 snapshot ReadReady。两个不安全接入函数明确要求调用者先建立可信身份及单写、借用约束。

当前完成固定记录广播与单消费者传输、受保护借用和协作退出。Arena payload、CRC 必选特性、完整 bank 轮换、pidfd/holder registry、真实 md/strategy/exec 业务进程及生产缓存行/NUMA 资格仍保留在 P1/P5。
