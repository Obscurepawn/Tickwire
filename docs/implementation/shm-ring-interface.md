# 共享 ring 的发布与借用接口

`tickwire-shm` 保存单个 bank 的发布、成员和借用状态，并用 `Publisher`、`Subscriber` 连接 Linux 映射。`tickwire-shm-sys` 负责操作系统资源；固定字节布局由 `tickwire-ipc` 负责。发布者先取得槽位写权限，写完全部字节，再发布；读者观察发布后借用，结束访问后释放。纯状态内核不直接访问共享字节。

## 所有权与完成点

`Producer<M>` 由唯一发布线程持有，最多登记 M 个消费者。`claim(count)` 检查整个批次的容量并返回不可复制的 `WriteClaim`；同一 bank 至多有一个未结束的 claim。`publish(claim)` 将连续完整前缀推进到批次尾，`abort(claim)` 放弃整个未发布批次。丢弃 claim 不会自动发布；显式 abort 前不再接受写入或 Freeze。

成员按 bank、channel、ring、feed 代次及 instance、subscription generation、独立 cursor 绑定。JOIN 在发布边界登记当前 `published` 作为 pending 成员的 gating 基线；Ready 前忽略普通 cursor 的推进。Active 成员的释放水位只能单调推进且不超过该 bank 的发布水位。最小 gating 缓存始终是保守下界，成员变化使缓存失效。

`Reader<B>` 是消费者的单写状态，最多跟踪 B 条尚未同时完成应用与释放的记录。借用返回不可复制的 `ReadLease`。多个 lease 可以乱序结束，但只有连续前缀推进 `released`；业务应用单独推进 `applied`。因此复制完成后允许 `released > applied`。这两个水位不能互相代用。

Quiesce 先关闭新借用，再等待全部 lease 释放；返回的证明精确绑定订阅及最后 read/released/applied。纯 `Producer` 接受经调用者认证的证明。实际 `Publisher::detach` 还会读取已登记 cursor 的 Quiesced 生命周期及最终水位，并与证明逐项核对，防止独立构造的纯状态证明绕过实际借用。心跳、超时和撤销交易授权没有 detach API。进程退出回收留待 pidfd/holder registry 集成。

Prepare 固定 successor 与 cutoff，超过 cutoff 的批次不再获准。Freeze 要求没有未结束 claim 且 `published == cutoff`；结果绑定前后代次、cutoff 和 membership version。Frozen bank 永远不能重新发布。

## Linux 映射 SDK

`Publisher<M>::create(header)` 建立唯一可写 bank、初始化发布页和不可变头、计算 descriptor hash，再锁定 seals。`join(key)` 先登记 pending 成员并返回首个受保护序号 J；消费者以 J−1 为基线调用 `Subscriber<B>::attach`，创建自己的唯一可写 cursor。发布者通过 `register_cursor` 验证 cursor 头与订阅身份，才把成员推进到 Ready。bank 对消费者只读，cursor 对发布者只读。

`attach` 与 `register_cursor` 是两个 `unsafe` 准入边界：调用者负责认证对端、精确绑定 FD 和订阅，并确保对端遵守单写、头部不变及借用协议。后续读写方法为安全 API。生产控制连接、`SCM_RIGHTS` 交付及 snapshot ReadReady 由后续控制模块实现；当前多进程测试用明确的父子关系和管道握手建立这些前提。

`publish(records)` 先验证整批长度与连续序号，再刷新消费者释放水位、取得 claim、写槽位，最后 release-store 批次尾序号。`borrow()` acquire-load 发布水位后返回 `RecordBorrow`；guard 内部保存指针，`bytes()` 才生成绑定本次 guard 借用期的引用，避免按值传参后提前析构与长寿命引用冲突。guard 析构只推进已经连续结束的释放前缀，`apply(sequence)` 单独推进业务应用前缀。遗忘 guard 会使对应槽位持续受保护。

`start_quiesce()` 具有幂等性，完成 Quiesced 后重复调用也保持完成状态。`quiesced()` 将最终水位写入 cursor 后发布生命周期；调用者若随后推进 applied，需要重新取得证明再 detach。

## 边界与验证

生产容量遵守 ABI：64 至 1,048,576，二次幂；每批 1 至 64 条，序号严格小于 `2^63`。状态内核操作有界、无堆分配。映射 SDK 当前传输 512/1024 字节的固定记录，检查记录大小与序号；完整消息语义交给 IPC codec。Arena、CRC 必选特性、bank 切换接续和进程死亡回收尚待实现。

测试约束满/空/环绕、pending JOIN、成员缓存、乱序释放与应用、迟到旧代次证明、冻结和序号上限。Linux 集成测试实际运行 1/2/4/8/16/32 个消费者进程，覆盖槽位复用和逐字节校验；[实现规格](../../specs/implementation/shm-ring.md)记录测试与模型边界。
