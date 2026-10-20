# L0 状态含义与边界

本级检查一个 bank 的槽位从写入到借用、释放和再次写入的全过程。`published` 表示完整 payload 的发布边界；`read` 表示消费者已经取得的借用前缀；`released` 表示引用已经结束的连续前缀；`applied` 表示业务处理完成的连续前缀。二者独立推进，只有 released 参与发布者回收，借用窗口使用二者较小值。

`p` 保存发布状态、claim、Freeze 计划和缓存最小值。`m[r]` 保存 pending/active/detached、JOIN 基线与发布者已观察的 released。`rdr[r]` 保存消费者的有限完成数组、活跃借用、实际共享游标 lifecycle、quiesced_seq 和已签发证据。`payload` 是 64 个物理槽位当前存储的序号；不变量直接检查活跃借用对应的槽内容，避免只检验水位标签。

发布者方法由 `&mut self` 串行调用。claim 到 publish 期间，消费者仍能借用已发布记录并释放引用；模型不允许另一个发布者控制方法重入。`Observe` 对应状态内核的 `observe_release`，模型将它限定在 claim 外，这是 SDK 同步串行调用域的选择：纯 `Producer::observe_release` 本身没有 boundary guard。该方法只增加 member 已观察水位，不修改当前 claim 或 cache；把 claim 期间的观察延后到 publish/abort 之后，只会保持更保守的采样，不会减弱已持有引用的保护。SDK 的 publish 在 claim 前逐一 refresh 所有已登记 cursor；本级允许调用者较少观察，覆盖状态内核合法但更保守的准入。

写入一次只更新一个槽，完整批次写完后才允许 Release 发布。SDK 的 Release store 与紧随其后的本地 `protocol.publish` 合并为一个线性化动作：两者之间没有另一个发布者方法能运行，读者只看共享发布值。消费者的借用/释放方法也按对其他参与者有效的共享原子 store 线性化；取得引用到 store read 的间隙仍被旧 released gate 保护。`Abort` 对应公开 `Producer::abort`，保留已写但未发布的字节；已验证布局的 SDK 正常写入不会主动取消。

## 可执行范围

| 配置 | 初态 | 输入限制 | 目标 |
| --- | --- | --- | --- |
| primary | 已 create/activate，published=0 | 1 读者、窗口 2、发布至 3 | JOIN、乱序完成、Freeze 和 detach |
| two_readers | 同上 | 2 读者、窗口 1、发布至 2 | 成员加入、观察与退出组合；实际满环见 wrap_two |
| wrap | published=64、活跃读者 baseline=0 | 1 读者、窗口 2、读前 2 条、发布至 66 | 满环、缓存刷新与 65/66 覆盖物理槽 0/1 |
| pending_wrap | published=64、pending 读者 baseline=0 | 同 wrap | 未登记 cursor 仍阻止复用 |
| wrap_two | published=64、两个活跃读者 baseline=0 | 窗口 1、各读第 1 条、发布至 65 | 两读者全部释放后才可覆盖槽 0 |

所有配置的物理容量均为 64。三个 wrap 初态来自真实可达前缀：create(capacity64, first1) → activate → JOIN(baseline0) → 可选 register_cursor → claim(64) → 按顺序写 1..64 → publish(64)。wrap_two 在 claim 前分别 JOIN 并登记两个互异消费者，二者 baseline 均为 0。初始化直接赋值这一前缀的终态；没有假设已释放任何记录。初始 payload[i]=i+1，cache=0，未完成借用为空。这三个配置只限制后续输入，未把容量换成 2。已完成 JOIN/ReadReady 的前缀配置使用专门的 Next，不把前缀中已完成的动作列为后缀覆盖。

普通配置允许批量 1/2 条，wrap 前缀使用合法最大批 64 条。固定身份、至多一次加入、有限序号是穷举输入边界，不是完整生产域证明。TLC 的 Java 整数无法直接计算 `2^63`；所选序号最多 66、membership 最多 5，`FiniteU64` 用这些数值全部满足的有限范围保留可表示性 guard，不声明检查生产数值上限。身份、页权限、数值上限和跨 bank 迁移由后续级与 Rust 测试覆盖。

错误方法通常不改变协议状态；两处真实副作用被保留：Full 会刷新 cache，BorrowCapacity/读到末尾也会更新 observed_published。已释放或已应用完成数组只有被新借用覆盖才消失，apply 的幂等前缀不回退。停止进度与忘记 guard 由任意停顿表示，本级不检查活性。

伪造证据使用一个独立纯 Reader 在 JOIN baseline 上产生的合法 `Quiesced`。`Detach` 同时枚举实际签发证据和该伪造证据，仍要求真实已登记 cursor 的 lifecycle=5、三水位精确匹配、quiesced_seq=read，再执行状态内核的边界和水位检查。`RejectForged` 只记录观察到的错误返回，便于证明这一拒绝分支实际可达；它不改变生产状态。

同一 Release/Acquire 位置的旧样本被抽象为延迟 `Observe`：旧值至多低于当前已释放前缀，只能更保守。唯一未完成 claim 的私有 serial 不存入模型；Rust 的 affine `WriteClaim` 不能复制，模型也只保留一个 claim，并在 publish/abort 时消费。超出有限输入域后的 serial 耗尽、任意伪造水位、身份重用和错误 FD 绑定未在本级穷举。
