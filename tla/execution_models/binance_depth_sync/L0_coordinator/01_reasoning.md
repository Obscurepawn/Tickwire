# L0 建模依据

采用源码状态迁移加受支持的失败输入：HTTP 失败/延迟、重复旧回调、缓冲溢出、无效消息、序列缺口和当前时钟回退。网络不被假定总成功；本层没有自造崩溃持久化语义，协调器本身没有持久队列。

一个动作对应一次完成的公共 API 调用。`on_snapshot` 内递归回放 FIFO，完整成功才产生可观察状态；中途 Book 已 Live 后遇到错误仍在该调用内 reset。Book 与缓存始终私有，安全 Rust 的独占借用使外部无法同时持有可变 owner 并读取中间 view。

固定 BookKey、symbol、目录与产品，令牌只保留会变化的连接 epoch、Book generation、请求序号。`issued` 是已返回给网络 owner 的快照令牌集合，供迟到/重复回调取用，属于环境观察量。每个连接 epoch 顺序增加，省略跳号；输入相等/较旧的拒绝不变。变量 clock 与事件 stamp 是同一单调时钟的有限参数；API 返回 view 的 now 另为观察值，不使时间自动流逝，也不模拟长期缓存 view。

队列保存完整的抽象事件：U/u/pu、原接收时刻、具体价位数量与 Book payload 校验结果。该数量表示同一已覆盖价位的最后一次通过序列检查的赋值；0 表示删除。快照和增量都能携带不同的数量，旧令牌错误将可实际改变这个见证。数组算法、tick/lot、双侧交叉和具体容量检查折叠为 payload 成功/失败；先检查序列再处理 payload 的顺序保留。Unknown 不折叠成数量 0，本层没有对未覆盖价位发查询。

主配置与扩大配置的 U/u/pu 域有界，并保留过旧、等 L、L+1、断档和错误 pu 的代表事件。缓存扩大为 2 时可出现前段成功、后段失败。Book generation 和请求序号使用小的最大值模拟 checked_add 的成功/耗尽分支；这是缩小计数器位宽后的控制分支检查，不是对所有 u64 的完整穷举。连接次数亦有界，达到配置上限后仍允许旧回调和管理当前连接。

本层检查安全性，不施加网络最终响应、限流退避或最终 Live 的活性要求。构造时内存分配失败、实际 HTTP 请求编码与 TLS 传输另由实现测试检查。没有使用状态约束裁掉错误分支；所有提交的配置均需完整结束并检查动作覆盖。


时间配置取 `0..2`，允许增量在 0 到达、请求期限为 2、快照在 1 完成。`SnapshotDelayed` 专门标记成功回放且完成时刻大于最后增量时刻的分支；`SnapshotRetainsArrivalStamp` 同时约束普通与延迟成功回放。这样原接收时刻的性质具有非相等时间见证。

回放失败先保留 `Replay` 已安装或已应用的诊断内容，再执行 reset：正常换代会清空 Book；如果同步代次已耗尽，源码只使 Book 失效，仍可通过 `last_update_id()` 看见安装或部分回放后的水位。模型保留这一差别，同时两种情况均关闭 view。

API 对应关系如下。分母是构造成功后的 9 个状态相关 API；只读计数器访问由对应变量表示，构造器的配置/内存校验由 Rust 测试覆盖。这是接口设计覆盖，不是二进制行覆盖。

| API | 模型动作 | 源码 |
| --- | --- | --- |
| connect | Connect、ConnectRejected | `crates/tickwire-binance/src/depth_sync.rs:202-218` |
| subscription_ready | Subscribe、OldConnection | `crates/tickwire-binance/src/depth_sync.rs:220-226` |
| request_snapshot | Request、RequestBadState、RequestBadDeadline、RequestPending、ClockRegression | `crates/tickwire-binance/src/depth_sync.rs:228-263` |
| on_delta | OldConnection、DeltaInvalid、DeltaBuffer、DeltaApply、ClockRegression | `crates/tickwire-binance/src/depth_sync.rs:265-317` |
| on_snapshot | OldSnapshot、SnapshotExpired、SnapshotHttpFailure、SnapshotInvalid、SnapshotValid、SnapshotDelayed、ClockRegression | `crates/tickwire-binance/src/depth_sync.rs:318-371` |
| snapshot_failed | OldSnapshot、SnapshotTransportFailure、ClockRegression | `crates/tickwire-binance/src/depth_sync.rs:372-389` |
| expire_snapshot | Expire、ClockRegression | `crates/tickwire-binance/src/depth_sync.rs:390-402` |
| disconnect | Disconnect、OldConnection | `crates/tickwire-binance/src/depth_sync.rs:403-414` |
| view | View | `crates/tickwire-binance/src/depth_sync.rs:428-433` |

`snapshot_failed` 到期和未到期的两个错误值合并为 TransportFailure；它们均只清除 pending，缓存/Book 不变，期限决定的准确返回值由 Rust 测试保留。`outcome` 是为性质与覆盖设置的观察量，不声称逐个复刻 Rust enum。旧连接的 delta、订阅确认、断开共用 OldConnection；旧快照完成/失败共用 OldSnapshot，因源码都在解析和时钟检查前返回且不修改状态。

初始 generation/请求计数/连接 epoch 取 0，构造器实际支持任意合法 u64 初值。本文的单 key、连续小 epoch、有限 U/u/pu 和最大计数只验证这些有限输入的控制分支，没有证明所有整数域或外部连接 owner 的令牌分配正确性。成功条件建立在 owner 正确标记连接、订阅确认、HTTP 请求归属和同一单调时钟之上；真实订阅发送与 callback 调度由本地协议服务器测试验证。


`DeltaBuffer` 在满队列时先直接计算 Reset，只在非满分支量化 `e ∈ Events(t,s.gen)`。等价依据是满队列分支不读取 e，而 Events 在本层 Ids、Quantities、BOOLEAN 域上始终非空；原来的存在量词只重复生成相同后继。这项求值优化保留全部状态、事件域和错误分支，ipc 已独立复核。四份安全配置没有加入状态约束。
