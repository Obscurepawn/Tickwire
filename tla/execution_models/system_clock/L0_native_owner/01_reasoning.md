# L0 推理与源码映射

验证模式为带故障输入的源码转录。系统调用错误、无效字段和转换溢出对应 `native_nanos` 的错误返回；两种读数回退对应 `Guard::observe` 的显式防御分支。模型允许这些输入以检查失败关闭，不将回退描述为正常 OS 时钟行为。

## 调用边界

每个公开调用是一个完整动作。`SystemClock::sample` 在一个可变借用内读取 Instant、调用 OS、检查 Guard 并返回；另一调用无法在这些步骤间修改该 owner。模型仍保存两种独立读数，不把它们合成同一原子时间快照。外部时间在同步调用内已经流逝多少，由本次输入的两个值表达。

`new` 先构造局部对象再调用 sample。探测成功才发布 owner，失败保留“构造失败”结果，后续不允许对该对象调用 sample。成功对象可继续直接采样，也可一次性移入 TimeClient；移交之后只有 TimeClient 动作能访问它。故障状态不会通过新建、重置或换域动作消失。

TimeClient 场景固定一个通过构造器检查的配置，不启动 HTTPS 请求。其校准器初始无 anchor，`local` 的校准读取只包含永久故障/域/单调读数检查和 Uncalibrated 分支；后续 public sample 再次读取同一 MonoTime，不改变这些状态，返回的 signed 恒为 None。这使本级能完整验证 provider→local→公共 sample 的失败顺序，同时不假造网络校准成功。

## 状态与分支

| 模型状态或动作 | 真实含义 |
| --- | --- |
| owner 状态：未构造、构造失败、直接持有、已移交 | Rust `Result<SystemClock>` 的发布点及 move 所有权 |
| native.last / native.faulted | `Guard.last` 和 `Guard.faulted`，last 仅在成功观察后改变 |
| native 失败类别 | 系统调用失败、InvalidTime、Overflow、Unsupported；Backwards 从输入比较产生 |
| client.lastInstant / client.fatal | `BinanceTimeClient.last_instant` 与 `fatal` |
| core.last / core.fault | 无 anchor 校准器的 `last_seen`、`fault`；与 client.fatal 分开保存 |
| providerCalls | 历史见证：永久 client.fatal 后不再调用 provider |
| nativeReads | 历史见证：native faulted 后不再进入 `native_nanos` |
| 成功输出与错误输出见证 | 保存返回类别、域、两种读数及调用前状态，避免只依据当前状态推断历史 |

构造动作也把成功的内部探测放入 `output/last` 历史见证；真实 `new` 返回的是 owner，没有向调用方交付 `SystemSample`。后续 sample 动作中的这两个字段才对应公开返回值。排序性质从首次内部探测开始检查，与 Guard 保存该次成功值的行为一致。

`nativeReads` 统计 `native_nanos` 的调用次数。Unsupported 分支的一次调用计为 1，但该平台分支没有实际 OS 系统调用。此见证用于检查故障后是否仍进入采样函数，不用来测量 syscall 次数。

原生错误先映射再交给 TimeClient。TimeClient 先检查已有 fatal；没有 fatal 时才调用 provider。provider 返回上述五类永久错误时保存原错误，未来调用继续返回它；其它错误直接返回，不设置 fatal。provider 成功后先检查 Instant 回退，成功才更新 lastInstant，再读取校准器。校准器拒绝的读数不会清除 client.lastInstant，也不会错误地被记录为 provider 错误。

原生场景检查两种读数均不减、相等 tick 可返回、固定域、构造失败无 owner、错误后 last 不变、永久故障后不再进入 native_nanos。组合场景另外检查固定 provider 错误映射、外层 fatal 以后无 provider 调用、校准器永久故障以后没有成功输出。通用 provider 场景保留暂时错误后继续成功的执行，防止把所有错误都错误建成永久停止。

## 有限配置与结论范围

原生与通用 provider 主配置使用 `0..2` 的读数、最多 5 次调用；原生扩大配置使用 `0..3`、最多 7 次调用。Unsupported 配置只执行构造失败分支。原生和通用 provider 使用各自具名 Next，按实际入口分别检查覆盖；原生直接调用的相等、单边回退、系统错误和错误后的再次调用保留独立入口；client 原生成功入口包含相等值，GenericSuccess 包含成功值与回退值，由源码检查决定结果。配置中的计数上限限制外部执行长度，不改变生产 Guard 语义。

本级不检查时钟精度、漂移、校准区间、签名窗口、u64 算术、网络请求或挂起后的辅助 Instant 期限。它向后续组合提供的是：构造成功的固定域、成功采样顺序，以及故障传播后不能再输出可信读数。完整 TimeClient 网络 L0 和私有流时钟 adapter L0 已单独列入本阶梯计划。
