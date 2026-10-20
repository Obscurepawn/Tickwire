# 发布对象和状态恢复的针对性验证

第一阶段实际命令及结果：

| 命令 | 结果 | 日志 |
|---|---|---|
| `cargo test -p tickwire-release --lib`（初始行为） | 3 失败，退出码 101 | [release-red.log](release-red.log) |
| 同命令，完成校验 | 3 通过，退出码 0 | [release-green.log](release-green.log) |
| 同命令，补齐边界 | 7 通过，退出码 0 | [release-boundaries.log](release-boundaries.log) |
| `cargo test -p tickwire-strategy-sdk --lib restore::tests`（初始行为） | 1 通过、2 失败，退出码 101 | [restore-red.log](restore-red.log) |
| 同命令，保留已消耗标志 | 3 通过，退出码 0 | [restore-green.log](restore-green.log) |
| 同命令，补齐闭包与回调边界 | 6 通过，退出码 0 | [restore-boundaries.log](restore-boundaries.log) |

发布对象的 Red 是编译成功后的实际断言失败：错误 operation/member 被接收、带空白的非规范 JSON 被接收、缺失能力仍返回成功。恢复的 Red 是旧版迁移和新版 restore 输出的动作标志为 0，违反已发生动作保持 1 的断言。

边界测试覆盖全部成员字段和三根、缺文件/重复角色内容、容量、源 schema、回滚方法、显式 rebuild 契约、实际输入字节和顺序摘要；SDK 覆盖真实规范来源闭包、删除账户 callback 后重新计算所有根仍拒绝、旧授权/动作键错配、默认 Unsupported、实际回调错误和非法输出。

SDK 边界测试首次编译因测试夹具把 `&[u8]` 当 `Vec<u8>` 返回而失败，原日志保存在 [restore-boundaries-compile-attempt.log](restore-boundaries-compile-attempt.log)，不计为行为 Red。修正夹具后 6 项通过。

本阶段运行纯单元，未运行 Linux 场景或 TLA。Loader 文件读取、真实 worker 启动与 rollback probe 接线完成后另补实际进程证据。

## 实际回执、闭包和执行端接线

[completion-binding-red.log](completion-binding-red.log) 是 SDK 测试编译成功后的真实断言失败：更改 Completion 的 Operation revision，单独 `verify` 仍成功。修复令 MemberRelease 保留完整 Operation，Completion 逐项比较后，[v2-sdk-green.log](v2-sdk-green.log) 的 19 项 SDK 单元全部通过。该测试实际调用 ObserveFactory 恢复与回滚函数，再检查缺失账户对象、输入字节损坏和 Operation 错配。

[v2-release-green.log](v2-release-green.log) 为 10 项 release 单元通过；[layout-green.log](layout-green.log) 为两个独立 literal offset/保留字节测试通过；[v2-lifecycle-green.log](v2-lifecycle-green.log) 保留精确 operation Halt 隔离回归。[v2-check.log](v2-check.log) 为 app、SDK、replica 的全部 targets 编译通过，包含 Host 的实际 probe 启动和 worker 可执行入口。编译不计作真实 probe 进程运行结果。

正常退订的 Linux 场景已独立通过，见[原始交接证据](../core-chain/spot-handover/README.md)。该冻结场景早于发布预检 schema2，不能替代本模块随后由 lab 执行的实际恢复/回滚/交接验证。

[template-red.log](template-red.log) 是编译成功后的断言失败：未列入发布清单的恢复模板被接受。新增 RecoveryTemplate 角色与实际引用路径检查后，[v2-sdk-final-green.log](v2-sdk-final-green.log) 的 20 项通过；覆盖缺模板、缺路径以及相同字节位于未绑定路径的拒绝。Host 启动检查和 SDK 主恢复/回滚都调用同一校验。

最新针对性结果还有：[v2-release-final-green.log](v2-release-final-green.log) 10 项、[v2-lifecycle-green.log](v2-lifecycle-green.log) 6 项、[shared-input-green.log](shared-input-green.log) 6 项、[v2-runtime-green.log](v2-runtime-green.log) 4 项。`v2-clippy.log` 保存了共享构建缓存出现旧账户 API 元数据时的失败输出；源码 API 未删除，由根线程清理相关包缓存后统一复查。


## Host 已存在对象的同步重试

独立源码审查发现：第一次 `create_new` 写入后，文件同步失败仍可能留下内容完全相同的可读文件。旧 `persist_object` 的 `AlreadyExists` 分支只校验字节并同步目录，重试因此会跳过文件同步。

[host-resync-red.log](host-resync-red.log) 使用真实临时文件和可控的同步失败：第一次写入后令文件同步返回错误，再用相同字节重试；旧实现返回成功，实际断言失败。修复后两个分支都先同步文件，再同步目录；[host-resync-green.log](host-resync-green.log) 的 1 项测试通过，同时检查失败时不继续目录同步、成功顺序为文件→目录、错误内容不调用同步。[host-resync-clippy.log](host-resync-clippy.log) 为 Host lib 的 `-D warnings` 检查通过。该测试注入同步调用失败，没有模拟断电。

[host-resync-compile-attempt.log](host-resync-compile-attempt.log) 和 [host-resync-fixture-attempt.log](host-resync-fixture-attempt.log) 分别保留早期夹具的依赖和平台问题，不计为行为 Red。


SDK 的共享账户测试夹具已改为只在 crate 根加载一次，handover 与 restore 测试复用该模块。[sdk-shared-fixture-green.log](sdk-shared-fixture-green.log) 记录 20 项 lib 测试通过，[sdk-shared-fixture-clippy.log](sdk-shared-fixture-clippy.log) 记录 lib 与 tests 的 `-D warnings` 通过。


## 控制轮询和退订诊断

MD、Host、SDK 和市场订阅器原先用 `recv(now + 1µs)` 模拟非阻塞轮询。底层先分配接收缓冲再检查期限，调用可能在首次读取之前超时。调用点现改为 `try_recv` / `try_accept`：`None` 表示本轮没有消息，错误仍执行原来的关闭保护；退订的 3 秒子期限与 Host 的 4 秒期限不变。MD 记录关闭时的阶段和 PID，worker 的错误标明退订过程中具体的收发步骤。

[control-poll-callers-green.log](control-poll-callers-green.log) 记录调用者 37 项 native lib 测试通过（Source 8、MD 7、Host 2、SDK 20）；[control-poll-callers-clippy.log](control-poll-callers-clippy.log) 记录这四个 crate 的 all-targets `-D warnings` 通过。实际控制套接字和完整交接的运行结果由相应 Linux 证据记录。
