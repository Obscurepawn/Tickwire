# 2026-10-29 阶段证据

本目录记录受管子进程的固定执行映像、实际内核身份采集，以及后续资源约束和实际实例观测阶段。实现边界见[监督器规格](../../implementation/supervisor-process.md)。

[真实进程验收](runtime-image-acceptance.json)保存两条固定 release 场景和 Host 的三项进程测试。完整输入回放交接达到 S2→C10→D285，声明重建在同一 PID 采集与安装后达到 D291，两场费用闭合。运行中的 `/proc/<pid>/exe` 样本为 memfd，摘要与本次固定 worker 相同。

[离线复核脚本](verify-runtime-image-acceptance.py)检查 49 个归档文件的摘要、六份签名收据、两份实际 memfd 样本及源码/二进制绑定。使用 Python 3.11+ 与 `cryptography`，仅读取本目录：

```sh
python3 specs/evidence/20261029/verify-runtime-image-acceptance.py
```

[验收源码清单](runtime-image-source-sha256.json)保留这些实际运行所用的 875 个构建输入。[最终检查清单](runtime-image-final-source-sha256.json)同样包含 875 项，仅两个配置服务测试文件不同，生产源码、依赖和工具链一致。[阶段报告](runtime-image.json)保存完整检查、失败记录、修复与最终源码的关系。

首次全仓检查中，归档操作的内部断言通过，随后 TLS 回应因连接的绝对期限到达而失败。受控短期限回归复现后，夹具改为先完成认证交换，再执行内部归档回调，业务期限与每次调用的信任核验保持原样。随后发现的选举测试固定推进 110 轮就假定已有 leader；修复后的测试显式构造真实分票，再按真实选举结果和原绝对期限推进，保留 503 与取消请求断言。

[只读锚点审计](runtime-image-anchor-audit.json)和[复现脚本](runtime-image-anchor-audit.py)覆盖 25 份历史锁，本阶段没有修改其覆盖的源码。历史 TLC 结果保持原范围，新增形式化验证留到功能开发完成后。

运行环境为原生 WSL2 Linux，本地 TLS 交易所替身提供业务验收。主 ELF 之外的解释器与共享库依赖受信宿主；旧内核兼容分支和完整生产资格尚未验证。每个 Registry 条目保留一份映像，后续托管资源核验需计入这部分内存。全局配置应用与新鲜 challenge 仍是后续研发事项。

## 策略子进程资源约束

[资源阶段报告](process-resources.json)记录 cgroup v2 专用组核验、执行前迁入和 CPU affinity 设置、全部线程资源观察，以及 App/Host 三种用途的资源启动接线。[源码清单](process-resources-source-sha256.json)固定 887 个构建输入，workspace 检查和本阶段 release 验收使用同一份源码。

完整 `check-rust.sh` 用时 373.92 秒，格式、workspace 所有目标、文档测试和 Clippy 通过。日志汇总包含 2096 个通过、26 个忽略；该汇总包括父测试启动的子测试输出，不表示 2096 个独立测试。真实子进程的 affinity 和线程继承、执行前失败、实际 cgroup inode lease、普通目录拒绝已运行。

[独立进程验收](process-resources-acceptance.json)记录本阶段固定二进制下的完整回放交接、同 PID 声明重建和三项 Host 真实进程测试。两条完整业务场景均达到 C10→D285，镜像与策略各核对 12 条账户记录，成交和费用闭合。App 的错误资源配置先通过严格配置检查，再由实际内核文件系统类型检查拒绝；跟踪确认没有执行策略 worker、没有下单或产生 ARM。

[资源验收复核脚本](verify-process-resources-acceptance.py)离线检查 77 个归档文件、六份签名收据、两份实际 memfd 样本，以及 App 拒绝路径的进程跟踪。依赖同上，仅读取归档：

```sh
python3 specs/evidence/20261029/verify-process-resources-acceptance.py
```

完整 cgroup 正向测试需要预配置的可写委派 leaf；本机当前组由 root 持有，缺少迁入权限和所需 controller 文件，因此这两项显式 ignored 测试尚未运行。上述组件测试与普通启动兼容场景分别记录，保留这个缺口。

[只读资源锚点审计](process-resources-anchor-audit.json)覆盖 25 份历史锁，所覆盖源码保持不变。新增实现的形式化验证仍留到功能开发完成后。资源观察属于有界冷采样，完整全局配置应用与新鲜认证事实 provider 尚待接入。

## 实际实例查询与持久观测

[实例观测报告](runtime-observation.json)记录 SDK 持续运行查询、Host 实际进程与消费前缀核验，以及 App 私有目录入口、独立日志和完整依赖持久化。[源码清单](runtime-observation-source-sha256.json)绑定 902 个最终构建输入，包含 IPC 契约校验器；全仓检查、固定 release 构建和实际运行使用同一份源码。

完整 `check-rust.sh` 用时 395.88 秒，格式、所有 workspace 目标、文档测试和 Clippy 通过。日志汇总为 2143 个通过、29 个忽略，包含父测试启动的子测试输出。新增 IPC 向量使总数达到 94，语义案例为 33，原有 88 份基准字节不变。

[实际进程验收](runtime-observation-acceptance.json)包含两条完整业务场景和 6 项 Host 进程测试。完整回放交接为 S2→C10→D285，声明重建在同一 PID 安装后达到 D285，两场各核对 12 条账户记录并完成费用闭合。连续查询、暂停后超时、取消排空、慢接收和正常退订均在真实 worker 上执行。慢接收用例实际消费 176 条账户记录，早期排队查询的完整闭包通过；末端新查询超过既有恢复对象 16 MiB 上限时明确拒绝，worker 仍存活并正常退出。

App 在同一实际进程上完成两次外部文件查询，READY 序号从 2 到 5，第二次请求的账户下界为 1、真实位置为 2；两次分别核验 14 个完整依赖对象。之后账户、策略与镜像均达到位置 8，成交和手续费闭合。重复请求保持原结果，内容冲突与四项错误请求正确拒绝；真实 App 重启后保留历史结果和原挑战，把持久化但未完成的请求标为 Interrupted，未向 worker 重新派发。重建和 App 场景各记录一条启动时的时钟 Gap，随后进入新的对账代次，真实日志确认完成对账后才注册命令；归档保留累计计数，不把它写成全程无缺口。

并发检查首次遇到三项锁重开失败。实际 PID、FD、inode 与 `flock` 记录确认：父 owner 已关闭自己的描述符，尚未执行新程序的子进程仍持有继承副本。七类文件锁改为在取得锁后立即由私有 guard 持有，构造失败与最终 owner 正常销毁时显式解锁；Raft 的合法共享 owner 仍持续持锁。原并发测试加入 100 ms 执行延迟后通过，没有串行化测试或放宽排他规则。

[离线复核器](verify-runtime-observation-acceptance.py)验证归档字节、请求与进程身份、源码和二进制绑定、签名收据，并从账户导出记录独立重算消费前缀。大型 ELF 与恢复 CAS 没有收入仓库，完整闭包由运行时和独立 Rust 检查器在实际文件上核验，离线脚本只验证归档记录：

```sh
python3 specs/evidence/20261029/verify-runtime-observation-acceptance.py
```

[只读锚点审计](runtime-observation-anchor-audit.json)扫描 25 份历史锁，标出日志与恢复存储源码变化及历史缺失路径；本轮未运行 TLC、重写模型或更新历史锚点。观测入口使用本机 Linux 目录权限和明确的构建批准，全局配置应用、跨主机签名事实与持久来源水位继续推进。生产延迟与完整委派 cgroup 正向资格尚待验证。

## 全局配置约束策略首次启动

[启动绑定报告](projection-launch.json)记录 App 从完整配置投影中核对策略身份、程序摘要、OS 身份和 CPU、NUMA、内存约束，并在读取首条交易意图前核验实际子进程资源的接线。[源码清单](projection-launch-source-sha256.json)固定 909 个构建输入；完整检查、release 构建及独立进程运行使用同一份源码。

完整 `check-rust.sh` 用时 353.95 秒，格式、workspace 所有目标、文档测试和 Clippy 通过。日志汇总为 2168 个通过、29 个忽略，包含父测试启动的子测试输出。新增投影单元测试 16 项，App 实际入口测试 7 项，覆盖绑定冲突、未知 OS 名称、受限启动模式、实际资源拒绝、错误 ELF，以及本地策略失败后持续停止交付意图。

固定 release 的普通策略交接场景通过，账户、镜像和策略均核对到第 12 条记录，成交与手续费闭合。运行中的 worker 为 memfd，摘要与固定二进制一致。此场景省略 `projection_launch`，用于核对现有业务链的兼容性；启用约束后的完整成功路径仍需可写委派 cgroup，本机尚未执行。

归档保留两次失败及修复依据：首次并行检查中的两个测试竞争同一个 cgroup 根 inode，合并共享资源断言后完整检查通过；首次进程验收的 socket 路径长 112 字节，超过接口的 107 字节上限，缩短外部验收目录后通过。后者没有改动源码或固定二进制。

首次启动证明只保存当时的启动绑定，不生成全局配置应用收据。完整投影消费者、签名事实来源与持久水位继续开发；新 TLA/TLC 验证仍留到功能开发完成后。
