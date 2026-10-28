# 受管进程的执行镜像与身份观察

监督器启动的程序必须来自已经核对内容摘要的固定镜像。原始路径只在准备阶段打开一次；它随后被改名、删除、覆盖或指向另一个文件时，实际执行字节仍取自准备时的封印内容。该能力用于实际运行实例采集，完整全局配置应用仍需各运行时 owner 接入。

Linux 的 `ExecutableImage` 从单个以 `O_NOFOLLOW | O_NONBLOCK` 打开的普通文件描述符有界复制 ELF，复制过程检查长度上限。复制完成后，memfd 加入 WRITE、GROW、SHRINK、SEAL 封印，按系统支持显式申请可执行属性；系统拒绝可执行内存文件时返回错误。监督器对封印后镜像计算 SHA-256，与批准的 `Identity.build_hash` 相同才启动。启动使用固定描述符路径及标准 `Command`；配置回调提供参数、环境、工作目录、UID/GID 与标准流；回调后再次核对执行程序为原固定描述符路径，改变则在 spawn 前拒绝。镜像由 Registry 条目持有，描述符使用 CLOEXEC，避免向子进程泄漏可复用句柄。每个条目保留一份镜像，额外内存按实际长度计入后续托管资源核验；目前没有跨实例镜像缓存，条目随 Registry 释放。仅在内核以 EINVAL 表示不支持新标志时，才使用旧内核 memfd 标志或省略可选 EXEC 模式封印；内容的四种不可变封印始终强制，EACCES/EPERM 等策略拒绝不会回退。非 Linux 平台明确返回 Unsupported。

`Registry::accept` 在握手前后检查仍持有的活跃 Child，以及 `/proc/<pid>/exe` 实际对象的设备、inode、长度和封印与固定镜像一致。实际程序再次 exec 到不同镜像时拒绝认证。`process()` 维持原有轻量退出检查，不在正常轮询中新增文件读取。

`Registry::observe` 是显式冷路径。它读取真实内核主机 boot ID、PID 的 starttime 和当前 UID/GID，在读取前后检查同一个 owned Child、固定镜像和内核启动身份。UID/GID 的 real、effective、saved 和 filesystem 四个值均须与批准启动身份一致；缺字段、畸形值、超限、权限不足、退出或镜像变化均拒绝。`/proc/<pid>/stat` 的 comm 可包含空格、括号与换行，解析按右括号定位后读取规定的 starttime 字段。所有文件读取有固定上限。

输出 `LiveChildObservation` 字段私有，仅由成功采集构造：批准的 instance/generation/role 等 Identity、实际 PID/UID/GID、实际 host boot、内核 start ticks、固定镜像大小与摘要、最后仍标记连接的控制 epoch，以及本地单调采样起始时刻（供保守的新鲜度检查）。批准的逻辑身份与独立读取的内核事实并列保留。控制 epoch 表示既有连接状态，不证明当次 challenge 已应答；全局 revision、projection、Applied、槽位与 locator 身份仍待真实运行时应用后产生。

SDK 的当前程序核验直接打开 `/proc/self/exe`，按既有预算读取实际镜像并与批准的 Binary 内容对象比较。恢复清单中的 Binary 仍保留独立、完整、内容固定的依赖；启动入口路径的存在与归档依赖的存在分别检查。缺少真实恢复对象时沿原规则拒绝，不用当前镜像悄悄替代历史依赖。

动态 ELF 的解释器与系统共享库由受信宿主负责加载和资格核验。使用 `$ORIGIN` 私有库的程序还需验证其在描述符执行路径下的加载行为。本层镜像摘要的覆盖范围限于主 ELF，入口拒绝解释器脚本。退出观察覆盖直接子进程，后代共享内存的释放另需实际回收证据。

验证使用真实 Linux 子进程：原路径替换/删除、固定后源文件改写、错误构建摘要、脚本和超限文件、退出及再次 exec、实际身份读取、恶意 comm、现有认证握手与独立策略重放/回退 probe。生产规则保持固定；最终全仓检查和真实策略进程场景分别记录源码清单与可执行文件摘要。新的形式化验证按项目约定留到功能开发结束。

内核接口依据：[memfd 可执行策略](https://docs.kernel.org/userspace-api/mfd_noexec.html)、[proc_pid_exe](https://man7.org/linux/man-pages/man5/proc_pid_exe.5.html)、[proc_pid_stat](https://man7.org/linux/man-pages/man5/proc_pid_stat.5.html)。
