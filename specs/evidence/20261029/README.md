# 2026-10-29 阶段证据

本目录记录受管子进程的固定执行映像、实际内核身份采集，以及后续资源约束阶段。实现边界见[监督器规格](../../implementation/supervisor-process.md)。

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
