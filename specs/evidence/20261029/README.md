# 2026-10-29 阶段证据

本阶段实现受管子进程的固定执行映像、实际内核身份采集，以及策略 SDK 对 memfd 程序的内容核验。实现边界见[监督器规格](../../implementation/supervisor-process.md)。

[真实进程验收](runtime-image-acceptance.json)保存两条固定 release 场景和 Host 的三项进程测试。完整输入回放交接达到 S2→C10→D285，声明重建在同一 PID 采集与安装后达到 D291，两场费用闭合。运行中的 `/proc/<pid>/exe` 样本为 memfd，摘要与本次固定 worker 相同。

[离线复核脚本](verify-runtime-image-acceptance.py)检查 49 个归档文件的摘要、六份签名收据、两份实际 memfd 样本及源码/二进制绑定。使用 Python 3.11+ 与 `cryptography`，仅读取本目录：

```sh
python3 specs/evidence/20261029/verify-runtime-image-acceptance.py
```

[验收源码清单](runtime-image-source-sha256.json)保留这些实际运行所用的 875 个构建输入。[最终检查清单](runtime-image-final-source-sha256.json)同样包含 875 项，仅两个配置服务测试文件不同，生产源码、依赖和工具链一致。[阶段报告](runtime-image.json)保存完整检查、失败记录、修复与最终源码的关系。

首次全仓检查中，归档操作的内部断言通过，随后 TLS 回应因连接的绝对期限到达而失败。受控短期限回归复现后，夹具改为先完成认证交换，再执行内部归档回调，业务期限与每次调用的信任核验保持原样。随后发现的选举测试固定推进 110 轮就假定已有 leader；修复后的测试显式构造真实分票，再按真实选举结果和原绝对期限推进，保留 503 与取消请求断言。

[只读锚点审计](runtime-image-anchor-audit.json)和[复现脚本](runtime-image-anchor-audit.py)覆盖 25 份历史锁，本阶段没有修改其覆盖的源码。历史 TLC 结果保持原范围，新增形式化验证留到功能开发完成后。

运行环境为原生 WSL2 Linux，本地 TLS 交易所替身提供业务验收。主 ELF 之外的解释器与共享库依赖受信宿主；旧内核兼容分支和完整生产资格尚未验证。每个 Registry 条目保留一份映像，后续托管资源核验需计入这部分内存。全局配置应用与新鲜 challenge 仍是后续研发事项。
