# 账户协调器持久化独立复核

独立审阅 `src/lib.rs`、`binding.rs`、`recovery.rs`，并新增 `tests/persistence_failures.rs` 与独立 `common/fault_backend.rs`。测试复用公共账户、TLS 对端和受信授权测试替身；持久化使用真实 `FileBackend`，没有修改生产源码或公共 fixture。

## 发现与修复

在 Queued 状态保存该订单的成交事实后，本地零接受取消仍先同步 Aborted，随后 `Account::apply_abort` 因已有成交返回错误。有效失败记录为 [abort-preflight-red.log](abort-preflight-red.log)：期望日志位置保持 2，实际变为 3。源码审阅同时确认恢复会调用同一个账户中止校验。

修复由主代理实施：先在候选账户上验证本地中止；验证失败便关闭发送并保留原日志，验证成功才同步 Aborted，最后发布候选账户。回归同时检查修复后重开能够恢复成交和 Unknown 状态。

[first-run.log](first-run.log) 中六项故障测试已通过；第七项当时因测试 trade ID 超长在 fixture 构造时报 Bounds，不算实现反例。缩短 ID 后取得上述行为失败。

## 故障覆盖

Admit、Aborted、VenueEvent 分别运行两种故障，共六项：

- 实际写入半个批次后返回错误；重开只能恢复此前完整前缀。
- 实际完成文件同步后返回错误，模拟同步结果丢失；运行实例不公布新日志位置，重开按完整尾记录恢复。

检查失败后连接关闭、账户冻结、未同步中止不释放预留、未同步事实不提前应用，以及 TLS 对端没有订单应用字节。Admit 恢复不产生活跃发送凭据；Aborted 完整尾仅在重放后释放；成交完整尾仅在重放后改变余额与剩余预留。已中毒日志拒绝继续写入事实。Aborted 两项恢复另外要求此前 Admit 的可信物理前缀。

这些测试覆盖真实文件和 socket 的调用完成边界；故障由包装后端注入，没有进行断电实验。恢复依赖授权使用明确的测试替身，本组不验证生产授权算法或签名字节映射。

## 验证记录

原生与 Linux 各七项测试，命令和结果分别保存在 [native.log](native.log)、[linux.log](linux.log)。Linux 使用固定 Rust 镜像和独立 target 目录。新增测试的 Clippy 结果见 [clippy.log](clippy.log)。源码摘要见 [source-hashes.json](source-hashes.json)。

首轮 Linux 命令未显式选择镜像内工具链，停在 rustup 补装组件；主动停止后按仓库 runner 的 `RUSTUP_TOOLCHAIN=1.99.1` 设置重跑。未开始测试的原记录保存在 [linux-toolchain-stopped.log](linux-toolchain-stopped.log)。
