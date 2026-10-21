# 交易命令绑定实现规格

接口见交易命令与持久意图的绑定。

`OrderRequest.command_digest` 是必填的 32 字节摘要，纳入内核的请求相等比较、提交收据和恢复记录。全零值在预留或恢复引入订单前被拒绝。保持固定大小的订单状态，完整命令放在不可变对象中。

业务 codec 升级为 schema 2，`PreparedIntent` 的键 14 保存摘要。业务摘要、逻辑摘要、日志 genesis 和链节点使用同一版本域；最大编码长度为 236 字节。原 schema 1 样本和生成器保留，当前解码器拒绝旧版本。业务 bootstrap 中的 codec 版本也随之变化，不允许把缺失参数的旧订单恢复为新的可发送订单。

恢复清单允许一个种类 13 的完整命令对象。`Persistence::publish/verify` 校验其实际字节、长度和摘要，并重新完成同步；私有构造的 `VerifiedClosure` 保存该对象摘要。业务日志在首次提交、幂等返回和 runtime 恢复时，核对它与意图的 `command_digest` 一致。仅有匹配的业务摘要、缺少命令对象的 capability 会被拒绝。

这一层检查摘要与字节持久性，不解释命令内部的产品参数。交易所无关命令类型负责规范编码和字段检查；网关还需核对执行域、身份、环境、预算单位及实际报文，再使用已持久化的同一命令发送。旧 `Exchange` 接口的调用不等于网络已经接受字节。

## 测试

先增加字段和行为断言，保留旧编码行为，实际得到三项失败：摘要在 CBOR 往返和文件 WAL 重放时丢失，全零摘要仍能预留。随后修正 schema 及业务校验。失败记录与当时的源码摘要保留。

再增加“闭包缺少完整命令时拒绝提交”的断言，实际复现旧 `commit_intent` 仍返回成功；增加种类 13 校验后通过，见依赖检查失败记录。回归包括另一个命令的 capability、同意图摘要变更、真实文件恢复、命令依赖缺失/重复及原有 NoBytes 顺序。

schema 2 的独立 Python 编码器曾生成固定 CBOR、业务摘要和逻辑链结果，不调用 Rust 实现（生成器原位于已删除的 `docs/validation/command-binding/generate-vectors.py`，命令已不可用；schema 2 回归以下方 cargo 测试为准）：

```sh
cargo test -p tickwire-execution -p tickwire-journal-codec \
  -p tickwire-business-journal -p tickwire-mock -p tickwire-recovery --all-targets
```

## 形式化模型

[业务日志 L1](../../tla/execution_models/business_journal/L1_command_binding/05_output.md)在实际单所有者调用顺序上增加命令摘要、对象依赖和冲突请求。三组配置覆盖 376、2664、7326 个不同状态，10 个不变量通过，每组全部 29 个 Next 动作均生成后继。模型保留日志写入/同步失败及进程崩溃，不把已确认同步的前缀回滚。

该模型的 `sent` 表示旧 `Exchange::submit` 收到的摘要。报文内容、首次应用字节接受和命令解析属于后续网关组合；相同请求的幂等返回由 Rust 测试检查，本层只展开内容冲突。
