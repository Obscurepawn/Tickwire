# 业务日志 codec 实现规格

当前格式已升级为[命令绑定 schema 2](command-binding.md)。下文保留 schema 1 的实现过程与测试记录，原测试样本用于检查旧格式拒绝。

本阶段落实 先行接口：独立 crate 编码 PreparedIntent、Fill、Fee、Terminal、AbortedBeforeSend，计算业务摘要和逻辑数据链。现有 execution、Mock、物理 WAL 与已冻结 TLA 模型保持原接口；真实业务 Journal 的接入另行实现。

## 实现与不变量

`Record` 把连续的 `journal_seq` 和原意图的 `intent_seq` 分开，并分别保存当前 `writer_exec_epoch` 和原订单 `intent_exec_epoch`。schema 1（历史格式，解码器必须拒绝）使用七字段信封及按类型确定的 payload map，最大 201 字节。当前生产编码为 **schema 2**：`SCHEMA_VERSION = 2`、`MAX_ENCODED_EVENT_BYTES = 236`（见 `crates/tickwire-journal-codec/src/lib.rs`），golden 向量位于 crate 内 `tests/golden-v2.json` 与 `tests/golden.json`。编码使用显式 CBOR major type、网络字节序整数与固定 enum 值，无 Rust 内存布局或 Debug 字符串依赖。解码器逐个核对字段，不跳过未知内容，也不根据不可信长度分配内存。

所有 i128 数值可保真保存，业务非法数值保留给 reducer 判断。负整数的 CBOR 幅度为 `!value`，避免 `-i128::MIN` 溢出；大整数仅接受 tag 2/3、9–16 字节非零开头、幅度不超过 i128::MAX 的唯一表示。规范整数边界与最大正负值有独立固定向量。

`business_digest` 绑定风险域、原订单代次、事件类型和所有业务字段。同意图重试时改变 deadline、config_revision、保留资金等字段会改变摘要；移动日志位置或更换写入者保持业务摘要。`logical_payload_hash` 再绑定 writer_exec_epoch，作为逻辑链公式中的 payload_hash。逻辑链绑定前驱、域、日志代次、journal_seq、类型与该逻辑载荷摘要，不含物理分段、批号、LSN 或物理帧哈希。

`LogicalChain` 仅从原始流 genesis 开始。域或日志代次变化、序号重复/缺口/耗尽、写入者执行代次回退、期望哈希不符都会返回错误并保留旧状态。序号溢出使用内部可表示边界测试验证，没有枚举 u64 全部前缀。重复交易结果记录可以占新日志位置，交易结果记录去重仍由执行内核负责。

## TDD 记录

先写接口、公共类型和 10 项测试，函数体保留 `todo!`。下列命令编译成功后执行测试，真实得到 10 个行为失败、0 个通过、exit 101；Red 日志 与 当时文件 SHA 已保存：

```sh
PATH="$HOME/.cargo/bin:$PATH" cargo test -p tickwire-journal-codec
```

首轮实现的 12 项测试通过后，独立审查发现原 schema 把写入者代次与订单代次合并，接管后的迟到事实无法同时表达二者。先添加 `same_late_fact_keeps_business_digest_across_writer_takeover`，实际得到编译成功后的摘要相等断言失败：接管 Red。随后为尚未发布的 schema 补上原订单代次并采用两层摘要。原 Green 和 基准样本 仅作修正前证据，当前接口以 crate 内 golden.json 为准。

最终为 **13 项测试通过**，包括 1 项单元测试、12 项集成测试、0 项 doctest。验证使用 rustc/cargo 1.99.1，主机为 aarch64-apple-darwin。命令与 Green 日志 对应：

```sh
PATH="$HOME/.cargo/bin:$PATH" cargo test -p tickwire-journal-codec
PATH="$HOME/.cargo/bin:$PATH" cargo fmt -p tickwire-journal-codec --check
PATH="$HOME/.cargo/bin:$PATH" cargo clippy -p tickwire-journal-codec --all-targets -- -D warnings
```

> 注：上述代码块中的向量生成器曾位于已删除的 `docs/validation/journal-codec/generate-vectors.py`，该命令已不可用；当前以 crate 内 `tests/golden.json`（schema 1 拒绝基线）与 `tests/golden-v2.json`（schema 2）为准，校验以 `cargo test -p tickwire-journal-codec` 为准。

接管 Red 的独立命令为：

```sh
PATH="$HOME/.cargo/bin:$PATH" cargo test -p tickwire-journal-codec same_late_fact_keeps_business_digest_across_writer_takeover
```

首轮 Clippy 提示测试辅助函数用 `as_chunks::<2>()` 替代固定大小的 `chunks_exact(2)`，调整后通过。生产 codec 无此修改。

## 基准数据与拒绝边界

基准样本由独立 Python 编码器产生，不调用 Rust crate。生成器先核对 RFC 8949 附录 A 的整数实例，再固定五种事件及一条接管后迟到 Fill，共六条记录的完整 bytes、业务摘要、逻辑载荷摘要、genesis 与连续逻辑哈希；另有 17 个整数边界和 201 字节最大记录（以上为 schema 1 历史基准的规模；schema 2 编码上限 236 字节）。这组数据验证编码及内容链，未作为业务 reducer 的合法订单流程。Rust 测试直接比较文件中的固定期望值，不调用被测实现生成期望。

拒绝测试覆盖未知/缺失/重复/乱序键、错 schema、enum 与类型、非最短整数/长度/tag、前导零或超范围 bignum、浮点、不定长项、超长、尾随字节和恶意声明长度。七条基准记录共 **591 个截断位置**全部拒绝；六条基础记录共 **99,840 个单字节变异**逐一解码，凡被接受者都要求重新编码与输入逐字节一致。该检查只覆盖这些有限输入，不是任意字节流的形式证明。

## 后续适配边界

当前输出只有内容与链一致性。后续适配器需要比对 WAL header 与业务信封的 domain/journal_epoch/journal_seq/writer_exec_epoch，检查 PreparedIntent 两代次相等及事实引用的原订单有效，在冻结状态中按统一顺序重放事实，验证依赖闭包、提交 profile 和带签名的收据，并在发送前中止事实持久化后才释放保留资金。可信中途恢复和保持前驱的 epoch 提升需要独立验证接口。此 crate 没有生产交易授权、镜像确认、实际同步或性能测量。
