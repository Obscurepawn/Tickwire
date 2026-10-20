# IPC 布局测试更新

根线程的固定源码副本在 macOS 全仓检查中得到 `35 passed; 1 failed`：旧测试仍将 `IntentBody[304..384]` 全部视为保留区，当前契约已在 `304..340` 放置 `command_digest` 和 `command_bytes`。`native-red.log` 是 `.tools/core-checkpoint/macos.log` 中该测试程序的原始输出摘录。

执行 `python3 crates/tickwire-ipc/tools/generate.py tests` 后，保留区检查更新为 `340..384`，并新增 `DepthCoverage`、`HandoverReadyBody` 的长度、保留区和往返检查。此次执行没有改动测试生成器、生产 wire 偏移或原有 `tests/vectors.rs`。

首次重跑随后暴露另一处旧断言：测试将布局数量固定为 36，而当前契约已有 38 个布局，见 `native-after-generation.log`。该断言现从契约读取布局数量，原有整个契约的 SHA256 绑定检查继续保留。新增三个独立字面量字节样本，检查命令绑定字段以及两个新增布局的非零字段与固定偏移。

最终检查：

- `cargo test -p tickwire-ipc`：115 项通过，见 `native-green.log`。
- `cargo clippy -p tickwire-ipc --all-targets -- -D warnings`：通过，见 `native-clippy.log`。生产改动仅将已完成 16 字节倍数校验后的 `chunks_exact(16)` 换成 `as_chunks::<16>().0`。
- `python3 scripts/verify-ipc-contract.py`：38 个布局、646 个字段、63 个原有测试向量、25 个语义用例通过；37 个兼容基准向量的摘要未变，见 `contract-check.json`。原有向量仍以固定十六进制字节和摘要为期望值，没有用生产编码结果重新生成期望值。
