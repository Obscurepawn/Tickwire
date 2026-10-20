# TopicSnapshot 构造验证

构造器已生成完整可接收的 TopicSnapshotManifest，独立基准样本比对、多标的状态保持及容量边界均通过。生产实现为 `crates/tickwire-snapshot/src/builder.rs`；`src/lib.rs` 仅增加模块与导出，现有接收校验器未修改。SHA-256 计算使用已有版本的 `sha2 0.10`，该依赖已从测试依赖移入运行依赖。

## 实际 Red 与 Green

| 阶段 | 实际结果 | 证据 |
|---|---|---|
| 初始 Red | 编译成功；基准样本构造断言收到 UnsupportedOperation，1 项失败 | [red.log](red.log) |
| 展开 Red | 编译成功；14 项构造与边界断言全部失败 | [red-expanded.log](red-expanded.log) |
| 首轮 Green | 14 项构造测试通过 | [green.log](green.log) |
| 最终 native | 1 个算术单元、17 个构造、12 个接收测试全部通过 | [tests.log](tests.log) |
| Linux aarch64 | 同样的 30 项测试，以及既有跨进程快照接收的 2 个入口（含 helper）通过 | [linux.log](linux.log) |
| Linux Clippy | 安装缺少的 Clippy 组件后，`-D warnings` 通过 | [linux-clippy.log](linux-clippy.log) |
| 静态与文档 | fmt、Clippy `-D warnings`、doc 测试入口通过 | [fmt.log](fmt.log)、[clippy.log](clippy.log)、[doctests.log](doctests.log) |

初始 stub 已有真实 API 和错误返回，Red 来自运行中的测试断言，不是缺少符号导致的编译失败。构造完成后又增加三项边界回归，覆盖完整 4096 标的清单、多个子对象共同预算、资源配置与非 Live 枚举。

## 检查内容

基准输入来自 `specs/contracts/ipc-vectors-v1.json`，测试重新编码一个完整 12 KiB 文件，逐字节比对；完整资源头、section 数据/描述符、子对象、清单、页尾和最终 JOIN hash 都在比较范围中。输入状态逆序及 CRC 开启的测试独立解码输出记录，确认 1 起行号、原观测时间和重新计算的 CRC。

三标的测试保留 Live、Invalid 和没有 Quote/Trade/MarkFunding 的 Bootstrapping 成员。所有子对象都处于 S=1000，最后事件可以为 0 或 990，Unknown coverage 与空深度保留。文件可被接收，readiness 正确为 NotReady。4096 标的用例覆盖清单行跨页，第一子对象 offset=532480，最后成员仍可完整读取；4097 项拒绝。

预算测试涵盖精确页边界、少一字节、超过 64 MiB 的单侧输入、多个成员分别可容纳但总和超限。直接算术用例使用 usize::MAX 检查长度乘加和页对齐溢出，不构造非法 slice。其他拒绝用例包括缺失/重复/逆序集合、混合 S/feed/catalog/version、重复 snapshot ID、状态重复与未知消息、非法深度、未来观测时间、过期对象、JOIN 身份冲突与非零派生绑定冲突。

复跑命令从仓库根目录执行：

```sh
cargo test -p tickwire-snapshot --all-targets
cargo test -p tickwire-snapshot --doc
cargo fmt -p tickwire-snapshot --check
cargo clippy -p tickwire-snapshot --all-targets -- -D warnings
```

Linux 使用与 `scripts/check-linux.sh` 相同的固定 Rust 1.99.1 容器。首次命令中的测试已全部通过，随后 Clippy 因组件未安装退出；安装组件后单独复跑通过，两个原始日志均保留。builder 交付的是拥有完整内容的内存对象；构造、封印、SCM_RIGHTS、pending gate 和 JOIN_READY 的组合行为由 JOIN 协调器集成测试验收。调用者负责在单一发布边界捕获同一 S 的实际状态，字段一致性检查不能代替这一所有权边界。

独立源码审阅已检查集合/共同截面、CRC 重排、深度、分配前预算、摘要和 JOIN 绑定，未发现阻断项。生产构造器 SHA-256 为 `8e2bdadb0c50f55149cbfb6f164463f82ff87f711ba99e7afd8b26d4c1f5f798`。
