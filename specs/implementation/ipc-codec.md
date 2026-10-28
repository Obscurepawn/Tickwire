# IPC codec 实现规格

实现位于 [`tickwire-ipc`](../../crates/tickwire-ipc)，接口见 IPC 编解码接口。协议源是 ABI 1.0 与 [机器契约](../contracts/ipc-wire-v1.json)。

`Wire` 处理稳定页面镜像和完整消息的固定布局；`decode_record`、`ControlFrame`、配置绑定和快照验证处理进一步的语义约束。单独调用 `Wire::decode` 只能证明长度和保留区正确，不能据此授予交易权限。

## 可执行约束

| 编号 | 约束 | 生产入口与验证 |
|---|---|---|
| IPC-C01 | 46 个布局均按固定 offset 小端读写；输入与输出长度必须精确相等 | `Wire`；46 个 `layouts` 测试及 88 个 `vectors` 测试 |
| IPC-C02 | 所有显式 reserved 字节为零，编码器始终写零 | 生成的 `Wire::decode/encode`；逐个翻转全部布局的 reserved 字节 |
| IPC-C03 | 消息类型选择唯一长度与字段白名单；未用联合体字段为零 | `decode_record` 与生成的 `validate_fields`；全部 31 个消息 ID 的静态分支 |
| IPC-C04 | report 数值有效位满足 allowed/required mask；未声明值为零，command/report kind 匹配消息类型 | `validate_fields`；`fill_*` semantic cases |
| IPC-C05 | depth 层数与 payload 长度相等，买价降序、卖价升序、数量非负；≤12 层内联，其余使用 arena | `decode_record`、`validate_arena_payload`；内联和 arena 边界/排序/尾部测试 |
| IPC-C06 | arena 引用匹配 resource/ring/sequence/class，选择能容纳 payload 的最小 class，块与尾部不越界 | `validate_arena_payload`；陈旧 allocation、极大 offset、非零尾部测试 |
| IPC-C07 | 序号不达到 `2^63`，未启用的时间字段和 CRC 为零 | `decode_record`；序号上限与 CRC 开关测试 |
| IPC-C08 | 每个控制帧验证固定包头、完整长度、opcode/flags/status、FD 数和双 CRC | `ControlFrame::decode`；全部控制帧向量、逐字节破坏、每个截断长度 |
| IPC-C09 | RESOURCE_OFFER 数量为 1–16，descriptor 固定 128 字节，每个 FD index 唯一且在范围内 | `validate_offer`；重复 FD index 测试 |
| IPC-C10 | QUIESCE Applied 与 SWITCH Applied/ResyncRequired 使用指定消息体和证据字段 | `ControlFrame::decode`；typed reply semantic cases |
| IPC-C11 | 配置 feature 先准入，再解析包体；身份来自显式上下文，body 不能增加权限 | `decode_negotiated`、`validate_config_binding`；错角色、错 boot、feature 缺失测试 |
| IPC-C12 | 配置提交 revision/hash、目标身份、收据角色和完成证据绑定同一次操作 | `validate_config_binding`；全部配置 semantic cases |
| IPC-C13 | grant、exec 短租约和资源会话分别检查截止时间 | `validate_grant_deadline`；live exec lease 不能覆盖过期 grant 的案例 |
| IPC-C14 | 清单覆盖注册表完整集合，所有子对象位于连续页边界且共享 S/feed/catalog；hash 覆盖全部成员 | `validate_topic_bundle`；两个完整快照向量、缺失标的、截断/篡改测试 |
| IPC-C15 | NormalizedMarketSnapshot 同时验证 state records 与 depth；FeedState 必须存在 | `validate_topic_bundle`；normalized snapshot semantic case |

生成器仅在开发时读取 JSON，输出固定结构、逐字段读写、静态消息分支和测试构造值。运行库不包含 JSON 依赖，也不进行按名称查找。`sha2` 用于不可变快照摘要，记录 CRC 使用明确的 Castagnoli 多项式。crate 禁止 `unsafe`；`Record` 在栈内保存最大消息，避免为 enum 大小差异引入堆分配。

生成代码带机器契约 SHA-256；契约更改后未重新生成会使 `generated_layouts_match_current_contract` 失败。生成命令：

```sh
python3 crates/tickwire-ipc/tools/generate.py wire
python3 crates/tickwire-ipc/tools/generate.py tests
cargo test -p tickwire-ipc
cargo clippy -p tickwire-ipc --all-targets -- -D warnings
```

## TDD 证据

先将接口与生成的 golden 测试写入工作树，再运行测试。首轮因 `MarketRecord`、`RecordHeader` 等类型尚未实现而编译失败，原始日志已归档。随后实现固定布局、消息验证、上下文验证并加入截断和篡改用例。

早期首轮回归为 110 个 Rust 测试通过：36 个固定布局测试、63 个 golden vector 测试、11 个语义与异常输入测试。契约随后扩展（新增布局与消息），当前工作树测试构成以源码为准：`tests/layouts.rs` 46 项、`tests/vectors.rs` 88 项、`tests/semantics.rs` 11 项、`tests/market_exact.rs` 6 项、`tests/layout_extensions.rs` 3 项、`tests/release_layouts.rs` 2 项、`tests/replay_control.rs` 5 项（精确总数与通过数以 `cargo test -p tickwire-ipc` 输出为准）。其中 `all_declared_semantic_cases` 实际遍历并断言契约全部 25 个案例。期望错误只用于测试断言；生产函数根据输入字段、CRC、hash 和上下文独立计算结果。Clippy 使用 `-D warnings` 通过。

## 后续模块的责任

共享内存映射、64 位原子访问、ring 发布与消费、arena 分配/回收、文件 seals、SCM_RIGHTS 收发和 peer credentials 分别由 `tickwire-shm[-sys]` 与 `tickwire-control-sys` 承担。codec 接收稳定 `&[u8]`，不能替代这些访问协议；尤其不能对正在更新的 `ProducerPage`/`CursorPage` 调用整页普通读取。

配置上下文中的证据布尔值表示调用者已经验证外部 artifact 的签发身份和内容；本模块不验证签名、不写配置文件、不提交运行时状态。`CONFIG_STATUS` 的查询/非终态/业务错误尚未在绑定层实现，返回 `UnsupportedOperation`；控制会话去重、修订状态机与恢复也由后续模块实现。

当前资源形状检查不验证真实 FD 的权限、seals 或主机资格；普通 bank 的完整头部准入、instrument 目录全部过滤条件、报单经济规则和 snapshot 生命周期仍需各自模块实施。测试覆盖全部现有向量和 semantic cases，但协议中未提供向量的状态组合仍需补充集成测试。

codec 是确定性的字节转换与检查模块。本阶段没有为它单独声明 TLA+ 证明；并发正确性应由连接到实际 ring、控制状态机和恢复实现的模型验证，不能用布局测试替代。


## 声明重建采集控制

`ReplayControlBody` / `ReplayAppliedBody` 保持原有 256 字节布局，新增 `CaptureSeal=8`。认证 Host 在实际 report 会话上请求封口：`operation_id` 是 capture ID，`operation_revision=1`，`expected_sequence` 是账户完整前缀位置，`expected_cut_digest` 是完整重建 Contract 摘要。对象摘要绑定请求或成功收据，challenge 绑定该次请求。SDK/Host 继续核对 bootstrap、实际 PID、时钟、报告 ring cursor 与持久 WindowSeal；包体可解码只说明格式有效。

kind 8 使用 opcode 41 请求、opcode 42 响应；成功 flags=10，显式拒绝 flags=18/status=1..3（Unsupported/Busy/Invalid）。拒绝回显完整请求。kind 1..5 保持原成功语义；kind 6..7 的 Arm/Commit 不变。未知 kind、越界状态及不允许的拒绝组合均失败。采集成功只证明有界窗口封口，发布操作在窗口固定之后签署。


声明重建安装复用 op41/42 的 ReplayControlBody，kind9（RebuildInstall）。expected_sequence 固定窗口 warm 行数，expected_cut_digest 固定 WindowSeal 摘要，operation_id/revision 固定 schema4 操作；拒绝 flags18/status1..3 与 kind6–8 相同。布局、偏移、保留区不变，增加 7 个独立 golden 向量；此前 74 个向量保持原值。安装的同进程事实与持久输入链由 SDK/Host 检查。

回退完成使用同一控制消息的 kind10（RebuildRollback），expected_sequence 为 0，expected_cut_digest 为原 Installation 摘要，请求对象绑定独立 probe Completion 与完整恢复对象列表。新增 7 个向量覆盖请求、成功及拒绝；与 kind9 合计新增 14 个，原有 74 个向量保持原值，当前共 88 个。
