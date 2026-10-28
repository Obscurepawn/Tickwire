# tickwire-ipc

## 1. 职责

固定布局的 Tickwire wire ABI 编解码层：页面镜像（`ProducerPage`/`CursorPage`/`ResourceHeader`）、完整记录（行情/精确事件/意图/报告/所有权）、控制帧、topic 快照束与配置绑定的**纯字节转换与校验**。它是数据面零拷贝广播链路最底层的"字节字典"——所有跨进程共享内存、控制 socket、快照文件都先由这里定义固定 offset 的小端布局，但本 crate **不触碰实时共享内存**，不做内存映射，也不授予任何交易权限。

- 源码：`src/wire.rs`（固定布局与消息校验）、`src/config.rs`（配置控制帧绑定）、`src/market_exact.rs`（精确行情事件与 `DecimalWire`）、`src/snapshot.rs`（资源形状与快照束校验）。
- `#![forbid(unsafe_code)]`；唯一运行依赖 `sha2`，无 JSON/运行时按名查找。

## 2. 公共 API 入口

-  trait `Wire`（`const SIZE: usize`、`decode`、`encode`）；`Error`/`Result`。
- 记录侧：`Record`（`Market`/`MarketExact`/`Intent`/`Report`/`Ownership` 五变体，栈内保存最大消息）、`decode_record`、`validate_depth_payload`、`validate_arena_block`、`validate_arena_payload`。
- 控制帧：`ControlFrame`（`decode`、`decode_negotiated`）。
- 布局常量：`WIRE_LAYOUT_COUNT`、`WIRE_CONTRACT_SHA256`；布局类型 `RecordHeader`、`MarketRecord`/`MarketBody`/`DepthLevel`、`MarketExactRecord`/`MarketExactBody`/`DecimalWire`、`IntentRecord`/`TriggerRef`、`ReportRecord`、`OwnershipRecord`、`ResourceHeader`/`ArenaClass`/`ArenaBlockHeader`、`SnapshotHeader`/`SnapshotSection`/`TopicSnapshotEntry`/`InstrumentEntry`/`DepthCoverage`、`ControlEnvelope` 及各控制体（`JoinBody`、`FreezeBody`、`SwitchBody`、`QuiesceBody`、`GrantBody`、`ExecLeaseBody`、`ConfigControlBody`、`ReplayControlBody` 等）。
- `crc32c`（Castagnoli，反射多项式 0x82f63b78，init/xorout 0xffffffff）。
- `market_exact`：`EXACT_MARKET_EVENTS = 1 << 9`。
- `snapshot`：`validate_resource_shape`、`validate_topic_bundle`、`validate_depth_coverage`。
- `config`：`ConfigContext`、`validate_config_binding`。

## 3. 在 workspace 中的位置

- 上游依赖：无 `tickwire-*` 依赖（最底层 codec）；外部仅 `sha2`。
- 下游消费者（已按各 `Cargo.toml` 核对）：`tickwire-shm`、`tickwire-shm-sys`、`tickwire-snapshot`、`tickwire-control`、`tickwire-control-sys`、`tickwire-control-store`、`tickwire-join`、`tickwire-market-publisher`、`tickwire-market-source`、`tickwire-md`、`tickwire-mock`、`tickwire-strategy-example`、`tickwire-strategy-host`、`tickwire-strategy-sdk`、`tickwire-supervisor`、`tickwire-lab`、`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/ipc-codec.md](../../specs/implementation/ipc-codec.md)
- 机器契约：[../../specs/contracts/ipc-wire-v1.json](../../specs/contracts/ipc-wire-v1.json)（46 个布局）、[../../specs/contracts/ipc-vectors-v1.json](../../specs/contracts/ipc-vectors-v1.json)（74 个 golden 向量 + 25 个 semantic case）；校验脚本 [../../scripts/verify-ipc-contract.py](../../scripts/verify-ipc-contract.py)。

## 5. 构建与测试

```sh
cargo build -p tickwire-ipc
cargo test -p tickwire-ipc
cargo clippy -p tickwire-ipc --all-targets -- -D warnings
cargo fmt --check -p tickwire-ipc
```

全部测试为纯字节、跨平台可跑（无 Linux-only 用例，无 bench）。测试分布：`tests/layouts.rs`（46 个布局）、`tests/vectors.rs`（74 个 golden 向量）、`tests/semantics.rs`（11 个语义/异常）、`tests/market_exact.rs`、`tests/layout_extensions.rs`、`tests/release_layouts.rs`、`tests/replay_control.rs`；`tests/fixtures/exact-events/*.hex` 为精确事件样本。

布局由开发时生成器产出（`tools/generate.py wire|tests`），运行库不含生成器。

## 6. 关键不变量与注意事项

- **ABI 锁定**：schema major 必须为 1、minor 必须为 0（`schema()`，lib.rs:80）；任何布局字段 offset/size/保留区改动都必须同步 `ipc-wire-v1.json` 并重跑生成器；生成代码携带 `WIRE_CONTRACT_SHA256`，契约改了没重新生成会使 `generated_layouts_match_current_contract` 失败。
- 序号（sequence/feed_generation/producer_generation/instrument_version/allocation_seq）必须 `< 2^63`，否则 `SequenceExhausted`；未启用的时间字段（flags 位 0/1/2）必须为零；未开 CRC（flags 位 8）时记录/载荷 CRC 必须为零。
- 深度：买价降序、卖价升序、数量非负，层数 = bid+ask ≤ 4096；≤12 层内联（payload_kind=1），其余走 arena class 1..=4（payload_kind=2），`allocation_seq == sequence`，块与尾部零填充，见 `decode_record`/`validate_arena_payload`。
- `validate_arena_block` 只校验**一个完整不可变块**，绝不跨可变邻居取切片；arena 引用必须匹配 resource/ring_epoch/allocation_seq/class，并选择能容纳 payload 的最小 class。
- 控制帧：magic `TWCTL001`、头 128 字节、`fd_count ≤ 16`、双 CRC（头 CRC 跳过 116..120，体 CRC）；只有 opcode 3（RESOURCE_OFFER）可携带 FD；offer descriptor 1..=16、每个 128 字节、fd_index 唯一且在范围内。
- ReplayControl kind6/7 为市场 Arm/Commit，复用固定 256 字节包体；expected_sequence=0、expected_cut_digest 绑定审批计划摘要。这些类型的 opcode42 可返回 flags18，status 限 Unsupported=1/Busy=2/Invalid=3；拒绝体回显请求，由 SDK/Host 核对身份与内容，不产生 Armed/Activated。
- ReplayControl kind8 为声明重建 CaptureSeal：expected_sequence 绑定账户前缀位置，expected_cut_digest 绑定 Contract 摘要，operation_id 绑定 capture ID。复用 op41/42 与 256 字节布局；允许同样的 flags18/status1..3 显式拒绝，kind1..5 保持成功语义。实际 PID、会话、窗口重算及持久状态由 SDK/Host 核对。
- 配置帧先检查协商 feature 位（bit 32）再解析包体（`decode_negotiated`/`validate_config_binding`）；身份全部来自显式 `ConfigContext`，包体不能增加权限；本模块不验签、不写文件、不提交运行时状态。
- 单独 `Wire::decode` 只证明长度和保留区正确，**不能据此授予交易权限**；也不能对正在更新的 `ProducerPage`/`CursorPage` 做整页普通读取（那是 shm/shm-sys 的责任）。
- `Record` enum 在栈内保存最大消息，避免堆分配；生产路径不得新增会扩大 enum 的变体而破坏此假设。

## 7. 改动检查清单

1. 改布局/消息/控制体前先改 `specs/contracts/ipc-wire-v1.json` 与 `ipc-vectors-v1.json`，运行 `python3 crates/tickwire-ipc/tools/generate.py wire` 与 `... generate.py tests`。
2. `python3 scripts/verify-ipc-contract.py`（或在仓库根按脚本约定运行）校验契约自洽与 golden 向量。
3. `cargo test -p tickwire-ipc && cargo clippy -p tickwire-ipc --all-targets -- -D warnings && cargo fmt --check -p tickwire-ipc`。
4. 布局/语义改动会影响所有下游（shm、snapshot、join、control、market-source 等），逐一确认解码侧假设；本 crate 无独立 TLA+ 模型，并发正确性由接入 ring/状态机的模型承担，布局测试不能替代。
