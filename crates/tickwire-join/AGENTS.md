# tickwire-join

## 1. 职责

基于**真实共享内存发布者与不可变快照**的 JOIN 协调：owner 侧登记/封印/交付/激活/退休，consumer 侧接入基线、登记 cursor、推进借用。它是数据面零拷贝广播链路中"新订阅者如何在不丢序号的情况下挂上 ring"的编排层，把 `tickwire-shm` 的 `Publisher`、`tickwire-snapshot` 的不可变基线、`tickwire-control` 的认证会话与 `tickwire-control-sys` 的 SCM_RIGHTS FD 交付串成一条三进程认证链路。`#![deny(unsafe_code)]`，仅 `mapped.rs`  discharge 既有 SDK 合同接缝。

- `src/lib.rs`：`JoinOwner<M>`（owner 状态机与控制帧编排）。
- `src/mapped.rs`：`JoinConsumer<B>`（consumer 侧接入与 cursor 生命周期）。

## 2. 公共 API 入口

- `JoinOwner<const M: usize>`：`new(publisher, registry)`、`header()`、`published()`、`membership_version()`、`publish(records)`、`publish_market(messages)`、`freeze_for_successor(ring_epoch)`、`registry()`、`stage(sub)`、`join(sub)`、`snapshot_header(sub)`、`prepare(...)`、`deliver(...)`、`read_ready(...)`、`join_ready(...)`、`catchup_target(sub)`、`expire(now)`、`quiesce(...)`、`quiesced(...)`。
- `JoinConsumer<const B: usize>`：`load_baseline<T>`、`cursor_offer(...)`、`read_ready_frames(...)`、`join_ready_frame(...)`、`borrow()`、`apply(seq)`、`watermarks()`、`quiesce(session, request, now)`。
- 值类型：`TopicRegistry{catalog_hash, instrument_set_version, instruments}`、`SubscriberRegistration{identity, subscription, qos}`、`JoinPlan{resource, join, sequence}`、`Stage{Reserved, Prepared, Delivered, Active, Failed, Expired, Quiescing, Detached}`、`PrepareOutcome{Prepared{subscription, response}, Completed, Pending}`、`instrument_set_hash(instruments)`、`Error`/`Result`。

## 3. 在 workspace 中的位置

- 上游依赖：`sha2`、`tickwire-ipc`、`tickwire-control`（认证会话/请求登记）、`tickwire-control-sys`（SEQPACKET/FD）、`tickwire-shm`（ring 协议）、`tickwire-shm-sys`（映射/seals）、`tickwire-snapshot`（基线验收）。
- 下游消费者（已核对 Cargo.toml）：`tickwire-market-publisher`、`tickwire-market-source`、`tickwire-md`、`tickwire-strategy-host`、`tickwire-strategy-sdk`、`tickwire-app`。

## 4. 对应规格

- [../../specs/implementation/join.md](../../specs/implementation/join.md)（owner 操作成功/失败条件表、协作退出、待实现项）。
- TLA+ 有限状态模型：[../../tla/execution_models/join_owner/L0_pending_activation](../../tla/execution_models/join_owner/PLAN.md)。

## 5. 构建与测试

```sh
cargo build -p tickwire-join
cargo test -p tickwire-join            # 全部 Linux-only
cargo clippy -p tickwire-join --all-targets -- -D warnings
cargo fmt --check -p tickwire-join
```

- `tests/flow.rs`（6 项）、`tests/owner.rs`（4 项）、`tests/process_linux.rs`（2 项）均带 `#![cfg(target_os = "linux")]`：三进程认证、SCM_RIGHTS、immutable snapshot、pending 满门禁、追平激活、持有 guard 时无法退出。
- macOS 仅编译；真实链路必须用 [../../scripts/check-linux.sh](../../scripts/check-linux.sh) Docker 内运行。
- 无 bench。

## 6. 关键不变量与注意事项

- **串行 owner 域**：owner 使用同步 `&mut self`，consumer 可在独立进程推进 cursor；外部不能绕过它取得发布者。
- **prepare 先保留 S 再构造封印**：已认证 Supervisor、完整可信订阅绑定、有效期限；先实际 `Publisher.join` 保留 pending gate，再构造/封印/保存响应；构造/封印失败保留 pending，重复已登记请求不重新构造、不变成新授权。
- **deliver 分别交付两种只读能力**：bank 用 0x17 单写 seal 的只读能力，snapshot 用 0x0f 不可变 seal；发送失败进入 Failed，已发生的登记保留。
- **read_ready 双资源核对**：offer/header/length/seals/access/generation 与原 membership V 全部匹配才置确认位，否则保持原值。
- **join_ready 条件**：双确认 + 实际 cursor 已登记 + 快照完整且具备必需状态 + applied 已追平同次调用读取的 published；错误正文不覆盖原 JoinBody；ready retry 返回首次激活水位，不更新其他订阅。
- **expire 不释放门禁**：JOIN 到期或时间倒退只把 pending 置 Expired，gate 保留。
- **quiesced 精确匹配**：实际登记 cursor 生命周期 5、quiesced_seq 与最终水位精确一致且无活借用；无法伪造控制正文移除门禁；成功 ACK 幂等。超时与 socket EOF 都没有 detach 权限。
- 累计历史登记数不超过 M，已 detach 条目不自动回收。
- `all_detached()` 只读检查空登记或所有历史项均为 Detached；pending、active、failed、expired 和 quiescing 均阻止目录切换，只有实际 cursor 验证成功后的 quiesced 才释放此条件。owner/flow 回归覆盖各边界与活借用阻止退出。
- 待实现（规格明确）：持久 owner/RequestStore 恢复、代次重用、Supervisor 常驻分发、真实业务状态源、死亡回收、最终 exec 增险授权。

## 7. 改动检查清单

1. 改 owner 阶段迁移/激活条件时，对照 join.md 操作表与 TLA+ `L0_pending_activation` 模型有限范围。
2. 所有测试 Linux-only；本地 macOS 只能编译验证，行为回归走 `./scripts/check-linux.sh`。
3. 控制帧/FD 协议变化联动 `tickwire-ipc::ControlFrame` 与 `tickwire-control-sys`。
4. clippy `-D warnings`、`cargo fmt --check` 通过；不得在 mapped.rs 之外新增 unsafe。
