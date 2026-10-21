# tickwire-shm-sys

## 1. 职责

Linux 共享映射与 ring SDK 的**小型审计过的 unsafe OS 边界**：memfd 创建、seals 加锁、单写/只读映射、64 位原子访问、不可变资源封页与校验。它位于数据面零拷贝广播链路的最底层物理层，上承 `tickwire-shm` 的协议状态机，下接内核 `memfd`/`mmap`/`fcntl(F_ADD_SEALS)`。本 crate 是 workspace `unsafe_code=forbid` 之外显式放行的审计边界（`#![deny(unsafe_op_in_unsafe_fn)]`），其他 crate 不自行开放同类能力。

- `src/lib.rs`：`Mapping` 底座、`WriterMapping`、`ReaderMapping`、Linux `platform` 模块与非 Linux `unsupported` 桩。
- `src/immutable.rs`：`ImmutableMapping`（0x0f 不可变封页协议）。
- `src/resource.rs`：`ImmutableResource`（资源头 + 内容与认证期望元数据比对）。

## 2. 公共 API 入口

- 常量：`PAGE_BYTES = 4096`、`ISOLATION_BYTES = 128`、`SINGLE_WRITER_SEALS = 0x17`、`IMMUTABLE_SEALS = 0x0f`。
- `WriterMapping`：`create(name, len)`（私有零填充 memfd + 唯一可写映射）、`seal_for_readers()`、`duplicate_fd()`、`len()`/`is_empty()`，unsafe 方法 `read_bytes`/`write_bytes`/`load_u64_acquire`/`store_u64_release`。
- `ReaderMapping`：`map(fd, expected_len)`、`len()`/`is_empty()`。
- `ImmutableMapping`：`create(name, bytes)`（先写后 seal，只保留 O_RDONLY 描述符与只读共享映射）、`map(fd, expected_len)`、`bytes()`、`read_bytes()`、`duplicate_fd()`、`validate_resource()`；静态辅助 `write_all`/`seal`/`check_seals`/`readonly`。
- `ImmutableResource`：`header()`、`bytes()`、`content()`（跳过首页资源头）、`duplicate_fd()`。
- `Error`/`Result`（`UnsupportedPlatform`/`WrongSeals`/`NotSealed`/`Misaligned`/`OutOfBounds`/`LengthMismatch`/`Wire`/`System` 等）。

## 3. 在 workspace 中的位置

- 上游依赖：`libc`、`sha2`、`tickwire-ipc`（`ResourceHeader` 等布局与 `Wire` 校验）。
- 下游消费者（已核对 Cargo.toml）：`tickwire-shm`、`tickwire-join`、`tickwire-snapshot`（仅 Linux dev-dependency）、`tickwire-strategy-sdk`。

## 4. 对应规格

- [../../specs/implementation/immutable-resource.md](../../specs/implementation/immutable-resource.md)（0x0f 不可变资源）。
- [../../specs/implementation/shm-mapping.md](../../specs/implementation/shm-mapping.md)（memfd、权限、seals、原子访问）。

## 5. 构建与测试

```sh
cargo build -p tickwire-shm-sys          # macOS 可编译（unsupported 桩返回 UnsupportedPlatform）
cargo test -p tickwire-shm-sys          # 真实用例全部 Linux-only
cargo clippy -p tickwire-shm-sys --all-targets -- -D warnings
cargo fmt --check -p tickwire-shm-sys
```

- `tests/immutable.rs`（11 项）、`tests/mapping.rs`（8 项）、`tests/process.rs`（2 项）均带 `#![cfg(target_os = "linux")]`：memfd、0x17/0x0f seals、映射权限、跨进程继承与封页校验。
- macOS 上只编译不运行真实内核测试；Linux 实测必须走 [../../scripts/check-linux.sh](../../scripts/check-linux.sh)（固定 rust:1.99.1-bookworm Docker 镜像、`TICKWIRE_BENCH_SAMPLES`、全量 `--all-targets`）。
- 无 bench。

## 6. 关键不变量与注意事项

- **seals 两套协议**：单写 bank 用 `SINGLE_WRITER_SEALS=0x17`（F_SEAL_FUTURE_WRITE 等，写者写完后 `seal_for_readers` 再交付只读 FD）；不可变快照用 `IMMUTABLE_SEALS=0x0f`（GROW/SHRINK/WRITE，先 `pwrite` 全部内容再 seal，成功 map 证明物理不可变）。
- **长度校验**：长度非零、为 4096 整数倍、≤ isize::MAX（`validate_length`）；`map` 在观察到 GROW/SHRINK seals 后再次核对长度。
- **原子宽度与对齐**：64 位原子访问要求 8 字节对齐（`check_atomic`），否则 `Misaligned`；`store_u64_release` 前 payload 必须完整，`load_u64_acquire` 后接 acquire fence；调用者保证跨进程发布/回收协议与一致的访问宽度。
- **所有权**：`Mapping` 唯一持有 FD 与指针，刻意不实现 `Clone`/`Sync`，`NonNull` 阻止 `Send`，把低层所有权固定在单线程；`duplicate_fd` 以 CLOEXEC 复制。
- **成功 map 消费 FD**：成功与失败路径都消费描述符；`ImmutableMapping::map` 只证明物理不可变，**不证明 Tickwire 身份或 JOIN 准入**，身份由上层认证。
- 非 Linux 平台所有系统调用返回 `UnsupportedPlatform`，不得在桩里伪造成功。
- 本 crate 不导出对原子的引用/指针；新增 unsafe 访问必须同时满足边界检查 + 调用者协议保证，并在注释中写明 SAFETY 依据。

## 7. 改动检查清单

1. 改 seals/映射/原子协议前先读两份规格，确认 0x17 与 0x0f 语义不被混用。
2. macOS 仅保证编译；真实行为用 `./scripts/check-linux.sh` 在 Docker 内全量回归（含本 crate 三个测试文件）。
3. 新增 unsafe 函数保持 `unsafe_op_in_unsafe_fn` 语义，clippy `-D warnings` 通过。
4. 任何布局/封页字节变化联动 `tickwire-ipc` 的 `ResourceHeader` 校验与上层 `ImmutableResource` 元数据比对。
