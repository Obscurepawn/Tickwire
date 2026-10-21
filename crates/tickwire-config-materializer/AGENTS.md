# AGENTS.md — tickwire-config-materializer

## 职责

配置面本地物化层：把严格投影对象、目录 FD 文件后端（`tickwire-config-fs`）与本地四阶段日志（`tickwire-config-journal`）连接成 `Local`，落盘 desired/effective 文件并补记业务事件。只保存提供给它的观察事实，不验证控制提交、不验证运行时应用来源、不启动进程、不开放交易闸门、不签发集群操作成功。

## 公共 API 入口

- `trait Files { create_directory(path) / read(path, max_bytes) -> Option<Vec<u8>> / install(path, bytes, previous: Option<Hash>) -> FileFact }`（`src/lib.rs`）；为 `tickwire_config_fs::Store` 提供内置 `impl Files`（NotFound 映射为 `None`，install 转成 `FileFact`）。
- `Local<F: Files, S: JournalStore>`（`src/local.rs`）：
  - `Local::open(files, journal) -> Result<Self>`（核对全部引用对象、历史 last-good 与当前文件后打开）。
  - `prepare(prepared: Prepared, object: &ProjectionObject) -> Receipt`。
  - `write_desired(id: Identity) -> Receipt`。
  - `observe_runtime(id: Identity, fact: RuntimeFact) -> Receipt`。
  - `write_effective(id: Identity) -> Receipt`。
  - 观察：`journal() -> &Journal<S>`、`is_poisoned() -> bool`。
- `Error::{Invalid, Conflict, Drift, Poisoned, File, Journal, Configuration}`。

## 在 workspace 中的位置

上游 `[dependencies]`：`tickwire-config`、`tickwire-config-journal`、`tickwire-config-fs`、`tickwire-journal`。

下游消费者（全仓 grep `Cargo.toml` 核对）：当前无运行时消费者（本地物化层待配置 agent 接入）。

## 对应规格

- [../../specs/implementation/config-materializer.md](../../specs/implementation/config-materializer.md)（投影对象+文件后端+四阶段日志连接、双收据一致、last-good 不可变保留、poisoned 语义）

## 构建与测试

```sh
cargo build -p tickwire-config-materializer
cargo test -p tickwire-config-materializer
cargo clippy -p tickwire-config-materializer --all-targets -- -D warnings
cargo fmt -p tickwire-config-materializer
```

`tests/sequence.rs` 用内存 fake Files 跑四阶段顺序/重试（跨平台）；`tests/linux.rs` 带 `#![cfg(target_os = "linux")]`，用真实私有目录/FD 后端验证文件同步与漂移，在 `./scripts/check-linux.sh` Docker 内执行；共享 fixture 在 `tests/common/`。

## 关键不变量与注意事项

- **双收据一致，禁止假成功**：`write_desired`/`write_effective` 先 `files.install` 拿到 `FileFact`，核对 `render_hash == 渲染 render_hash` 且 `bytes == 实际长度`，不符返回 `Error::Drift` 且不写业务记录；核对通过后才 `journal.record`（`local.rs::write_desired/write_effective`、`config-materializer.md`）。
- **链式阶段**：desired 引用原 Prepared 记录；effective 引用原 RuntimeObserved 记录；两文件都由固定渲染器生成，再经文件同步/rename/目录同步返回文件事实，最后写对应业务事件。
- **last-good 不可变**：每个 effective 版本先生成不可变 last-good 副本，保留全部有界历史；引用对象与 last-good 预算 512 MiB，预留三份最大 8 MiB 文件空间（desired/effective/未完成 last-good）。
- **open 全量核对**：已存在记录时，当前文件的实际字节、长度、设备、inode 全同才接受；唯一活动操作的下一阶段允许出现尚未记日志但与原事件重新渲染完全一致的文件，重试重新同步并补记原记录；历史重试只返回原记录，不把文件覆盖回旧版本。
- **失败即 poisoned**：任何持久步骤或恢复核对失败，`Local` 进入 poisoned，再次推进需重开日志和文件；非法参数在写盘前拒绝，不改进度。缺 last-good、缺对象、内容漂移、当前 inode 被替换均拒绝恢复；文件时间不参与版本选择。
- 本层不做控制提交核验与运行时来源核验（属配置 agent 认证入口）。

## 改动检查清单

1. `cargo build -p tickwire-config-materializer && cargo test -p tickwire-config-materializer` 通过；文件行为改动另在 Docker 跑 `tests/linux.rs`。
2. 渲染器/双收据/last-good 规则联动 `config-materializer.md`；阶段链联动 `tickwire-config-journal` 的 `config-local-journal.md`。
3. 预算（512 MiB 对象/last-good、三份 8 MiB 文件）改动同步 `capacity` 检查与规格。
4. 本 crate 不验证控制授权、不验证运行时事实、不启动进程。
