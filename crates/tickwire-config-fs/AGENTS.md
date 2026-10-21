# AGENTS.md — tickwire-config-fs

## 职责

配置面的 Linux 文件持久化边界：从受信父目录 FD 打开私有根目录，提供描述符相对的 `Store`（配置文件安装/读取）与 `JournalStore`（物理 journal 的分段文件后端）。只产生文件持久化结果，不解释配置、不授予部署或运行时权限；非 Linux 平台返回 `Unsupported` 占位。

## 公共 API 入口

- `Store`（`src/linux.rs`，仅 `target_os = "linux"`）：
  - `Store::open_at(parent: File, name: &str, owner: u32) -> Result<Self>`。
  - `create_directory(path)`、`read(path, max_bytes) -> Vec<u8>`、`install(path, bytes, expected_previous: Option<Hash>) -> Result<DurableFile>`、`is_poisoned() -> bool`。
- `DurableFile { hash(), bytes(), device(), inode() }`（安装成功后的文件事实：SHA-256、字节数、设备、inode）。
- `JournalStore`（`src/journal.rs` / `src/linux/journal.rs`）：`JournalStore::new(store, limits)`，实现 `tickwire_journal::store::Store`；`JournalLimits { max_segment_bytes=16MiB, max_segments=128, max_total_bytes=256MiB }`。
- `Hash = [u8;32]`、`MAX_FILE_BYTES = 8MiB`；`Error::{Io, Invalid, Conflict, Poisoned, Unsupported}`。
- 非 Linux：`Store`/`JournalStore` 为占位结构体，全部方法返回 `Unsupported`。
- 本 crate 在 Linux 专用模块内使用 `#[allow(unsafe_code)]`（目录 FD/openat 边界），其余遵循 workspace `forbid(unsafe_code)`。

## 在 workspace 中的位置

上游 `[dependencies]`：`tickwire-journal`；第三方 `libc`、`sha2`。

下游消费者（全仓 grep `Cargo.toml` 核对）：运行时 `tickwire-config-materializer`；`tickwire-config-journal` 仅在 `dev-dependencies` 中使用它做真实文件测试。

## 对应规格

- [../../specs/implementation/config-file-store.md](../../specs/implementation/config-file-store.md)（私有目录、0700/0600、临时文件→回读校验→sync→rename→目录 sync、旧字节 SHA-256 比较、独占锁）
- [../../specs/implementation/config-journal-store.md](../../specs/implementation/config-journal-store.md)（`JournalStore` 分段、manifest 旧值严格检查、多段恢复同步顺序、SIGKILL 故障注入结果）

## 构建与测试

```sh
cargo build -p tickwire-config-fs
cargo test -p tickwire-config-fs
cargo clippy -p tickwire-config-fs --all-targets -- -D warnings
cargo fmt -p tickwire-config-fs
```

`tests/`：`files.rs`、`journal.rs`、`journal_process.rs`，全部带 `#![cfg(target_os = "linux")]`（真实文件权限、rename、分段恢复与 SIGKILL 子进程），macOS 上编译为空；在 `./scripts/check-linux.sh` 的 Docker 内执行。

## 关键不变量与注意事项

- **私有根目录与锁**：父目录属 root 或指定 UID、group/other 无写权限；根目录及下级目录要求指定 UID、0700、同一设备；逐级相对打开拒绝符号链接；文件要求普通文件、0600、单硬链接、≤8 MiB；根目录 `LOCK` 持有独占内核锁（`linux.rs`、`config-file-store.md`）。
- **安装流水线**：先比较实际旧字节 SHA-256，再独占创建随机临时文件、完整写入、回读校验、sync 文件、同目录 rename、sync 目录；最终核对目标 inode、完整字节、目录身份与 LOCK 身份才返回 `DurableFile`。精确重复重新同步；rename 后失败关闭 Store，调用者重开并依据 journal 恢复。
- **漂移即拒绝**：根目录/子目录/锁文件被替换、属性漂移、外部替换文件即使字节相同也报告漂移；恢复按明确路径读取、忽略临时文件、不按 mtime 选版本。
- **JournalStore 严格旧值检查**：manifest 安装先比较当前摘要与后端此前观察摘要再允许精确重复；分段名只接受二十位十进制编号，拒绝链接、异常属主/模式、跨设备文件；恢复先截断最后一段再按序同步完整前缀（`config-journal-store.md`）。
- **写盘不确定失败即 poison**：追加在实际 offset 写入并持有同一 FD 到 sync 结束；写入/截断/sync 不确定失败后关闭 Store，不拼接旧读取与新同步结果。
- 本层只产生文件持久化事实，控制提交、运行时 ConfigApply、物化流程由上层负责。

## 改动检查清单

1. `cargo build -p tickwire-config-fs` 通过；Linux 行为改动在 `./scripts/check-linux.sh` Docker 内跑 `cargo test -p tickwire-config-fs`。
2. 文件安装/锁/权限规则联动 `config-file-store.md`；分段/manifest/恢复顺序联动 `config-journal-store.md` 与 `tickwire-journal` 协议。
3. 新增路径或文件类型必须同步 FD 相对打开、symlink 拒绝与属主/模式校验。
4. 本 crate 不做配置校验、不读 schema、不签发授权。
