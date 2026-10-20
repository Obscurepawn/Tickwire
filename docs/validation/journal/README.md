# Journal 实测证据

本目录保存 `tickwire-journal` 的真实 Red/Green 日志。完成范围是物理帧、批同步和文件恢复；业务恢复闭包与执行授权尚未接入。

| 文件 | 内容 |
| --- | --- |
| `frame-red.log` | 帧接口桩的 5 项失败行为测试 |
| `batch-red.log` | 批接口桩的 7 项失败行为测试 |
| `directory-red.log` | 源审建议引入的真实目录权限失败回归 |
| `native-green.log` | macOS 上最终 21 项测试及四个 SIGKILL 边界 |
| `linux-green.log` | Linux aarch64 上相同 21 项测试与 SIGKILL |
| `clippy-green.log` | 最终 crate 的 Clippy 检查 |
| `environment.json` | 环境、命令、源码和测试 SHA-256 |

日志中的本机绝对路径用 `$REPO` / `$TMP` 替换；错误、测试数量、耗时和进程输出保持原样。子进程入口 `crash_child` 在普通 test 发现阶段标记 ignored，父测试通过 `--ignored --exact crash_child` 四次真正运行它，因此不是跳过故障场景。

本机复跑：

```bash
cargo test --locked -p tickwire-journal -- --nocapture
cargo fmt -p tickwire-journal -- --check
cargo clippy --locked -p tickwire-journal --all-targets -- -D warnings
```

Linux 复跑使用仓库已经固定的镜像；独立 target 目录避免与本机产物混用：

```bash
docker run --rm --init \
  --mount "type=bind,source=$PWD,target=/work" \
  --mount "type=bind,source=$PWD/.tools/linux-cargo-registry,target=/usr/local/cargo/registry" \
  -w /work -e CARGO_TARGET_DIR=/work/target/linux-journal \
  -e RUSTUP_TOOLCHAIN=1.99.1 \
  rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29 \
  bash -c 'uname -srmo; rustc --version; cargo test --locked -p tickwire-journal -- --nocapture'
```

调用计数测试核对每批一个 `sync_segment`，FileStore 对应 `File::sync_data`；段建立和 manifest 切换另做文件与目录同步。故障注入在真实写入一半后返回错误，或完成真实 manifest rename 后模拟目录同步失败。没有真实断电实验或延迟分位数测量。
