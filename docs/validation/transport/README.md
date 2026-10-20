# 网络传输验证记录

`tickwire-transport` 通过非阻塞 TCP/TLS 驱动本地真实协议服务器。最终包含 25 个测试：13 个真实 socket 测试、12 个协议分帧测试。native 与 Linux 的运行结果、Clippy 检查、环境和源码摘要分别保存在本目录。

## 测试过程

| 记录 | 实际结果 |
|---|---|
| [network-red.log](network-red.log) | 编译通过的接口 stub，6 个网络测试失败；连接尚未实现导致有界等待到期 |
| [framing-red.log](framing-red.log) | HTTP/WS/FIX stub，8 个分帧测试失败 |
| [plaintext-budget-red.log](plaintext-budget-red.log) | 请求每次最多 1 字节，实际解密交付 128 字节；断言失败 |
| [plaintext-eof-red.log](plaintext-eof-red.log) | 加入解密预算后，EOF 提前关闭导致尚未交付明文丢失 |
| [framing-boundaries-red.log](framing-boundaries-red.log) | 超大 chunk 未立即拒绝、HEAD 的长度被当成实际 body 容量，两项失败 |
| [linux-first.log](linux-first.log) | 完整 TLS 冲刷后同次收到 EOF，本地完成记录延后；HTTPS/FIX 两项失败 |
| [framing-eof-red.log](framing-eof-red.log) | EOF 检查 stub 未拒绝未完成片段，失败 |
| [ws-poll-red.log](ws-poll-red.log) | 三态入口 stub 把 33 帧输入的处理预算耗尽误报为缺输入，失败 |
| [native-check.log](native-check.log) | 最终 25 测试 PASS |
| [native-clippy.log](native-clippy.log) | `--all-targets -- -D warnings` PASS |
| [linux-final.log](linux-final.log) | 固定 Linux ARM64 镜像，最终测试与 Clippy |

服务器使用动态 loopback 端口和独立生成的本地证书，客户端只加入对应根证书。错误主机名测试沿正常验证失败。HTTPS 测试验证请求字节和 chunked 响应；WSS 测试验证 Upgrade/Accept、客户端掩码和控制帧穿插的文本片段；FIX 测试把文本分帧放进同一真实 TLS 连接。还有实际填满内核发送缓冲的 WouldBlock、接受后取消/超时、未就绪不调用门禁、零接受结果取走后使用新槽位的用例。

最初网络 stub 的失败证明功能未实现，不承担发送边界证据；随后列出的实际实现回归分别保留失败断言和修复后的测试结果。

market_core 独立审阅 HTTP/WS/FIX 分帧后指出，原 `next_message` 用同一个 `None` 表示缺输入和处理预算耗尽，事件循环可能停等后续就绪。新增 `poll_message` 将这两种结果显式区分。回归先一次推入 33 帧，验证 `Yielded` 后无需新网络数据即可取出消息；再推入半帧，验证仍有缓冲时也能准确返回 `NeedInput`。该变更只涉及 WebSocket 解析器，首次输出实现和 L0 模型保持原冻结版本。

## 复现

```bash
cargo test -p tickwire-transport
cargo clippy -p tickwire-transport --all-targets -- -D warnings
```

Linux 使用以下固定镜像，容器 target 单独保存以避免与其他构建混用：

```bash
docker run --rm --init \
  --mount type=bind,source="$PWD",target=/work \
  --mount type=bind,source="$PWD/.tools/linux-cargo-registry",target=/usr/local/cargo/registry \
  -w /work -e CARGO_TARGET_DIR=/work/target/linux-transport \
  -e RUSTUP_TOOLCHAIN=1.99.1 \
  rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29 \
  bash -c 'set -eu; cargo test -p tickwire-transport; rustup component add clippy --toolchain 1.99.1; cargo clippy -p tickwire-transport --all-targets -- -D warnings'
```

[environment.json](environment.json) 记录工具链和平台；[source.sha256](source.sha256) 固定本 crate 的源码、manifest 和测试。risk_core 独立只读审查首次输出实现及模型，未发现阻断项；其结论不替代实际网络运行日志。

[L0 模型结果](../../../tla/execution_models/transport/L0_first_output/05_output.md) 包含 TLS/普通 TCP、单项/双项报文和更大缓冲四个配置；全部动作有覆盖。模型不包含 TLS 密码学、HTTP/WS/FIX 语法或上层订单业务完成。
