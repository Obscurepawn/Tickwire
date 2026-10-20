# 账户恢复编码验证

接口位于 [account-codec-interface.md](../../implementation/account-codec-interface.md)，完整字段与边界见[实现规格](../../../specs/implementation/account-codec.md)。

`baseline-red.log` 保留接口可编译但 6 项行为失败的输出；`records-red.log` 保留恢复记录接口的 3 项行为失败。`records-fixture-first.log`、`records-fixture-deadline.log` 单独记录测试装置的名称冲突和过期时间输入，未作为实现缺陷。`green-initial.log` 是实现初期的类型别名编译问题。

`native.log` 包含单元、协议边界和真实文件重放测试。`reference_vectors.py` 独立声明输入并实现递归 CBOR 编码，不调用 Rust 编码器；生成结果保存在 `vectors/` 和 `reference-vectors.log`。输入单位与字段分别来自测试构造的 Spot 和 USD-M 账户。

```sh
python3 docs/validation/account-codec/reference_vectors.py
cargo test -p tickwire-account-codec --all-targets
cargo clippy -p tickwire-account-codec --all-targets -- -D warnings
cargo fmt -p tickwire-account-codec -- --check
```

文件集成测试使用实际同步的文件对象与 WAL，重新打开后逐项核对账户资源。流水线中的授权和发送提交组合仍由后续执行模块负责。源码摘要记录具体检查版本；日志末尾空白行统一清理。

## 整仓检查

本批账户、恢复编码及 Spot 请求扩展从暂存区导出后独立执行 `scripts/check-rust.sh` 与 `scripts/check-linux.sh`。macOS 汇总 541 项通过，Linux 汇总 614 项通过，均为零失败；总数含子进程辅助入口报告。显式忽略项是由父测试按需调用的辅助入口，未作为独立成功用例计入。两平台格式、doctest 与 Clippy 检查通过。

[macOS 输出](macos-workspace.log)、[Linux 输出](linux-workspace.log)与 [results.json](results.json)保存结果及环境。`workspace-sources.sha256` 覆盖实际受检的 183 个源码、锁文件和检查脚本，均与提交内容逐字节核对；`source-hashes.json` 另列本模块实现、测试与规格的摘要。此检查目录没有包含尚在开发的流解析代码。
