# 私有流校准时钟适配验证

新增本地截止修复及12项时钟组合回归见[local-deadline](local-deadline/README.md)。下文10项双平台结果及本目录 source-hashes.json 保留修复前版本。

`red.log` 保存可编译接口的6项真实行为失败；`green.log` 保存首轮6项通过。随后新增原 stamp 保留、无校准、预算与域变化等边界，最终原生10项通过，记录在 `native-final.log`。`compile.log` 和 `fixture-type-compile.log` 是测试参数类型/类型别名编辑问题，不算行为 Red；`clippy-initial.log` 是测试返回类型复杂度提示。

主集成先使用真实 `BinanceTimeClient` 完成 HTTPS `/api/v3/time` 校准，再短期借用适配器驱动同一生产 `PrivateClient` 在 WSS 上签名订阅。服务器从实收参数重建签名原文并独立核验 HMAC。使用正常测试 CA 和固定测试域名，不绕过证书或主机名校验。

测试显式提供单调时钟和交付预算，不用 SystemTime 作为校准来源。预算回调可推进单调时间或切换域；最后一次 check_send 重新采样，分别拒绝过期和变域。显式 SignedTime 来自校准器，账户 scope 由测试启动方固定。

```sh
cargo test -p tickwire-binance-session
cargo clippy -p tickwire-binance-session --all-targets -- -D warnings
```

Linux 使用 `rust@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29`，挂载仓库到 `/work`，设置 `CARGO_TARGET_DIR=/work/target/linux-private`；同一容器依次执行上述两个命令，原始输出在 `linux-final.log`。最终源码与依赖哈希归 `source-hashes.json`。

最终原生/Linux各10项测试及Clippy通过；运行前后的3个crate文件和14个直接依赖/共用测试文件哈希一致。root独立审阅scope/source/domain、原stamp、预算前后fresh检查和无校准分支，无开放阻断，未重复运行测试。生产适配器源码SHA-256为 `8278292e485dd6721fb81782628d1764ebfdc0e067ce3bde6edc0aac3f4ab4cd`。
