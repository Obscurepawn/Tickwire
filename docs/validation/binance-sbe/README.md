# SBE codec 与真实深度 owner

固定 schema 与来源见 [interface](../../implementation/binance-sbe-interface.md)，实际完成范围见 [spec](../../../specs/implementation/binance-sbe.md)。

- `codec-red.log`：编译通过后 3 项行为失败，固定帧/整数入口尚未实现。
- `native-green.log`：14 项 codec 测试通过，含四种独立字节样本、所有截断前缀、组宽度与预算、精确整数换算和 depth20 覆盖范围。`generator-check.log` 记录常量和四份样本再生成完全一致。
- `typed-red.log` 首轮含测试 hex 读取错误，不作为 codec 缺陷；修正 fixture 读取后，`typed-behavior-red.log` 两项均为真实行为失败。`typed-green.log` 为新增 2 项与原 DepthSync 18 项通过。
- `network-red.log`：真实 WSS owner 仍使用 JSON 入口时两项失败。`network-green.log` 记录实际 SBE Upgrade/Binary/HTTPS 首次通过；`network-live-delta-green.log` 进一步包含基线安装后的 Binary。
- `public-owner-green.log`：既有双产品 4 个真实协议测试、新增 2 个 SBE 配置边界与 2 个真实 WSS/HTTPS 场景全部通过。
- `publication-compile.log` 是复用测试缺少 dev-dependency 的编译错误，`publication-compile-green.log` 是补齐后的原生编译。实际 memfd 发布测试仅 Linux 执行，本次尚待主线程统一运行。

针对性命令：

```sh
cargo test -p tickwire-binance-sbe
cargo test -p tickwire-binance --test sbe_depth --test depth_sync
cargo test -p tickwire-binance-public --test sbe_config --test sbe_network --test network
cargo test -p tickwire-md --test sbe_publication
```
