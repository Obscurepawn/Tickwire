# 一键脚本复跑

`./scripts/run-core-chain-linux.sh` 在独立的新 Linux volume 中完成运行，退出码为 0。[运行元数据](execution.json)保存命令与卷名，[完整输出](console.log)保存构建及最终结果。

源码在本轮构建和运行期间固定，与 [run7 摘要](../run7/workspace-sources.sha256)一致。最终仍为成交 0.001 BTC、BTC 余额 1.001、USDT 余额 940；4 条账户规范记录逐条镜像一致，策略实际消费到账户水位 4。结果见[账户重放](account-report.json)和[验证输出](run-result.json)。

本轮同样停在成交已入账、独立手续费核对尚未完成的边界，具体含义见 [run7 说明](../run7/README.md)。脚本退出后已回收其容器，原始部署文件和日志保留在 `tickwire-lab-20261020T192610Z-64558`。
