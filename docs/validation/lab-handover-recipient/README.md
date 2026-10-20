# 生命周期接收者代次修复

真实 Linux handover 在旧、新策略均完成账户位置 3 后，被 `lifecycle recipient identity` 拒绝。生成器把物理 WAL 的 `writer_epoch=4` 用于 `Member.exec_epoch`，而执行端根据 `Deployment.policy.owner_generation=1` 核验接收者。

生成器现在从实际 Deployment 读取 owner、风险域、owner 代次和时钟域。物理 WAL 的 writer 代次保持原值，执行端身份校验未修改。

`native-red.log` 是生成器纯单元的真实失败（4 != 1）；`native-green.log` 为修复后 11 项 lab 单元通过。真实场景由主线程统一重新生成并运行。
