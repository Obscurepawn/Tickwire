# Tickwire TLA+ 验证

`execution_models/` 保存执行提交与经济事实、共享 ring、控制会话、物理日志和盘口状态的分层模型。每级保存输入、源码映射、抽象依据、配置、运行证据和结论。各层当前结果及待验证范围见 [ROADMAP](ROADMAP.md)。

从仓库根目录运行：

```sh
./scripts/check-tla.sh
```

脚本下载并核验 TLC 1.7.4 的固定哈希，逐一运行执行阶梯中的全部配置，固定单 worker，启用 action coverage。任一 TLC 运行失败或动作 generated 为零都会返回非零；各级运行报告保存完整覆盖计数。jar 和机器路径配置不提交到仓库。

`PASS` 只覆盖该层列明的源码、状态域和故障边界。模型 action coverage 表示规格里的动作被触发，与 Rust 源码执行覆盖率是不同的统计。`02_models/anchors.lock` 保存源码锚点哈希；源码变化后先检查模型是否需要同步，再重新运行。
