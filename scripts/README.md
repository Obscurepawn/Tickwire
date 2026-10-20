# 设计契约校验

这些脚本核验设计文件、编码向量和参考配置，不连接 Binance，不读取交易凭据，也不代表 Rust 运行时、真实进程通信或生产资格测试通过。

需要 Python 3.11 或更新版本。在仓库根目录运行：

```bash
python3 -m venv /tmp/tickwire-design-validation-env
/tmp/tickwire-design-validation-env/bin/python -m pip install -r scripts/requirements-design.txt
/tmp/tickwire-design-validation-env/bin/python scripts/verify-design-contracts.py --openapi
/tmp/tickwire-design-validation-env/bin/python scripts/verify-documentation.py
```

| 脚本 | 实际检查范围 |
|---|---|
| [verify-design-contracts.py](verify-design-contracts.py) | 汇总 IPC、风控和配置检查；重新计算部署内存、CPU、存储及延迟合同的跨文件一致性；`--openapi` 用标准验证器检查 OpenAPI |
| [verify-ipc-contract.py](verify-ipc-contract.py) | 布局覆盖、字段对齐、保留字节、类型宽度、编码向量与哈希、帧校验和指定语义案例 |
| [verify-risk-vectors.py](verify-risk-vectors.py) | 使用 Decimal 重算风险案例，并逐项比对预期数值 |
| [verify-config-contract.py](verify-config-contract.py) | 全量与投影 schema、参考 TOML、跨字段约束、接口引用及负例；生产模板含未填写输入，必须拒绝 |
| [verify-documentation.py](verify-documentation.py) | 本地链接文件存在、参考链接定义、代码块闭合、测试计划编号、图片格式、图资产哈希与交互图交付记录 |

`reference.toml` 是模拟环境的结构与容量参考，其中的 fixture 工件、秘密引用和模拟入口没有部署到运行环境。生产模板中的 `REQUIRED` 必须替换为实际输入，并通过完整运行时准入与资格验证。文件解析成功不能替代这些验证。

Rust 实现使用 `./scripts/check-rust.sh` 执行格式、全 workspace 回归（含 example 和基准 smoke）及 Clippy。`cargo run -p tickwire-mock --example spot-roundtrip` 运行本地业务闭环。

`./scripts/check-tla.sh` 校验固定 TLC 1.7.4 jar 的 SHA-256，顺序运行每个模型的全部 cfg 并报告动作覆盖；首次会下载 jar，需要 Java。详细结果与有限域保存在 `tla/`。完整性能基准单独执行 `cargo bench -p tickwire-book --bench depth_apply`，避免把调试构建的基准 smoke 当成性能数据。
