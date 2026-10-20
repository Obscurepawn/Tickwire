# 配置候选验证

本阶段实现 `tickwire-config` 的严格 TOML/JSON 解析、规范字节、固定 schema、静态资源校验、逐进程权限投影和变更分类。

- [初始行为失败](red.log)：1 项通过、5 项失败，暴露重复键、UTF-16 键序、标量类型、容量与缺失结构校验。
- [静态入口失败](static-red.log)：6 项失败，入口尚未执行拓扑、预算与期限检查。
- [负零失败](negative-zero-red.log)：JSON 整数 `-0` 经解析器浮点分派后被误拒，新增词法归一化保留整数契约。
- [最终测试](final-tests.log)：13 项通过。
- [Clippy](clippy.log)：全部 targets、`-D warnings` 通过。
- [投影初始失败](projection-red.log)：6 项失败；补齐固定政策读取、实际分配作用域、角色用途上限和独立输出 schema 后通过。
- [参数夹带凭据引用](projection-secret-red.log)：实际投影器曾接受参数中的 `secret://` 引用；新增递归检查，输出中的所有凭据引用必须来自该进程已授权的用途绑定。
- [投影完成后的测试](projection-green.log)：累计 20 项通过，包括策略无凭据、exec/guardian 凭据用途区分、MD 发布者校验、政策字节/revision/目标绑定，以及其他策略的参数不会改变本策略投影。
- [投影 Clippy](projection-clippy.log)：全部 targets、`-D warnings` 通过。
- [变更初始失败](changes-red.log)：6 项失败，随后补齐分类、精确十进制保护方向和完整交付集合。
- [授权回归](changes-authorization-red.log)：角色与账户作用域错误合并，以及未分配政策被账户读者访问，均由实际失败测试复现后修复。
- [参数路径回归](changes-parameter-red.log)：自由参数中的 `risk_budgets` 被误识别为系统预算，收紧数值被判为 Hot；改为按完整结构路径分类，并保留参数数组的每项差异。
- [变更分类完成后的测试](changes-final-green.log)：累计 29 项通过；[对应 Clippy](changes-final-clippy.log) 全部 targets、`-D warnings` 通过。
- [独立审查失败](independent-review/red.log)：7 项真实行为失败，分别涉及逐合约保护、预算新增、整个策略新增、重排与修改并存、自由参数误判、缺失策略 payload 和投影超出预留容量。
- [审查修复后的测试](independent-review/green.log)：累计 36 项通过；[对应 Clippy](independent-review/clippy.log) 全部 targets、`-D warnings` 通过。新增预算和逐合约覆盖要求对应账户的 risk-admin；数组相对顺序独立记录；每个策略进程都有唯一 payload；投影同时受全局上限和候选预留容量约束。生产模式只在部署引用、地址、路径和传输字段检查占位值，参数字符串由参数 schema 解释。
- [角色与标的范围失败](independent-review/role-symbol-red.log)：扩大可交易标的漏掉 risk-admin，RiskAdmin/ClusterAdmin 缺少契约规定的只读权限；修复后保持读取作用域，且不附带 operator 权限。
- [保护方向与产品失败](independent-review/protection-product-red.log)：调高磁盘冻结阈值、放宽请求接收窗口未增加风险授权；Spot 账户还接受了交割合约覆盖。修复后[累计 41 项通过](independent-review/protection-product-green.log)，[Clippy](independent-review/protection-product-clippy.log) 通过。

最初的可编译占位实现保存在 [pre-implementation.rs.txt](pre-implementation.rs.txt)。参考配置的共享内存计算与独立 `deployment-reference-v1.json` 逐项一致。这里的通过范围不含控制 API、运行时热更或文件物化；它们仍按[配置接口](../../implementation/configuration-interface.md)开发。

复跑命令：

```sh
cargo test --locked -p tickwire-config --all-targets
cargo clippy --locked -p tickwire-config --all-targets -- -D warnings
```
