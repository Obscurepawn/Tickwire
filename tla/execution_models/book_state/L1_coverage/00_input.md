# L1 输入：有限价格与覆盖知识

接续 [L0 生命周期](../L0_lifecycle/05_output.md)：有效内容只在完整快照或成功增量时提交；错误身份、stale 不改内容；Invalid 退出需要重建；view 仅在创建时检查 Live 与时效。L1 细化侧内容和知识返回值，保留 generation、生命周期以及容量失败。

源文件 SHA-256 仍为 `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`。

| 源码 | 具体意义 |
|---|---|
| crates/tickwire-book/src/lib.rs:239-260 | Complete、Through、Unknown 的价格区间 |
| crates/tickwire-book/src/lib.rs:574-586 | Book 字段与单侧存储 |
| crates/tickwire-book/src/lib.rs:620-642 | 快照成功安装与覆盖边界 |
| crates/tickwire-book/src/lib.rs:644-710 | 接受增量、净容量及失败提交边界 |
| crates/tickwire-book/src/lib.rs:712-735 | 重建清空与外部失效 |
| crates/tickwire-book/src/lib.rs:737-761 | 创建 view 的门槛，由 L0 具体展开 |
| crates/tickwire-book/src/lib.rs:850-858 | 用最后一个非零价位确定 Through；空 partial 为 Unknown |
| crates/tickwire-book/src/lib.rs:902-929 | quantity 与 best 的不同知识规则 |

`world` 是快照及已接受更新所对应水位的完整侧内容，不是任意墙上时刻的交易所真值。初始/重装快照与 complete 标识准确、接受事件完整描述该版本的变化，是调用者输入契约；当前模型不验证网络适配器。L0、L1 是逐步细化的独立有限检查，未运行机器检查的跨层 refinement 证明。
