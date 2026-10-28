# 运行实例签名事实

`tickwire-config-service::runtime_facts` 校验受信来源对当前配置基线全部进程的观察。结果保存内容与来源身份，供规划器核对目标集合；当前资源登记头、来源水位的持久防回退、配置应用和双收据完成仍由上层独立验证。单份有有效签名的快照不等于实时 registry，不产生 Ready、Succeeded 或交易授权。

启动配置通过 `RuntimeTrust` 把既有 evidence trust 摘要与明确的 issuer、host、process 集合绑定。每个 host 只有一个来源，process 在全部 scope 中唯一；不允许通配符、重复或重叠范围。签发者必须已被底层 trust 授予 `RuntimeTargets` kind。运行时事实使用新的 `runtime_targets` 签名 kind，沿既有 Ed25519 envelope 和 artifact 内容摘要验证，HTTP 请求不能授予来源权限。

校验输入包括真实 committed head 提供的 base Candidate、current revision，以及从同一 base 构造的完整 ProjectionSet。每个 host 一份严格 schema 1 文档，绑定 cluster、namespace、host、host boot、来源 epoch/sequence 和观察时间；该文档只列其实际观察到的进程。所有文档合并后必须精确等于 base 的全部 process 集合，不能漏掉未变更或待退休进程，不能用候选 projection 代替基线。新增目标的 successor 资格由后续规划器验证，不在此伪造当前实例。

逐进程核对 host、role、artifact build hash、完整实例/进程 boot/host boot、PID、exec generation、locator generation、instance slot，以及当前 revision、global hash、projection hash、projection policy revision 和 applied event sequence。UUID 字节及 hash 不得全零；PID 在 Linux 正 PID 范围；版本和序号用规范十进制字符串，取值为 1..2^63-1。每个实例、process boot 和 slot 在集合中唯一。同一 host 的重复文档一律拒绝，防止拼接不同 epoch 的分片。

观察时间不晚于签发时间，签发时间不晚于服务端当前时间；有效期取 envelope 到期与观察时间加 max_age 的较小值，到期时刻即拒绝。max_age 为 1..60000 ms。上层再次使用结果时仍须检查当前 trust、真实 head、来源上下文及新时间，不能把历史核验时间用于新操作。

输入最多 256 个 host 文档、4096 个进程；每份文档最多 4 MiB，总文档字节最多 16 MiB。解析沿配置严格值树，拒绝重复键、未知字段、非法数值和额外内容。`RuntimeTargets` 的字段私有，只有完整检查成功后才能构造；输出摘要绑定 base、revision、启动 scope 与全部实际验签的来源对象和 envelope。引用顺序不影响摘要。

回归使用真实 Ed25519 签名和完整基线投影，覆盖正确集合、错误 signer/scope、跨 host、漏行/重复行/额外行、错误 build/role/实例、替换基线投影、不同 host epoch 拼接、future/expired 时间与容量。生产采集 provider 和持久来源水位未在本模块实现。
