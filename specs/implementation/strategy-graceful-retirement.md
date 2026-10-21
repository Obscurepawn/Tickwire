# 共享行情订阅的正常退休

远程 Host 不先 kill 共享 MD 的订阅进程。它发送认证 report QUIESCE，等待 worker 完成 MD DETACH 和 report cursor 释放，再等待其直接持有的 Child 成功退出。超时返回失败，生命周期保持关闭，新实例不激活。

该流程使用现有布局：subscriber DETACH（21）携带自己的完整 Join 身份，不带虚构水位；MD 发 QUIESCE（20），通过 `JoinOwner::quiesced` 核验真实 cursor；成功后 STATUS（49，Response|Applied）绑定 DETACH 请求 ID、控制代次、membership、ring 和完整 QUIESCED body 的 SHA-256。worker 对该确认逐字段核对后才向 Host 发 report QUIESCED。Host 的 `detach_registered` 再检查实际 report 映射。单纯连接关闭仍不是 QUIESCED 证据。

MD 只对真实 Detached peer 做无全 topic 失效的清理。未完成退出、伪造响应、控制超时和意外 EOF 继续走既有保守故障处理。已退休 Host 不再收到 Multi 回调；其身份仍留在有界历史中，不能重复启动。

针对性 Red/Green 和当前进程验证状态见退休证据。
