# 策略实例退休判定

Host 原先忽略 `Registry::terminate` 的错误，仍向 Multi 返回旧实例已退休。`retire_owned_child` 提取该调用边界后，真实空 Registry 的单元测试在旧吞错行为下失败，见 [red.log](red.log)。修复后，只有终止调用返回实际 `ExitObservation` 才记录 `strategy_retired` 并返回成功，见 [host-green.log](host-green.log)。这项测试验证错误传播，没有注入操作系统 kill/wait 故障。

[multi-green.log](multi-green.log) 检查旧实例退出返回失败时，新实例激活方法未被调用，双方继续暂停；执行端随后通过现有失败处理进入 Halted。该测试使用内存策略回调，不代替实际子进程交接。

生产调用边界为 `MultiStrategy::handover_activate` → 旧 `HostedStrategy` 或 `RemoteHostedStrategy` → `Registry::terminate` → 新实例授权核验和激活。退出观察只证明直接持有的 Child 已退出，不构成共享内存 cursor 的 QUIESCED，也不构成全域 Finalized。

本轮实际命令和返回码见 [commands.json](commands.json)，检查时源码 SHA-256 见 [source-hashes.json](source-hashes.json)。两个 Green 日志是归档时实际重跑的对应单元输出；此前构造 Red 原样保留。

## 共享 MD 的正常退订

实际 Linux 交接中，旧 worker 已退出，新实例却只消费到账户位置 4。日志顺序为旧实例 `strategy_retired`、MD `Closed`、MD 对其余订阅者发 QUIESCE、新实例停止。源码原因是 MD 对任何 peer 错误调用整个 topic 的 `require_rebuild`。完整失败场景保留在 `.tools/handover-recipient-check/source/.tools/lab-results/20261020T195531Z-5118/`；该路径是本次运行目录，未把未提交运行目录当作长期公共证据。

修复后的远程退休顺序为：Host 通过认证 report 连接要求旧 worker 停止；worker 结束全部借用并设置实际 report cursor 为 QUIESCED，再向 MD 发送 DETACH；MD 发出已有 QUIESCE 请求，验证 `JoinOwner::quiesced` 对实际 mapped cursor 的检查成功后移除该订阅，并返回绑定原请求、控制代次和完整 QuiesceBody 摘要的 STATUS Applied。worker 收到该确认才回复 report QUIESCED 并自然退出。Host 验证实际 report cursor，随后等待 Registry 的真实成功退出观察。任一超时或错误都使激活返回失败，不能凭 EOF 释放 gate。

`Stage::Detached` 只在上述真实 cursor 检查成功后出现；该 peer 的移除不再重建整个 topic。意外 EOF 和未取得证据的错误仍触发重建。Multi 成功退休后释放旧 Host 对象，保留已退休身份以处理重复激活。

[md-detach-red.log](md-detach-red.log) 是实际编译成功后的断言失败，检查成功退订不能触发全 topic 重建；[md-detach-green.log](md-detach-green.log) 为修复后的同一单元。协议身份、代次、水位和证据错配测试见 [detach-protocol-green.log](detach-protocol-green.log) 与 [worker-retirement-green.log](worker-retirement-green.log)，各 1 项通过；[graceful-multi-green.log](graceful-multi-green.log) 为 7 项 Multi 单元通过。[graceful-check.log](graceful-check.log) 为 app、MD 和 worker 二进制必要编译通过。随后实际 Linux 进程场景通过：旧实例完成正常退订并退出，新实例继续消费到账户位置 9，镜像核对 9 条记录；原始源码、二进制摘要和进程日志见[真实交接记录](../core-chain/spot-handover/README.md)。该场景使用冻结的 schema1 生命周期；新发布预检的 schema2 场景另行验证。
