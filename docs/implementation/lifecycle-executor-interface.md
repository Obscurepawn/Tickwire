# 策略交接执行端

`tickwire-app` 的可选 `lifecycle` 配置把协调器请求接入实际账户所有者。请求和回执使用 [协调器协议](lifecycle-coordinator-interface.md)；每个执行端只读取本风险域的私有请求目录。`Member.exec_epoch` 对应部署的 `policy.owner_generation`，账户 WAL 的 `writer_epoch` 另行记录。

```json
{
  "requests_directory": "/srv/tickwire/lifecycle/requests/account-a",
  "receipts_directory": "/srv/tickwire/lifecycle/receipts",
  "journal_directory": "/srv/tickwire/lifecycle/exec-wal",
  "stream_epoch": [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],
  "recover": false,
  "segment_max_bytes": 16777216,
  "coordinator_id": [2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2],
  "coordinator_public_key": [3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3],
  "signing_key_pkcs8": "/srv/tickwire/keys/exec-ed25519.pk8",
  "maximum_requests": 1000,
  "maximum_per_poll": 4
}
```

数组仅示意字段长度；部署使用实际身份和公钥。目录权限为 `0700`，协调器按原子重命名发布 JSON 文件。执行端先验证签名、成员、请求身份与本地时钟期限，再持久登记决定。

| 请求 | 实际完成边界 |
|---|---|
| Prepare | 关闭旧授权，停止旧实例意图转发；只终止本进程能够证明零输出的请求；新实例已处理当前账户回报并封存检查点 |
| Commit | 再次核对当前账本、订单摘要及回报水位；完整恢复对象同步后，账户 WAL 同步 `StrategyHandover`，再更新管理者 |
| Release | 校验全部成员的签名组凭据、当前风险闸门版本及本域 Committed 回执摘要；使用另行安装的新策略授权，切换新实例转发 |
| QueryApplied | 读取本地状态并核对实际账户 WAL；可发现账户提交成功、控制状态尚未记录的交接 |
| Halt | 关闭策略授权和转发，推进风险闸门版本，持久化后确认 |

真实账户回报通过 `InputSeal` 顺序累计。检查点封存对象另包含完整策略输入、依赖及账户输入摘要；执行端要求认证子进程的 READY、共享内存实际消费水位、当前账户 WAL 三者一致。新成交使旧捕获过期时，协调器刷新 readiness 后重新 Prepare。

交接只改变订单管理者及管理版本。原订单身份、逻辑归属、成交、费用和资金预留继续保留。已经可能输出的请求不重发，Unknown 不因升级释放预留。当前未输出请求采用本地终止政策。

正常回执只有在账户动作和执行端状态均持久化后才发布。拒绝结果使用 `.rejected` 文件，不作为 Applied 回执。重启把历史操作恢复为 Halted，不重放旧 Release；连续发布和回滚都使用新的实例及更高策略代次。关闭之后到达的旧组凭据无法重新放行。

旧操作失效或收到迟到的 Halt 时，只把该操作的历史状态记为 Halted。只有当前 gate 的完整 binding 相符，才关闭实际授权和策略转发；binding 的成员摘要包含风险域、执行器 epoch、新旧实例及代次。已有后继 gate 时，旧操作不能按相同逻辑策略 ID 关闭它。尚无 gate 的初次 Halt 仅在当前授权精确对应该操作的旧或新实例时关闭；无当前身份依据时，不停止其他实例。

账户恢复和离线 replay 会重新验证交接对象、检查点及实际历史回报前缀。
