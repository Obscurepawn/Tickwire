# 在线本地控制

`tickwire-app::control::ControlInbox` 接收本机可信控制目录中的 JSON 命令。主循环调用 `poll(&mut Endpoint)`，每轮最多处理配置的条数。控制命令先完成身份与状态预检，再写入物理 WAL 并同步，最后更新门禁或安装额度。写入结果不确定、身份冲突或重放失败时，收件箱停止处理，账户发送门禁关闭，额度端点冻结。

## 启动与文件格式

部署配置的 `control` 是可选项：

```json
{
  "inbox_directory": "/var/lib/tickwire/control-inbox",
  "journal_directory": "/var/lib/tickwire/control-wal",
  "stream_epoch": [21,21,21,21,21,21,21,21,21,21,21,21,21,21,21,21],
  "recover": false,
  "segment_max_bytes": 268435456,
  "maximum_commands": 4096,
  "maximum_per_poll": 8,
  "startup_consumer_lease_until_ns": null
}
```

操作方先创建可信目录；Unix 下收件目录不得允许组或其他用户写入。所有路径为绝对路径。将完整命令写入临时文件，再原子重命名为 `00000000000000000001.json`。后续文件序号连续递增，收件箱不会跳过缺号或删除原文件。

完整命令包含 `schema: 1`、`sequence`、非零且唯一的 16 字节 `command_id`、`binding` 与 `action`。`Binding::from_config(&policy)` 生成绑定：账户范围、产品、执行者、owner generation、writer epoch 和时钟域。它们必须同时匹配启动配置与当前控制平面。相同 ID、序号与规范化内容的重试只返回已有结果；ID 内容冲突会关闭门禁。JSON 拒绝未知字段。

`open(config, &policy_bootstrap, &quota_bootstrap, plane, clock)` 返回收件箱和 `Endpoint`。`plane` 必须是尚未接收运行回调的启动控制平面。启动时锁定 WAL，校验不可变 bootstrap 摘要；更改旧 bootstrap 后直接恢复会被拒绝。该目录和 WAL 共同承担单机控制权威职责，不能将独立的新额度存储冒充同一 namespace 的恢复，也不提供跨机器控制共识。

## 控制动作

以下是命令中 `action` 的示例。数值仅表示格式，绝对期限由操作方按部署声明的单调时钟域填写。

```json
{"kind":"mode","revision":2,"mode":"manage_only"}
```

```json
{"kind":"revoke_strategy","strategy":7,"authorization_epoch":4}
```

```json
{"kind":"owner_lease","lease":{"lease_epoch":3,"valid_until_ns":900000000000,"isolation_record":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]}}
```

其他门禁动作包括 `versions`、`strategy_grant`、`management_grant`、`revoke_owner`、`revoke_management`，字段直接采用 `runtime-policy` 的类型。owner lease 保留显式隔离记录引用；控制文件本身不证明该隔离措施已经执行。行情序列由实际发布者更新；私有流对账仍要求 `ActivationProof`。文件命令没有伪造 FeedUpdate 或对账状态的入口。

额度动作示例：

```json
{"kind":"quota_renew","until_ns":1900000000000}
```

```json
{"kind":"quota_windows","windows":[{"name":"api_next","namespace":[8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8],"scope":"account","subject":[9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9],"pool":1,"window":2,"revision":1,"starts_ns":1000000000000,"ends_ns":2000000000000,"limit":100,"recovery_reserve":10}]}
```

```json
{"kind":"quota_install","grant":{"request_id":2,"class":"normal","expires_ns":1800000000000,"costs":[{"window":"api_next","units":70}]}}
```

```json
{"kind":"quota_policy","generation":2,"starts_ns":1000000000000,"ends_ns":2000000000000,"rules":[{"source":"spot_test","action":{"family":"time","kind":"query"},"class":"normal","api_weight":1,"websocket_messages":0,"costs":[{"window":"api_next","units":1}]}]}
```

`quota_policy` 提供完整替换表；示例只允许该时间查询，其他操作没有规则便被拒绝。规则代次必须递增。旧请求身份记录继续保留，同一请求按不同费用表重试会发生冲突，操作方应在没有待发送请求时切换规则。新窗口不得覆盖已有窗口，同一 bucket 的窗口编号递增、时间区间不能重叠；旧发放占用不会因切换而清零。

`quota_cooldown` 使用 `until_ns`，只能延长当前冷却。`quota_freeze` 无附加字段，持久关闭额度端点。`quota_renew` 必须在旧消费者租约到期前调用。

## 恢复与界限

恢复会按原记录时刻重建完整控制历史与全部额度发放占用，不假定历史额度未被消费。每次重启都先同步一个新的消费者 session，再返回空的额度端点；旧 capability 不会重新安装。只有新序号的 `quota_install` 才能给新 session 分配剩余额度。`startup_consumer_lease_until_ns` 可以显式延长新 session 的租约，但不产生请求额度。旧撤权和永久额度冻结继续生效。

当前实现固定 writer 与时钟域，跨域接管需要单独的控制历史迁移。每个命令最多 32 KiB，每轮最多 32 条，总命令最多 8192 条，WAL 最多 16384 条记录、64 MiB 有效负载；额度内核另有限制为 256 个窗口、4096 次发放与 4096 个请求身份。达到限制会拒绝新操作，不淘汰记录来绕过去重。历史压缩与跨 writer 迁移尚未提供。
