# 配置计划内容准备

本模块把配置纯值计划器与唯一 Raft owner、当前 mTLS 主体和有界部署检查连接起来。结果 `PlanDraft` 固定一次实际提交基线与完整内容闭包；持久计划、当前运行实例采集、资源登记头核验和应用驱动接入前，结果没有提交或运行时授权。

## 基线与权限

`ReplicaControl::begin_plan` 接收已认证主体、完整候选或作用域补丁、If-Match 与计划选项，分配实际 ReadIndex。完整候选只能由当前全局 Editor 使用；补丁沿现有 selector 授权顺序检查。第一次屏障完成且业务状态扫描到屏障后，取该 owner 的 `CommittedHead`，再执行 `PreparedCandidate::prepare`。客户端不能提供基线配置、revision、Raft 提交位置或信任来源。

`PendingPlan` 绑定发起它的 Replica 实例。转交另一个 owner、取消后重用、完成后重用均拒绝。读取未完成时占有一张票；进入后台计算前释放该票。取消和错误结束释放现有票及后台结果容量，后台线程仍运行时保留它自己的容量份额。

## 有界后台计算

`deployment_owner::Owner::start_plan` 与既有内容验证共用 1–8 个任务容量。worker 固定同一个 source bundle、evidence trust 和 limits，计算候选 `ContentValidated`、旧新 `ProjectionDelivery` 及 `ComputedPlan`。所有旧新投影来自同一 owned artifact catalog。部署检查返回的候选摘要、计划候选摘要、交付候选摘要逐字相等，交付基线摘要等于实际提交基线。

计划计算只使用服务时钟。任务接受、worker 开始、worker 完成、结果消费与最终交付均检查时间不回退，且严格早于内容事实到期。替换 provider 即使新旧字节相同也使旧任务和旧结果失效。未消费的完成结果继续占用容量。

## 完成复核

worker 完成后重新分配 ReadIndex。第二次屏障完成前不返回草案。再次核对证书、当前角色、原始输入的授权与 If-Match，以及完整 `CommittedHead` 身份；任何改变均拒绝，调用者需要重新准备。草案关联原主体和证书摘要，内部保留完整目标集合，尚不生成可向账户作用域调用者直接公开的 HTTP 视图。

`PlanDraft` 暴露计算计划、内容事实、交付闭包、基线及固定待核验项：`retained_registry_head`、`runtime_targets`、`runtime_apply_driver`、`durable_plan_admission`。这些字段用于后续接线，不形成 `Ready`、revision 分配、operation 或 `Succeeded`。现有 HTTP plans/commits/preview 路由继续按未实现返回，直到满足其完整契约。

## 当前资格刷新限制

现有资格报告绑定完整业务基线。候选业务字段变化后，重新签发的资格引用也进入 diff；当前 `changes.rs` 将这类引用变化归为全局 Quiesce，并要求集群权限。因此，单字段风险收紧的纯值 Hot 分类尚不足以形成可发布 Hot 操作。完整发布接入前还需明确资格元数据刷新与实际运行时动作、作用域授权之间的分工；本阶段保留现有保守规则。

## 验证

使用真实三 voter 文件日志和真实 mTLS 主体验证：首次隔离无法取得基线；后台完成后再次隔离仍无法返回结果；连通后返回原基线与完整目标集合且不修改 desired；错误 ETag、无 Editor 权限、证书撤销、provider 替换、跨 owner 使用、取消与重复消费均拒绝。纯值层另验证逐目标依赖、变化分类、ceiling、交付闭包和摘要稳定性。运行实例签名观测的验证范围见相应事实规格；本阶段不生成新的 TLA/TLC 证据。
