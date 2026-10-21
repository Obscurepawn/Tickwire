# Spot 同 client ID 减量修改

接口：spot-amend-interface.md。当前完成账户、规范事件、账户编码和原策略权限门禁；REST 修改结果、历史查询、私有 `REPLACED` 与真实网络恢复组合继续实现。

- 原普通 Spot LIMIT/GTC、非冰山、无结构腿；内部 ID/native ID/client ID/原 intent epoch、当前 manager/revision 全部相符。新总量大于已记成交且严格小于有效总量，client ID 必须显式沿用。
- 新 Admit 属于非增险管理，不释放原订单资金，也不生成新订单。每个订单最多一个未决修改；待定修改的原 manager/revision 与已知成交量在首次发送前重验。
- 原 spec/数量不变，`OrderView.effective_quantity` 只有匹配真实修改结果才能降低。`AmendmentView` 保存原请求、旧/新量和来源独立的执行 ID；容量耗尽拒绝，不淘汰旧 ID。
- schema 3/tag 6 保存修改事件；旧 schema/tag 字节不变。REST 响应、REST 历史、私有流 ID 分开存储。重复组件仍处理新的 observation，不能错绑另一待定请求。
- 预留按 `max(有效总量, 修改后已观察累计量) - 已记成交量` 的非负部分计算。累计缺少明细时冻结且保留差额，不生成 Fill。真实迟到成交照实记账，超过有效总量则冻结；旧数量观察不能提高有效总量。
- 持久 Aborted 只结束本次修改，不中止原订单。真实结果与已中止修改矛盾时保留结果并冻结；已确认修改后的 NoBytes 不能覆盖结果。
- 账户状态与策略订单摘要对存在修改历史的状态使用 v4 域，并加入有界 amendment-state.v1。恢复合成的 unknown/frozen 不进入该摘要；原持久观察异常与数量责任仍参与。
- `ControlPlane` 只有收到真实 `Admission::increases_risk()==false` 的 `AmendKeepPriority` 才跳过行情/私有资格。策略本身的 amend grant、owner lease、writer/config/deadline 和交接限制仍有效；管理 cancel/query grant 不授予 amend 权限。

实际能力 Red 与后续 Green 保存于 验证目录。新增能力的初始 Unsupported/Stale 不列为已部署功能故障。
