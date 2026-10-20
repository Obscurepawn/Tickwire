# Gateway 模型状态

| 层级 | 源码范围 | 结果 |
|---|---|---|
| L0_journaled_sender | `JournaledSender` 与 WAL/transport 调用接缝 | 四配置 PASS；41 个 Next 动作全覆盖 |
| L1_response_binding | 连接代次与业务结果关联 | 计划，待源码 |
| L2_recovery_activation | 对账与风险重建后的激活 | 计划，待源码 |
