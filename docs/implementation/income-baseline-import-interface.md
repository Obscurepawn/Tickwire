# 收入基线导入

状态：查询清单、专用审批验签、输入 WAL 与账户 WAL 安装/重放及 app 收入扫描/审批文件调度已实现。响应、资产 `updateTime`、两次相同快照均不能单独签发收入覆盖资格。

## 审批者和完成点

专用 `IncomeBaselineAuthority` 固定一个 risk-admin 控制者 ID 和 Ed25519 公钥。该密钥仅批准收入基线导入，不接受普通策略 grant、owner lease、运维 Admin 或 fencing 的签名。控制服务先核对 risk-admin 权限并记录审批审计，再由对应专用签名服务签发；交易进程只有公钥，不提供自动签发函数。

一次导入明确断言：给定账户基线已经包含清单中的收入键及完整内容；指定截止之前未列出的新收入仍是未知记录。它是经批准的风险基线决定，签名不代表 Binance 提供跨 API 原子快照。

完成顺序：保存查询/快照原文 → 生成有界导入提案 → risk-admin 审批签名 → 在当前 writer/账户/输入前缀重新核对 → 同步输入 WAL 的审批原文 → 同步账户 WAL 的 `IncomeBaseline` → 发布账户状态。任一阶段失败都不开放增险。导入只建立已覆盖集合，重新激活仍需正常的当前账户、仓位、订单及私有流对账。

## 固定绑定

`IncomeCoverageManifest` 具有独立域和版本，确定编码后以 SHA-256 引用，包含：

- 完整 `CanonicalBaseline` 摘要（包含账户 scope）、account epoch、当前 ledger sequence、钱包 ID；
- account/input stream，提案生成时的完整输入前缀位置、当前私有 connection epoch 和 gap generation；
- 明确收入截止、每个已覆盖 `CanonicalAccountInput` 的完整字节，按收入类型和交易 ID 排序；
- 本次所有收入区间叶节点与账户/普通订单/条件订单快照的输入位置及原始记录摘要。

清单最多 128 条收入、128 个证据引用，整体最多 54 KiB。每个引用必须存在于当前输入 WAL、身份一致且原文重解码成功。收入区间必须完整、互不重叠；饱和页不是叶节点，同一毫秒仍饱和时不能提交完整提案。该规则核对实际查询范围，不把查询完整性升级为基线覆盖证明。覆盖决定由 risk-admin 签名承担。

清单使用固定头 `TWIMNF01`；`SignedIncomeApproval` 的签名正文使用独立固定头 `TWIMAP01`，绑定 controller ID、非零唯一 request ID、清单摘要、完整 baseline 摘要（包含账户 scope）、account/input stream、account epoch、当前 writer epoch、当前 ledger sequence、目标进程时钟域、签发和到期时间。整数采用小端编码；固定大小字段、长度前缀、严格递增引用和完整消费检查共同拒绝非规范、超限及尾随输入。

当前身份、前缀或 writer 改变即拒绝旧审批；输入前缀推进后不能静默套用旧提案。相同 request ID、相同审批只返回已有结果；相同 ID 改内容是冲突。已成功使用的请求 ID 不淘汰，也不因进程重启恢复可用。

## API

```rust
struct IncomeBaselineAuthority; // 固定专用 controller ID 和验证公钥
struct IncomeImportProposal;    // 私有字节、摘要、收入集合及输入引用
struct VerifiedIncomeImport;    // 私有构造，绑定原审批及接受时刻

impl AccountIngress {
    fn propose_income_import(&self, request: ImportRequest)
        -> Result<IncomeImportProposal>;
}
impl IncomeBaselineAuthority {
    fn verify(&self, proposal: &IncomeImportProposal,
              approval: &SignedIncomeApproval, now: MonoTime, writer_epoch: u64)
        -> Result<VerifiedIncomeImport>;
}
impl RestOwner {
    fn import_income_baseline(&mut self, proposal: IncomeImportProposal,
                              approval: SignedIncomeApproval, now: MonoTime)
        -> Result<Applied>;
}
```

`ImportRequest` 仅选择已经持久化的证据；不包含通过标志。`RestOwner` 先查持久 request ID，再用配置中的专用公钥构造 `VerifiedIncomeImport`，在私有安装函数中重核输入前缀与当前账本。一个自行构造的公钥对象不能替换输入配置绑定的审批者。

恢复验证使用输入 WAL 保存的原提案、签名、首次接受时刻及当时 writer，重新核对签名和完整引用。历史审批只需证明首次接受时未到期，不能因恢复发生在到期后否定已经持久的合法账本。新的现场安装仍按当前时钟检查有效期。恢复配置须保留同一专用公钥身份；更换密钥需要显式版本化信任配置，不允许以新普通 grant 替换。

账户 `IncomeBaseline.coverage_digest` 取清单字节摘要，避开审批引用自身的循环。账户记录中的 `through_sequence` 覆盖审批输入记录；完整输入前缀和清单作为恢复/复制依赖保存。仅有账户 WAL 或清单摘要、没有原查询和签名输入时，恢复拒绝。

离线报告逐条枚举已同步审批的完整 `IncomeBaseline` 绑定，并与账户 WAL 配对。审批已同步、账户记录尚未同步时，`input_facts_not_in_account` 增加 1；只有完整绑定相同的账户记录才消除此项。读取审批绑定不安装基线，也不产生新的审批权限。

## app 配置与审批文件

Deployment 的 `income_baseline_authority` 配置专用控制者和公钥；输入 WAL 的配置正文固定该身份。`reconciliation.income_baseline_import` 可省略。启用时填写如下结构，数组与时间取部署实际值：

```json
{
  "wallet_id": 1,
  "start_time_ms": 1792440000000,
  "through_time_ms": 1792443600000,
  "limit": 16,
  "maximum_queries": 64,
  "request_id": [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
  "approval_valid_for_ns": 600000000000,
  "file_poll_interval_ns": 1000000000,
  "proposal_file": "/deployment/control/income-proposal.json",
  "approval_file": "/deployment/control/income-approval.json"
}
```

两个文件的父目录须事先存在；控制器固定其绝对位置。真实收入扫描和三个快照完成后，控制器原子发布 `tickwire.income-baseline-proposal.v1` 文件，其中含 `manifest_hex`、摘要和 `approval_payload_hex`。外部 risk-admin 服务核对清单与权限、保存审批审计后，对**该原始 payload 字节**签名，再原子发布：

```json
{
  "format": "tickwire.income-baseline-approval.v1",
  "payload_hex": "审批提案给出的完整小写十六进制正文",
  "signature_hex": "64字节Ed25519签名的小写十六进制"
}
```

示意字符串须替换为实际十六进制。文件最多 4096 字节，拒绝多余字段；旧 prefix 的审批保持不适用，不会被改写成当前批准。轮询按 `file_poll_interval_ns` 限制，等待审批期间不重复发收入/快照请求。到期提案重新发布，审批完成后重新获取绑定新 ledger 的三个快照，再执行正常激活。

初始导入要求当前账本仍对应固定基线且未跨越账户交易输入。已有未知收入或账户变化时不会自动改写为新基线；此时保留冻结和审计记录，等待后续显式基线重建流程。
