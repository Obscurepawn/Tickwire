# 账户有序日志接口

`tickwire-account-journal` 在真实文件 WAL 中保存 `tickwire-account-codec::CanonicalRecord`。一个日志固定一份完整账户基线和目录，八类 `RecoveryRecord` 共用连续顺序。它不持有运行中的账户、不发送网络报文，也不计算第二份资金预留。调用者在候选 `Account` 上计算准入，日志同步成功后才发布候选。

## 身份与文件

`Config` 包含物理日志配置、受信 `baseline_digest`、初始 writer 代次和账户 `Scope`。`Scope` 固定风险域、交易所、账户、环境；产品与资产单位保留在完整基线、目录、命令和事件中。物理风险域必须与账户风险域相同。账户代次标识账户基线，writer 代次标识写入者。

`FileBackend::open(path)` 复用 `FileStore` 的独占写锁。目录处于受信本地文件系统中，所有写者遵守该锁。创建先同步 `ACCOUNT-BASELINE.cbor` 及目录，再同步版本 1 的 `ACCOUNT-BOOTSTRAP.json` 及目录，最后创建物理日志。bootstrap 使用固定字段顺序的确定 JSON 编码，包含上述全部绑定。两个账户文件均以独占创建方式发布；既有文件不覆盖，包括前次创建失败留下的部分文件。

`create(backend, config, baseline)` 检查基线摘要和身份，拒绝已有账户文件或物理日志。`recover` 的配置由受信调用方独立提供。恢复逐字节比较期望 bootstrap，重新解码基线并核对摘要、账户身份和物理配置。文件摘要提供内容绑定；外部可信启动和防回滚由调用方提供。

## 追加与完成

```rust,ignore
AccountJournal::create(backend, config, baseline)
AccountJournal::append(record: RecoveryRecord) -> AppendReceipt
AccountJournal::recover(
    backend, expected_config, new_writer_epoch,
    required: Option<&PhysicalFrontier>, verifier
) -> Recovered
```

追加由日志填入固定基线摘要和下一个记录序号，每次写入一个完整物理批次。`AppendReceipt` 字段私有，返回记录摘要、序号、基线摘要和已经同步的物理水位。它只证明这些字节已持久化；风险准入、零输出证据和费用闭合由调用方核验。恢复为内部历史索引保存的水位是本次重新同步后的完整前缀，证明该前缀包含对应记录；它不是那条记录最初写入时的物理收据，也不用于产生新的发送许可。

Admit 按 `(intent_exec_epoch, intent_id)` 保存身份。相同完整恢复输入重试返回原收据，不再次追加；同键不同输入拒绝并封闭追加。重试保留原 `evaluated_at` 和 `AdmissionImage`，改用新时间属于冲突输入。其它七类输入保持到达顺序，包括相同成交 ID 的再次观察；附带的新订单观察仍可能改变账户。

记录序号从 1 连续增长，并与物理 `record_seq` 相同；它与 `Account::ledger_sequence` 独立。重复事实可以增加日志序号而不改变账户状态序号。首次 Admit 的原意图代次必须等于当前 writer 代次；迟到事实的原意图代次可以小于或等于 writer。恢复验证物理 writer 代次从初始代次起单调不减，接管 writer 大于或等于最后一条记录的 writer。

所有编码、身份和容量检查在追加前完成。追加或同步出现不确定失败后，日志进入 poisoned 状态，后续调用不再追加，不发布新的成功序号或收据。完整但未返回成功的尾批可能在重开时保留，未完成尾批由物理日志按既有规则处理。调用者在错误后冻结增险并保留待定预留，直到重开核验。

## 冻结恢复

`history_frontier()` 返回私有构造、只读的 `HistoryFrontier`，包含账户 Scope、物理 stream、基线摘要、记录数和顺序链摘要。空链为 `SHA256("tickwire.account-history-empty.v1" || bootstrap_bytes)`；每条成功记录更新为 `SHA256("tickwire.account-history-record.v1" || previous_hash || writer_epoch_le_u64 || sequence_le_u64 || CanonicalRecord.digest())`。重试不更新链，失败不发布新前缀。

前缀的确定二进制编码为 128 字节：`TWAF0001`（8）、risk_domain（16）、stream（16）、baseline hash（32）、venue（u32 LE）、account（u64 LE）、environment（u8，Production=1/Test=2）、三个零字节、记录数（u64 LE）、链摘要（32）。`digest()` 为 `SHA256(bytes())`，与恢复对象服务的内容地址相同；对象类型由 `TWAF0001` 区分。固定编码及空链共同绑定 bootstrap，包括初始 writer 和物理分段配置；它没有把逻辑前缀伪装成某个历史物理收据。

`RecoveryVerifier::verify_admission(baseline, record, preceding)` 是必填的历史闭包核验入口，每条 Admit 均调用一次。`preceding` 是根据已验证物理记录重建的该 Admit **之前**的精确前缀。实现者检查原始命令、准入记录、基线对象、这个前缀对象和其它历史依赖，必要时重新同步已有内容；缺失对象或核验失败直接拒绝恢复；只允许重新同步已有内容。接口不提供默认放行实现。

恢复先验证全部记录的编码、基线、顺序、账户身份和 writer 代次，再核验历史准入闭包，最后调用 `Account::recover`。它使用原 `evaluated_at` 重算准入并比较完整 `AdmissionImage`，按原顺序应用交易结果、中止、费用闭合、外部现金流、外部仓位和两类归属转移。拒绝的账户事实仍留在 WAL；该事实使账户恢复失败，不跳过输入拼出可用账户。

返回的 `Recovered` 包含冻结账户、日志和被丢弃的未完成尾字节数，没有网络队列。恢复日志在对账激活前拒绝新增 Admit，允许记录后续对账事实。`append_reactivation` 持久保存完整对账决定后解除日志的新 Admit 禁令；该记录绑定原始输入、历史摘要、账本序号和远端可用余额，恢复时由必需的核验器重新检查。普通事实追加不会解冻账户或签发活跃 Admission。

`required` 必须来自独立受信的 `PhysicalFrontier`。物理恢复要求该精确前缀存在，并在修剪尾部前核验。传入 `None` 只验证当前介质中的完整链，不检测整段已提交历史被回滚。日志封装不从文件末尾自行生成“受信前缀”。

单条记录沿用 60 KiB 上限；基线沿用 2 MiB 上限；记录总数限制为 `MAX_FACTS + MAX_ORDERS`，与当前账户恢复 API 一致。增加容量时同时扩展日志与账户恢复能力。

文件发布使用标准库的原子独占创建，已有文件或符号链接会失败；成功返回前显式同步文件和目录，不依赖析构完成同步。[OpenOptions::create_new](https://doc.rust-lang.org/std/fs/struct.OpenOptions.html#method.create_new)、[File::sync_all](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all) 定义了这里使用的文件操作语义。
