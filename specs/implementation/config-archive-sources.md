# 配置归档的信任配置与来源资料

本规格约束 `tickwire-config-service::evidence::Trust` 和 `source_bundle::Bundle` 的可持久编码。它们为后续计划归档保留完整输入，不产生部署授权、运行时应用收据或当前登记头证明。

## 信任配置的规范读回

`Trust::canonical_bytes()` 返回 `Trust::new` 已用于计算摘要的规范 JSON 描述。描述和摘要域保持兼容：`tickwire.config.evidence-trust.v1\0` 后接规范字节，字段为 `schema=1`、`cluster_id`、`namespace_id`、十进制字符串 `revision` 和 `issuers`。issuer 按 ID 排序；每个 issuer 的 `kinds` 按既有 `Kind` 枚举顺序排列且唯一，公钥保留 32 字节数组形式。

`Trust::decode(bytes, expected_digest)` 只接受不超过 16 KiB、深度不超过 8 的完整规范 JSON；重复键、未知字段、非规范空白/数值/转义、未知 kind、无序或重复 issuer/kind 均拒绝。revision 必须是无前导零的十进制字符串，范围为 `1..2^63`。读回经 `Trust::new` 重建，复用 scope、issuer、公钥、数量与唯一性校验，再核对完整规范字节和期望摘要。解析成功仅表示信任配置值可重建；上层仍须证明该配置在归档准入时已获批准。

信任配置本身不含当前时间。历史审计可在已持久、已验证的准入时刻重算原证据，不能由此恢复当前资格。当前部署继续调用 `Trust::verify` / `VerifiedEvidence::revalidate`，使用当前批准的信任配置和当前时间；已过期、未来生效或已撤销的证据仍拒绝。

## 来源资料包的确定性重编码

`Bundle::encode(limits)` 返回完整 `TWDSRC01` 资料包：8 字节 magic、4 字节 little-endian 规范 header 长度、规范 JSON header、按完整 artifact reference 排序的原始对象字节。header 保留 schema、全部 artifact reference/长度、全部已有证据 envelope、retained root 和 credential roots。证据按 `(Kind, reference)` 排序，credential roots 按 reference 排序；对象正文不作规范化或改写。未被候选配置直接引用的资料也必须保留，避免归档丢失后续核验依赖。

编码复核调用方预算：对象个数、单对象字节、对象总字节、header 和整体资料包上限；长度加法溢出拒绝。header 在序列化时受上限约束，正文只在最终长度通过后分配。原 decode 的 magic、对象摘要、重复项、引用完整性和有界校验继续生效。

重编码不修改 `Bundle::digest()` 或 `stored_bytes()`：这两者始终描述该实例最初读入的原始包。若原始包的对象/数组顺序或 JSON 格式不同，规范输出可有不同摘要；重新 decode 的实例摘要描述新输出本身。归档 owner 必须分别保存需要追溯的原始来源身份和实际存储对象摘要，不能把两者互换。

`decode → encode → decode` 必须保留全部 artifact 原字节、证据规范字节、retained root 和 credential root 集合，且通过内容验证得到相同来源内容摘要。编码/解码不验签、不赋予 freshness；损坏的签名可以作为原始事实保留，但当前资格检查必须继续拒绝它。

资料包可大于单个持久对象上限；上层可拆成不超过 16 MiB 的块，在核对块集合、次序、长度及摘要后还原完整包并执行这里的 decode。本模块不把拆块成功或 JSON 读回当成部署成功。

## 验收

- 真实 Ed25519 envelope 在信任配置读回后仍通过；摘要与既有构造保持一致。
- 篡改 scope/key/revision/kind、错误期望摘要、重复/未知字段、无序数组、非规范 revision 和超预算拒绝。
- 不同原始排列编码为同一资料包；重读后逐项原字节相同，保留原始包摘要语义。
- 编码的各项预算有精确边界；畸形 framing、对象正文篡改和遗漏根继续拒绝。
- 在历史准入时刻验证成功的归档资料，在当前过期或撤销信任下仍不能作为可部署资格。
