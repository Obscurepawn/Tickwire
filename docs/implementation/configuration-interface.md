# 统一配置发布接口

实现遵循[配置契约](../design/configuration.md)、[控制 API](../../specs/contracts/control-api.openapi.json)及[配置 schema](../../specs/contracts/system-config.schema.json)。`tickwire-config` 已实现严格候选解析、规范摘要、固定结构校验、静态拓扑/资源校验、权限投影及变更分类，累计 41 项针对性测试与 Clippy 通过。计划、控制服务和物化接线继续按下述接口实现。现有本地控制命令仍只负责其已实现的授权和配额操作。

## 模块与完成边界

| 模块 | 输入与输出 | 负责的完成点 |
|---|---|---|
| `tickwire-config` | 有界 TOML/JSON → 规范候选、语义校验、权限投影、计划 | 严格解析与固定内容身份；没有 I/O 或交易授权 |
| `tickwire-control-raft` | 已认证控制命令 → 三 voter 持久日志、应用记录与安全读 | 本任期 barrier 已提交应用；完整控制日志及状态持久化 |
| `tickwire-config-service` | HTTPS 请求 → 授权、计划、commit、操作查询 | 控制状态已提交；只有固定集合的全部真实收据满足后才写成功记录 |
| `tickwire-config-agent` | 已验证提交、目标投影、运行时收据 → 本地文件和物化收据 | desired/effective 各自完成文件与目录同步，收据已写 journal |
| 各运行时配置适配器 | 精确目标、版本和投影 → 事件边界应用 | 先同步本地配置应用记录，再替换内存视图、返回实际水位 |

普通订单只检查本地已应用版本。控制组、物化服务与运行时分别产生各自的私有证明类型，不能由调用者提供一个非零摘要来构造成功证明。

## 候选和规范字节

`Candidate::parse(format, bytes, limits)` 先检查请求尺寸，拒绝重复键、浮点数、非安全整数、TOML 日期时间及过深容器。JSON 整数范围为 `[-(2^53-1), 2^53-1]`；可能超过该范围的 revision/日志位置继续使用契约规定的十进制字符串。错误只返回错误类别与受控字段路径，不附带原始值。

两种格式都转换到同一 `serde_json::Value` 树，执行随程序固定的 JSON Schema 2020-12 与语义检查。外部 schema 和发布对象只通过内容寻址的受信对象解析器获得；校验器不能读取候选指定的任意 URL 或本机路径。schema 默认值在计算摘要前显式补齐，数组顺序保持原样。

规范化遵循 [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785.html)：对象键按 UTF-16 代码单元排序，字符串使用 JSON 的最小转义，无无意义空白。整数语法的 `-0` 规范化为 `0`，字符串 `"-0"` 保留原值。完整候选、权限 payload 与实际交付字节分别计算摘要，并在各自对应的绑定字段中校验。

```rust,ignore
Candidate::parse(format, bytes, limits) -> Result<Candidate>;
Candidate::validate_structural(&Schema) -> Validation;
Candidate::validate_deployment(&mut Resolver, &ObservedDeployment) -> Validation;
Candidate::canonical_bytes() -> &[u8];
Candidate::hash() -> Hash;

Projector::project(&ValidatedConfig, &Target, &PinnedPolicy) -> Result<Projection>;
Planner::plan(&Head, &Candidate, &Principal, &ObservedTargets) -> Result<Plan>;
```

结构校验覆盖数据类型和固定字段；部署校验另外要求实际发布对象、参数 schema、账户模式、权限、元数据和容量事实。`Validation` 分别返回 completed/pending checks。结构校验成功不能构造部署已验证的私有类型。

## 授权、计划与控制状态

已认证主体包含证书身份及当前角色/账户域。查询幂等结果前重新授权。完整候选要求完整读取与修改作用域；scoped-patch 先验证 selector/path 权限，再读取并合并。最终 diff 重新计算 mutation/dependency scope，避免共享政策间接改动其他账户。所有进程的 NoOpRevisionAdvance 仍进入固定 delivery 集合。

计划保存完整候选、base ETag、请求摘要、到期时间、固定目标身份与 observed runtime 版本、变更分类、必需角色、依赖和动作。分类由服务端计算，未知字段/分类拒绝；Hot、Quiesce、RollingRestart 和 ForbiddenRuntime 按契约处理。风险放宽需要 operator 与 risk-admin；拓扑/信任边界变化另需 cluster-admin。

控制状态 reducer 的输入为已提交命令。幂等键先于当前 ETag 比较：相同主体、端点、键、请求和 base 返回原结果，异内容冲突；首次提交才比较当前 ETag。一次 ConfigCommitted 原子推进 desired revision 并创建 operation，不改变运行时成功状态。Stalled 的恢复沿同一 revision；回滚使用新 revision；supersedes 必须覆盖全部残留版本差异。

Raft 存储按 Ready 顺序持久化 entries、HardState 和 snapshot。肯定投票/追加响应、外部可见 commit 和状态应用都受实际持久化顺序约束。安全读必须经过 ReadIndex 与 applied 水位；新 leader 完成本任期 barrier 后才能出具控制证明。三个 voter 均保存完整控制状态，订单 WAL 不进入控制 Raft。

## 物化与运行时接线

目标绑定包含稳定 process/host ID、instance、boot、exec generation、slot、locator generation、旧运行版本、新全局版本及投影政策。物化服务验证提交对象和固定目标后同步 ConfigPrepared，再用同目录独占临时文件完成 desired 的 write/fsync/rename/directory-fsync，最后同步 DesiredMaterialized。

运行时只有在 desired 收据验证通过后才能应用。适配器先按事件边界持久记录 ConfigApply，再替换不可变配置并返回绑定当前实例的收据。exec 的交易政策变更关闭增险，保留旧意图版本和账户事实，并在最终发送时使用新约束。物化服务核验运行时收据，再以相同同步顺序保存 effective 并写 EffectiveMaterialized。任何后续失败保留已经完成的内存事实，进入 Stalled。

文件操作使用目录 FD 和无符号链接的相对打开，检查属主、权限和 link count。启动从固定 locator 及 journal 证明恢复，不按 mtime 选文件。至少保留两份 last-good；last-good 不授予回退配置或交易权限。RollingRestart 与根目录迁移分别实现契约的 successor/cutover 和 locator 状态机，不把它们降成普通文件覆盖。

收据汇总器对完整固定 target set 检查 desired、runtime、effective 的全局版本、各目标投影、政策、实例、slot、locator 和实际持久来源。只有全部匹配才能生成待提交的 OperationSucceeded；控制组再次提交应用后 API 才报告成功。

当前的静态校验已覆盖唯一 ID 与引用、策略角色和预算币种、CPU/NUMA 与内存、主备及 voter 故障域、ring/arena 几何、共享内存总量和各自独立的期限。固定参考配置算得 bank/cursor 3,496,542,208 字节、快照 2,147,483,648 字节，加上投影和 relay 共 6,449,856,512 字节，与独立部署契约一致。实际已占用的退休资源和设备清单在部署与应用预检时另行输入。规格与失败回归见[候选配置规格](../../specs/implementation/config-candidate.md)。

接下来实现控制状态 reducer 与持久 Raft，再接 HTTPS、物化文件和运行时。每一层先写可失败的针对性测试，再实现该层生产代码。

权限投影政策使用有界、严格 JSON 对象，固定 `schema=1`、`revision` 和逐进程 `targets`。每个 target 显式列出 `process_id/account_ids/venue_ids/topic_ids/secret_purposes`，并与候选中的实际进程分配交叉校验。`PinnedPolicy::decode` 校验完整文件 SHA-256 与引用后缀；`project` 再校验该引用、revision 与候选内固定政策一致。策略、exec、guardian 的账户集合必须等于候选中实际分配，MD 的 topic 必须由该进程发布；其他角色也只有政策显式列出的视图。角色允许的凭据用途有代码内固定上限：策略的凭据集合为空，guardian 与 execution 各自使用独立引用。生成 payload 后再次执行独立 `ProjectionPayload` schema，按相同规范化算法计算 projection hash。政策摘要验证只证明配置引用一致，控制提交和部署授权仍由后续服务提供。

`ChangeSet::between(old,new)` 从通过静态校验的完整候选计算逐字段差异，按账户、策略、共享风险政策、venue 和 topic 追踪实际影响范围。精确数值限额收紧可为 Hot；放宽为 Quiesce，并增加 risk-admin 要求。保护阈值的方向单独定义，例如费用缓冲降低、行情年龄上限增大均属于放宽。参数在取得固定参数 schema 的 Hot 证明前按 Quiesce 处理。各进程即使业务投影未变化也保留在 delivery 集合中。此阶段只产生差异、权限要求和候选动作；后续计划器还需绑定真实实例、当前 effective、物化对象和部署验证结果，才可保存可提交计划。

首批测试固定 TOML/JSON 同内容同摘要、UTF-16 键排序、重复键/浮点/越界/未知字段拒绝、生产模板拒绝和参考配置通过。后续测试依次覆盖权限与共享依赖、ETag 并发、幂等丢响应、固定集合漏收据、每个持久切点、计划外 boot、根目录迁移和部分应用恢复。真实文件与子进程验证不以内存 Mock 的成功代替。全部系统代码完成后，再逐模块及全局执行 TLA+。
