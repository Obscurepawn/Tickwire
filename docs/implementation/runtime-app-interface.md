# 运行程序接口

`tickwire-app` 装配本地账户 WAL、输入 WAL、恢复对象、时间校准、公共深度、私有流和 REST 订单连接。所有网络连接都经过 TLS 证书及协议主机名校验，签名密钥只从部署指定的文件加载。

```sh
cargo run -p tickwire-app -- inspect --config /absolute/deployment.json
cargo run -p tickwire-app -- run --config /absolute/deployment.json
```

`inspect` 读取部署配置和规范账户基线，检查产品、账户、时钟域、writer 及标的绑定，不连接交易所。`run` 打开实际持久化目录并运行事件循环。配置中的 `stop_file` 出现时，程序停止发送并断开会话；`maximum_runtime_ns=0` 表示不设置运行时长上限。健康变化、提交结果和 REST 回报输出为单行 JSON，不输出密钥。

## 部署输入

完整字段由 `crates/tickwire-app/src/config.rs::Deployment` 定义，未知字段会被拒绝。

- `baseline` 是 `CanonicalBaseline::bytes()` 的完整二进制文件，包含资产、持仓、目录和风险配置。
- `account_stream` 与 `input_stream` 是两个独立 WAL 的 128 位标识；WAL 的 `domain` 使用基线中的 `risk_domain_id`。
- `recover=false` 创建新日志；`recover=true` 重放已有日志。恢复入口保留冻结状态，随后由对账及控制授权恢复交易资格。
- `product` 接受 `spot`、`usdm`（线性永续）与 `usdm_future`。标的从基线目录按 `instrument_id` 精确选取。
- `boot_id`、`clock_generation` 与 `policy.domain` 使用相同的时钟域。授权期限、命令期限及额度窗口都是该时钟域中的绝对纳秒值。
- `network` 明确指定 REST、时间、公有流、快照、私有流及管理连接地址，以及 DER 格式的 TLS 信任证书。连接地址可使用 `host:port`，SNI 和 HTTP authority 仍由产品及环境固定。
- `credential` 指定 API key、签名密钥文件、凭证代次及 Spot 认证方式。支持 HMAC 原始密钥和 RSA/Ed25519 PKCS#8 DER。HMAC 密钥按文件原始字节读取。
- `policy` 与 `quota` 使用各自的 `bootstrap::Config`。每种操作的额度成本、窗口、租约、策略权限和控制版本均由部署显式提供。

所有文件路径使用绝对路径。账户日志、输入日志和恢复对象使用不同目录。程序保存每一条原始输入及其解释结果，重启后先完成输入重放，再接收新私有流输入。

## 规范命令文件入口

`strategy.mode="observe"` 只消费行情及账户输入。`file_queue` 从指定目录按序号读取 `00000000000000000001.json` 等文件；生产者先写临时文件，再原子重命名为最终文件。`enabled` 明确控制是否提交。

每个描述文件包含完整规范命令的路径和摘要、内部订单 ID 列表，以及实际恢复依赖。恢复依赖覆盖 kind 2～11，每个对象提供 schema、owner、路径及 SHA-256；`inputs` 指向包含真实有序输入和完整哈希链的 `TWINP001` 文件。执行器核对内容摘要并持久保存完整依赖，随后才能写入 Admit。

读取命令保留其原有身份、权限版本、行情引用和期限。提交结果交给账户 owner 去重，文件序号不会代替账户意图 ID。缺失的下一份描述文件表示等待；格式错误、摘要不匹配或过期命令使该入口停止继续消费。

## 对账和本地控制

`reconciliation` 配置读取者身份、请求代次、重试间隔和证据期限。执行器先取得真实私有流 gap token，逐单查询原订单状态并读取成交分页，核对累计成交和手续费，再读取账户、普通挂单及条件挂单快照。对账激活保存账户与输入的已同步前缀；发生新的私有流断档后，前一轮结果不再用于恢复资格。

`control` 可选配置本地控制收件箱和独立控制 WAL。主循环有界读取连续编号的完整命令，预检并同步后才更新门禁、租约或额度；重启保留撤权和已经发放的额度占用。窗口续建和费用规则换代也通过该日志推进。完整格式见[本地控制接口](local-control-interface.md)。

`strategy.mode="multi"` 的 `members` 为最多 32 个 `{logical_strategy_id, strategy}` 成员。逻辑 ID 唯一，禁止嵌套 Multi；每个 Hosted 成员使用独立 topic、进程和 SPSC 通道。成员退出后，其授权撤销，其他成员继续运行。

## 离线恢复和导出

```sh
cargo run -p tickwire-app -- replay --config /absolute/deployment.json
cargo run -p tickwire-app -- export --config /absolute/deployment.json --out /absolute/new-export
```

两个命令都不加载交易凭证、不连接交易所。它们锁住原持久化目录，在有界临时副本上使用实际恢复算法；原文件保持不变。`replay` 输出冻结账户的资产、持仓、订单和未决事实。`export` 还保存规范基线、按账户 WAL 顺序排列的完整记录、摘要及恢复依赖；导出目标须为新目录，完成 manifest 最后写入。
