# 固定配置 locator 内容

`tickwire-config::locator::Locator` 按固定 `ConfigLocator` schema 构造或解析 JSON/TOML，限制输入为 16 KiB、解析深度为 8。generation 使用十进制字符串，范围为 1 至 `2^63-1`；UUID 为固定小写形式。active root 长度至多 4,096 字节，路径表示拒绝空分量、`.`、`..`、重复斜杠和尾部斜杠。

checksum 绑定去掉自身字段后的 RFC 8785 规范字节；`Locator::hash` 绑定包含 checksum 的完整规范对象。两种格式的相同内容具有相同完整摘要。首次 generation 的前驱摘要为零，后续代次要求非零前驱和 migration ID。清单摘要始终非零。

`validate_successor` 检查 cluster 相同、generation 恰好加一、前驱等于旧完整摘要、root 改变且 migration ID 不复用紧邻前次操作。内容校验不激活目录；固定 bootstrap 入口还要核对 migration journal、实际完整文件清单和旧写者状态。

四项能力 Red 与最终结果覆盖两种格式往返、独立摘要计算、迁移链冲突、字段/路径拒绝和正文损坏。配置模块全部 94 项测试和 all-targets Clippy 通过；IPC agent 对解析、摘要及相邻链规则作了独立只读审查。
