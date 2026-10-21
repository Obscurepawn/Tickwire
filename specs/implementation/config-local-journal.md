# 本地配置业务日志

`tickwire-config-journal` 在物理 WAL 上保存一个固定 cluster、namespace、host、process、slot 和 locator generation 的配置观察序列。首次创建单独同步 scope 头部，后续每条事件独立封批并同步。cluster、host 和 process 使用系统配置中的小写名称；namespace 和 slot 保存 UUID 字节。

每项操作依次记录 `Prepared → Desired → RuntimeObserved → Effective`。Prepared 保存完整旧、新版本与目标、提交引用和预期实例；后三步分别引用前一步的完整编码摘要。一个 scope 同时只有一个未完成操作。下一项 Prepared 的 base 精确等于最近 Effective 记录的完整版本，目标 revision 严格增加。同一步相同字节的重试返回原序号、原摘要和原物理同步前缀；不同内容拒绝，历史幂等映射占用固定预算。

记录使用 schema 1 的严格 JSON 封套，单条最多 16 KiB，解码后重新编码的字节与原文完全相同。操作数最多 4,096，累计正文最多 64 MiB；预算包含 scope 头部和全部历史正文。恢复另限制物理读取总量，覆盖正文、每条记录的帧开销及一个未完成批次。投影对象长度最多 4 MiB，文件长度最多 8 MiB。

物理 `Journal::recover_checked` 在完整物理验证后、任何截尾或同步之前调用应用校验。scope、编码、业务顺序、容量或启动 writer 不符时，原文件保持不变。`Recovery::observed_exec_epoch` 包含通过校验的未完成批次头；旧 writer 不能截去较新 writer 的尾部并继续写入。这个观察值不属于同步收据。原物理 `recover` 保持已有默认行为。

追加同步失败后，日志关闭后续追加，内存业务状态保持原先已确认进度。重开根据真实完整提交前缀重建：已写完整但应答丢失的事件可恢复，之后相同请求返回它的原收据。收据证明观察内容已经同步；控制提交、文件当前状态和运行时应用证明由物化 owner 分别验证。本模块未产生交易权限，也未接入完整配置 agent。

Linux 固定结果包含本模块 13 项、目录 FD 文件后端 22 项和物理 WAL 22 项测试，以及三库 all-targets Clippy。三个 ignored 项都是由父测试实际启动的子进程入口。测试覆盖每个业务步骤完成后的 SIGKILL、同步应答丢失、有效物理 seal 中的非法业务记录、旧 writer 的未完成尾部、实际目录 FD 后端和真实文件收据。SIGKILL 测试保留运行中的内核，未模拟机器断电。
