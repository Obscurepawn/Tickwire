# 共享额度内核验证记录

接口先于实现写入 `docs/implementation/quota-interface.md`。最初使用可编译的返回 Unsupported 版本运行四项行为测试，`red.log` 记录四项均实际失败；初始源码保存在 `red-lib.rs.txt`。`green.log` 记录实现后同四项通过。

`current-green.log` 记录当前九项原生测试，覆盖多桶扣减失败原子性、精确重试与到期、恢复保留额、停止回收、重启占用、提交后丢回执、旧协调器比较交换失败、域与回退、冷却和冲突、窗口轮换及租约续期。`fmt.log`、`clippy.log` 记录对应源码检查。没有运行 Linux 或全仓测试。

测试使用共享的 `MockQuotaStore`：成功写入对其他句柄立即可见，提交后失败仍保存候选状态。它证明内核按 Store 契约处理故障，不表示数据库或文件系统已实现该完成点。

接线审查发现裸整数尝试 ID 无法区分各自从一开始编号的时间与私有请求 owner。隔离副本保留原标量 API，`attempt-namespace-red.log` 记录两个独立 owner 的第二次调用实际得到 Conflict。当前 API 使用 `AttemptId { producer, sequence }`，新增对应回归；原副本与测试以 `.txt` 保存，没有把编译失败当作 Red。
