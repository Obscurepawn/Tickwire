# Tickwire 验证对象

目标是验证单所有者交易 reducer 的资金预留、发送准入和崩溃恢复，再逐层加入行情、IPC、多进程接管。用户的完整目标仍覆盖 Binance 现货与 USDⓈ-M；当前模型结论按各级实际覆盖范围记录。

- 源码：`crates/tickwire-execution`、`crates/tickwire-mock`、`crates/tickwire-book`、`crates/tickwire-risk`、`crates/tickwire-ipc`、`crates/tickwire-shm`、`crates/tickwire-shm-sys`、`crates/tickwire-control`、`crates/tickwire-control-sys`、`crates/tickwire-journal`。设计依据：`docs/design/`、接口依据：`docs/implementation/`、实现契约：`specs/implementation/`。
- 排除：第三方 toolchain、`target/`、未实现的实盘 TLS/WS/FIX、真实 SQL/KV 和尚未实现的业务持久日志适配器。物理 WAL/FileStore 已有独立模型和真实文件测试；它的收据仅代表底层同步完成。mock 的持久化含义是对象跨模拟 exec 重启存活，不能据此证明 fsync 或机器掉电。
- Rust Cargo workspace；`Engine` 的变更使用 `&mut self`，`MockRuntime` 同步串行调用 `reserve → commit → accept_commit → begin_send → exchange.submit → result`。选定模型没有两个线程同时写 Engine；故障级允许进程在这些操作之间崩溃。
- 当前关键状态：订单槽固定容量；订单 request 固定 commit_profile；durable 代表满足该 profile 的回执已经接受；MayHaveSent 代表已授权给传输层但缺乏完整终结证据；恢复将全部 durable intent 置为 MayHaveSent 并冻结准入。
- 故障模型：日志操作失败、记录已保存但回执丢失、发送结果未知、exec 进程重启。日志和交易所 mock 跨重启存活；不注入数据篡改、磁盘丢失或伪造回执。
- 模型边界使用 const-generic 容量的小实例：L0 两/三 intent，L1 一/二 intent、最多一/两次重启；数值预算归一到小正整数以穷举准入竞争。边界覆盖不是对生产容量的性能证明。
- Toolchain：TLC 1.7.4；机器路径位于被忽略的 `config.env`。`run_tlc.sh --det --coverage` 固定单 worker 并清理 scratch。现有 Java 8 可执行此 TLC 版本的 smoke；正式运行结果另记录。

- 控制请求当前由同步 `&mut Session`/`&mut MockRequestStore` 串行处理。L0 验证登记，L1 加入单调期限；Linux FD/认证由真实进程测试覆盖。生产 launcher 需保持同一 time namespace 并保证 epoch/nonce 不复用。
- 物理 WAL L0 使用固定段、帧粒度的写入/同步/收据/崩溃模型；分段与 manifest 的真实实现已有 Rust 故障回归，形式模型留待 L1。没有外部可信前缀时，不声称检测成功同步后媒体无声删除整个尾批。
