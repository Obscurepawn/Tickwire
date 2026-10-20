# 维护记录

本级源码冻结后锁定 `JournaledSender`、Binance REST 适配器、BusinessJournal 和 transport 摘要。REST 适配器已独立只读核对：可信时间失败在 WAL 前退出；签名 query 原样进入 HTTP 编码；Host、API key、命令摘要和 authority 由同一准备结果取得。该审阅及真实 TLS 测试是实现证据，L0 模型没有重新证明签名算法。

恢复后的历史 `accepted`、`wire`、`enqueues` 是模型见证，保留崩溃前发生的输出事实；实际恢复实例通过 WAL 建立 Unknown/Aborted，不能读取崩溃进程的 transport 报告。正是这种信息缺失使恢复不能凭“曾经排队但未观察发送”重新发送。

当前 API 没有业务结果入账或恢复后解冻入口。后续增加这些入口时，扩展本阶梯；如改变当前发送/中止/恢复语义，保留本版模型和结果，建立新版本。

market_core 进一步独立核对物理日志恢复会重同步已验证段、中止与已接受证据、重复请求及 API 串行范围，并提出零 IoBudget 的分支区分；该建议已落实为独立动作。
