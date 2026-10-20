# 接入校准时钟前的源码

这份源码对应 `../review-final-green.log` 的 27 项原生测试。它已经实现网络会话及独立审阅修复，签名新鲜度仍只使用单点时间与本地期限。后续迁移使用 `tickwire-clock::SignedTime/MonoTime` 和必须实现的发送前区间检查；本目录的通过结果不覆盖该迁移。

`source-hashes.json` 记录每个源码及测试文件的 SHA-256，`source.tar.gz` 保存相同版本。
