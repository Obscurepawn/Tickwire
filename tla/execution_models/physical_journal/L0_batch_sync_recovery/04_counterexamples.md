# L0 反例记录

三组配置都完成且没有性质违例，无开放 candidate。初次解析错误只涉及 TLA token 空格，见运行日志，不是代码反例。

目录 0700 权限硬化来自 root 源码审查建议，经真实 Rust Red/Green 验证；没有把它归因为 TLC 发现。字节 codec 的独审由 ipc_codec 完成，writer/recovery 和模型独审由 root 完成。
