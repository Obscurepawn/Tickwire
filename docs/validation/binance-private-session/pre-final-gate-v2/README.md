# 末次采样修复前的历史证据

source-model-evidence.tar.gz 保存修复前92个源码、模型、配置和证据文件；source-hashes.json 列出逐文件及归档哈希。v1 的 PASS 只涵盖当时单一 Gate 时间样本的抽象，未区分 private 保存的 second 与 ClockAdapter 预算回调后的 MonoTime，因而没有覆盖本次实际截止缺陷。原始源码和历史 PASS 不被新锚点替换。
