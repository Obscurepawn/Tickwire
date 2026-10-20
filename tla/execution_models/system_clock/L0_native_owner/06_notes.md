# L0 审阅说明

本级只增加形式模型和实现规格，没有修改生产源码。原生 provider 的错误状态与 TimeClient/core 的状态分别保存，是源码审阅中特别核对的部分：core.fault 不会自动设置 client.fatal，后续 provider 调用仍可发生。

`nativeReads` 为调用尝试见证；Unsupported 构造会增加一次尝试，但没有 syscall。成功的构造探测保存在 native.last，移入 TimeClient 前后的采样也继续受这个值约束。

Generic wrapper 的结果分类条件均由 ClientSample 已蕴含。它们让 TLC 单独报告正确域输入、错误域输入、永久 provider 错误和暂时错误；没有删掉原 helper 的后继。

记录的首次 native 实现 Red、provider fatal Red 与后续 Rust Green 来自根线程的真实测试，见实现规格；本模型没有把这些已有修复记成自身发现。
