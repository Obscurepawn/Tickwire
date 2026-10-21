# 不可变资源实现规格

`ImmutableMapping` 与 `ImmutableResource` 已按先行接口加入 `tickwire-shm-sys`。原有 `WriterMapping`/`ReaderMapping` 的 `0x17` 协议保持独立；新对象使用精确 `0x0f`，没有原子访问或写接口。

## 创建与接收

创建依次执行平台/长度检查、memfd_create、ftruncate、完整 pwrite、F_ADD_SEALS、F_GET_SEALS、只读 FD 转换和 RO mmap。pwrite 处理短写与 EINTR，零写返回错误；任何失败由 OwnedFd 关闭私有文件。该构造过程不建立 RW mmap，也不在封印前导出 FD。

接收先检查普通文件和期望长度，再检查实际 seals 为 `0x0f`。此集合含 F_SEAL_SEAL，锁定封印集合、文件大小和内容；复用现有 mmap 方法时再次检查文件长度。输入 O_RDONLY FD 以 CLOEXEC 复制；输入 O_RDWR FD 通过仍由本对象持有的 `/proc/self/fd/<fd>` 重新只读打开。原 FD 活到 open 完成才析构，返回对象只保留 O_RDONLY FD。

RO 共享映射沿用 MADV_DONTFORK，Drop 先 munmap 再关闭 FD。`bytes()` 在调用时构造借用当前映射的只读切片，结构内部只存指针，不保存长寿命 `&[u8]`。F_SEAL_WRITE/GROW/SHRINK 提供这段借用期间的不可改写、不可扩缩容保证，因此读方法是安全 API；原有可变 ring 映射的读方法仍保留 unsafe 协议要求。

## 资源包装

独立 `resource.rs` 消费不可变映射并验证以下内容：

- 完整 ResourceHeader 解码与零保留区、全字段精确匹配已认证 expected、头部 SHA-256（仅将 descriptor_hash 字段置零）。
- 固定 ABI/schema/目标架构/页配置、已知 feature、非零资源/生产者/host boot/namespace 身份，生产者 generation 合法，其他身份计数均小于 `2^63`。
- kind 5–9、slot_offset=4096，producer/cursor/arena 区域及 class descriptors 均无效并置零，实际文件长度与资源头一致。
- 对象不超过 64 MiB；`4096 + content_bytes` 的加法、页补齐和范围合法；外层页尾全零；整文件 SHA-256 与已认证对象哈希一致。
- kind 9 的 consumer 非零、配置 feature 已声明、record_bytes=1、capacity=content_bytes，投影不超过 4 MiB。
- 其他快照的 256 字节 SnapshotHeader 位于资源头之后，entry_offset=4352，object_bytes 精确匹配，resource kind/snapshot kind/条目宽度数量符合固定布局，条目区不越过指定有效内容。

`content_bytes` 来自调用方对有效内容范围的确定，通用包装只校验这段范围及外层页尾。内部 section/成员、各成员零尾、快照覆盖、共同截面、有效期和 JOIN 身份绑定由上层对完整 `bytes()` 调用相应快照 validator 校验；`content()` 只是去掉外层资源头和页尾后的视图。

expected header 与 object hash 必须来自已认证 registry、资源描述符和 JOIN 等外部预期。从收到的文件自己计算两者再传入，只能证明自洽，不能认证资源。

## 验证与边界

完整 TDD 与实测归档见验证记录。Linux aarch64 上的新用例验证了正确内容、真正只读的 CLOEXEC FD、所有 seal 缺项/错误配置拒绝、写/扩缩容/RW mmap/mprotect 拒绝、真实 EBUSY→撤销映射→成功封印，以及错误返回后的 FD 和映射回收。

实现前的功能 Red 是 9 个主场景失败；首轮 Green 后补查发现 `channel_epoch=2^63` 未被通用资源包装拒绝，再以真实 Red 固定边界并修复。头部哈希负测让 expected 与损坏头的字段保持相同，独立验证 hash 计算，而非靠字段不相等提前拒绝。

本轮实际资格限 Linux aarch64、4096 页；macOS 验证生产入口明确拒绝。Linux x86_64、跨 UID/GID 的实际 FD 交付、owner 配额与 holder 登记、真实 JOIN 协调、退出证据和生产 NUMA/锁页/延迟验收仍由各层完成。memfd 不承诺持久化。
