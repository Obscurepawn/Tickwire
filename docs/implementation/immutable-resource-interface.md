# 不可变 memfd 资源接口

`tickwire-shm-sys` 为快照、目录和配置投影提供不可变资源。`ImmutableMapping` 证明文件长度与内容受到 Linux 封印保护，因而可以安全返回普通字节引用；`ImmutableResource` 再验证资源头与受信预期信息。获得这两个对象都不会改变订阅状态，也不会授予 JOIN、交易或回收权限。

```rust
pub const IMMUTABLE_SEALS: i32 = 0x0f;

impl ImmutableMapping {
    pub fn create(name: &str, bytes: &[u8]) -> Result<Self>;
    pub fn map(fd: OwnedFd, expected_len: usize) -> Result<Self>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn bytes(&self) -> &[u8];
    pub fn read_bytes(&self, offset: usize, len: usize) -> Result<&[u8]>;
    pub fn duplicate_fd(&self) -> Result<OwnedFd>;
    pub fn validate_resource(
        self,
        expected: &tickwire_ipc::ResourceHeader,
        content_bytes: usize,
        object_hash: [u8; 32],
    ) -> Result<ImmutableResource>;
}

impl ImmutableResource {
    pub fn header(&self) -> &tickwire_ipc::ResourceHeader;
    pub fn bytes(&self) -> &[u8];
    pub fn content(&self) -> &[u8];
    pub fn duplicate_fd(&self) -> Result<OwnedFd>;
}
```

`create` 接受包含全部文件字节的非空、4096 对齐切片。它创建私有 memfd，设置精确长度，以 `pwrite` 完成内容；其间没有 RW mmap，也不导出 FD。随后添加 `F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL` 并核对精确值 `0x0f`，重新打开为 `O_RDONLY | O_CLOEXEC`，关闭原始 O_RDWR FD，最后建立 RO 共享映射。Linux 在存在共享 RW 映射时会拒绝添加 F_SEAL_WRITE，故该顺序不能换成保留写映射的 `0x17` 单写画像。[Linux 文件封印](https://man7.org/linux/man-pages/man2/F_ADD_SEALS.2const.html)

`map` 在成功和失败时都消费传入的 `OwnedFd`。它验证普通文件、精确长度与完整 immutable seals，得到真正的只读 FD 后才映射。seal 检查以实际 `F_GET_SEALS` 为准，不信任描述符中的声称。导出的副本也始终是 O_RDONLY、CLOEXEC；SCM_RIGHTS 接收方仍须重新检查这些属性和完整资源。memfd 的名字仅用于诊断，不用作资源身份。[memfd_create(2)](https://man7.org/linux/man-pages/man2/memfd_create.2.html)

`bytes` 与 `read_bytes` 是安全 API，因为 F_SEAL_WRITE 禁止内容再被改写，GROW/SHRINK 禁止范围变化。引用借用当前映射，不能在映射被销毁后继续使用；映射不提供原子操作或写方法。范围计算检查整数溢出，空范围只允许落在文件范围内。裸映射沿用现有非零、页对齐、`isize::MAX` 上限；资源包装另限制整个对象不超过 64 MiB，ConfigProjection 有效投影不超过 4 MiB。

`validate_resource` 消费映射，只接受资源 kind 5–9。`expected` 来自已认证 registry/资源描述符，`object_hash` 来自已认证 JOIN 或相应资源操作；调用方不能从待验证文件读出一份 header/hash 再将其冒充预期值。包装核验完整 4096 字节头的编码、头部摘要、精确字段身份、目标画像、无原子页/arena 的固定布局，以及整个文件的 SHA-256。

`content_bytes` 是 ResourceHeader 后的有效字节数，不含头部及外层零页尾。要求 `file_len == align_up(4096 + content_bytes, 4096)`，其后的字节全零；`content()` 只返回这段有效内容。kind 9 另外要求 `content_bytes == capacity`、`record_bytes == 1`。其他快照的通用包装检查头和条目区范围，但不能仅凭这个参数推导全部 section、成员和内部零尾；这些仍由相应业务 validator 校验。完整 TopicSnapshotManifest 的各成员已页对齐时，外层 `content_bytes` 通常等于 `file_len - 4096`。

接收 JOIN 基线的顺序是：认证控制会话和 offer → 接收 FD → `map` → `validate_resource` → `tickwire_ipc::validate_topic_bundle` 检查完整成员、共同截面、版本和 JOIN hash → 上层协议决定是否 Ready。不存在资源有效就自动 Ready 的捷径。

平台资格沿用现有 shm-sys：Linux、小端 x86_64/aarch64、64 位原子能力、4096 页；其他平台返回 `UnsupportedPlatform`。对象仅在内存中存在，封印不构成磁盘持久化。
