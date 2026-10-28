# tickwire-instrument-index

## 职责与依赖

无内部或第三方依赖的 `u64`→连续索引内核。固定目录冷建、动态目录只追加；业务目录的版本和权限由调用方管理。本 crate 禁止 unsafe 和 SIMD intrinsics；索引核心禁止浮点，基准耗时统计可使用浮点。

## API

- `DenseId::index() -> usize`：字段私有，ID 只在产生它的索引实例内有效。
- `FixedIndex::{new, lookup, key, len, is_empty}`：输入顺序决定 dense，重复键拒绝。
- `AppendIndex::{with_capacity, lookup, key, len, is_empty, insert, reserve}`：只追加，重复幂等，`reserve` 参数表示总容量。
- `Error::{DuplicateKey, Capacity, Allocation}`、`MAX_KEYS`。
- 两种索引的 `memory_stats() -> MemoryStats` 返回键/槽预留字节，`total_bytes()` 求和；不含索引结构和分配器元数据。

固定索引冷建贪心 mask 和软件 gather，过大位表自动回退 flat 开放寻址；查询必须核对完整原键。追加索引预留存储后插入不分配，扩容先完整构造候选，失败不得改变既有映射。禁止把 DenseId 跨目录或跨实例复用。

## 规格与检查

规格：[instrument-index.md](../../specs/implementation/instrument-index.md)。

```sh
cargo test -p tickwire-instrument-index --lib --tests
cargo clippy -p tickwire-instrument-index --all-targets -- -D warnings
cargo fmt -p tickwire-instrument-index -- --check
cargo bench -p tickwire-instrument-index --bench lookup
```

基准只在主机空闲时显式执行，不能把 bench smoke 当性能证据。修改映射算法保留未知键、冲突、容量和稳定 ID 回归；新增哈希或 gather 优化先比较软件参考行为。
