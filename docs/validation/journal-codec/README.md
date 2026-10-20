# journal-codec 验证记录

验证对象是 `tickwire-journal-codec` 的 schema 1、严格 CBOR 解析、业务摘要和固定日志代次的逻辑链。接口见 [journal-codec-interface.md](../../implementation/journal-codec-interface.md)，实际行为与边界见 [实现规格](../../../specs/implementation/journal-codec.md)。

| 证据 | 结果 |
| --- | --- |
| [red.log](red.log)、[red-evidence.json](red-evidence.json) | 编译成功后 10 项行为测试失败；记录实施前的接口、源码、测试与 fixture SHA |
| [identity-red.log](identity-red.log) | 首轮实现上添加接管后业务摘要稳定性断言，编译成功后实际失败 |
| [green-initial.log](green-initial.log)、[golden-initial.json](golden-initial.json) | 身份修正前的 12 项测试与黄金数据，仅保存为历史证据 |
| [green.log](green.log) | 最终 1 项单元测试与 12 项集成测试通过，0 doctest |
| [clippy.log](clippy.log) | `cargo clippy -p tickwire-journal-codec --all-targets -- -D warnings` 通过 |
| [fmt.log](fmt.log) | `cargo fmt -p tickwire-journal-codec --check` 通过，成功时无输出 |
| [vectors.log](vectors.log) | 独立 Python 编码器重算，与固定 golden.json 相同 |
| [environment.json](environment.json) | 工具链、验证范围、源码与契约 SHA |

日志中的仓库绝对路径归一化为 `$REPO`，保留命令结果与断言。最终六条事件记录的黄金字节分别为 75、65、63、63、59、65 字节；最大记录为 201 字节。591 个截断输入被拒绝；99,840 个单字节变异中所有被接受者都重新编码为原输入。

`generate-vectors.py` 是独立的有限数据编码参考。其整数结果先与 [RFC 8949 附录 A](https://www.rfc-editor.org/rfc/rfc8949.html#appendix-A) 固定实例核对，再用 Python hashlib 计算固定 SHA-256 期望。使用 `--check` 只比较，不重写 fixture：

```sh
python3 docs/validation/journal-codec/generate-vectors.py --check
```

本目录只记录 native Rust 的内容级验证。未接线真实业务 Journal、持久依赖闭包或签名收据，也没有为该 codec 新建形式模型或运行性能测试。

主代理两次独立源码审阅：首轮指出原订单代次与写入者代次混用，已通过真实断言 Red 复现并修正；第二轮核对字段键、两层哈希、链更新顺序、CBOR 最短表示和 i128::MIN 路径后通过。这个发现来自源码审阅与 Rust 回归，没有归因为模型检查结果。

另一独立代理 `ipc_codec` 对照 RFC 8949 复核最终严格解析器与整数边界，包括 map/key 顺序、bignum 长度及幅度、MIN 的编码、受控切片、总长度和完整消耗，未发现阻断项；该审阅未重跑测试。
