# Linux 构造与 SBE 发布修复

`md-stack-red.log` 和 `publication-red.log` 来自实际冻结快照 `.tools/retirement-sbe-check-20261020T200800Z/source`：MD 启动栈溢出；三个 SBE 测试中两条真实网络通过，mapped 发布测试在构造时失败。

MD 的 `PublicDepthClient` 原来内联持有 `DepthSync`，其中 `Book<5000,5000>` 含固定容量数组。SBE 构造包装增加了按值返回的临时对象，叠加 MD 的分支临时后耗尽栈。`constructor-native-red.log` 用子进程中的 2 MiB 线程栈真实复现；`DepthSync` 改为初始化时分配一次 `Box<Book>`，同一测试在 Spot JSON、Spot SBE、USD-M 生产容量下通过。Book 初始化仍有栈临时，因此测试实际执行构造，未仅凭结构尺寸判断。热路径不新增分配。根线程使用三文件派生快照完成了 Linux 两项构造测试，并完成真实交接场景。

mapped 测试原来先登记 reader，再创建 `TopicPublisher`，违反发布器的初始 membership 检查。修正后的顺序为创建发布器→认证 Session→`JoinOwner.prepare` 构造不可变基线并保留 pending gate→Subscriber attach→真实 WSS/typed depth/arena 发布。测试全程不激活或移除该 gate，生产 membership 检查没有修改。

修正 JOIN 后，Linux 测试继续暴露发布调用栈溢出。`publication-stack-red.log` 保留首个失败；`publication-stack-marked-red.log` 加临时阶段标记，确认 bank、发布器和 JOIN 均已构造完成，栈溢出发生在行情通知发布时。native 未优化汇编中，`apply_view`、`decode_sbe_diff<4096>` 和 `normalize_depth<4096>` 的栈帧分别约 648 KiB、1,027 KiB 和 386 KiB，叠加测试与网络调用超过默认 2 MiB。独立 decoder 的短栈探针通过，保存为 `decoder-native-probe.log`，未将这个通过的探针记作失败复现。

修复让 SBE bridge 从已完整验证的借用 `Frame` 直接向最终 `DepthUpdate` 数组做精确整数转换，移除同容量的中间归一化数组。容量、精度拒绝、交易对绑定及删除数量 0 的语义不变，未增加线程栈、unsafe 或消息处理分配。临时阶段标记已移除。

`publication-stack-green.log` 是独立派生快照 `.tools/sbe-stack-check/source` 的实际 Linux 结果：完整 SBE WSS→规范盘口→真实 pending gate→SHM arena 读取测试 1 项通过，另外 2 项网络测试因名称过滤未执行。该次命令、镜像、逐文件清单摘要与差异在 `publication-stack-results.json`；完整 Red/Green 清单位于 `.tools/sbe-stack-check/{red,green}-manifest.json`。本次无需重复执行同一 Linux 测试。

`overlay-files.txt` 列出相对最初失败快照的五个覆盖文件；`source-hashes.json` 对应最终源码。前三文件保持首次构造修复的哈希，原清单另存 `constructor-source-hashes.json`。Linux mapped 验证期间未修改生产源码；之后仅增加了一个 native typed decoder 边界单元。最终 `decoder-native-green.log` 为 5 项通过（含一个子进程辅助入口），覆盖生产容量短栈、正常同步、旧 token、身份/模板和精确小数拒绝；`decoder-native-clippy.log` 为该测试目标的 Clippy 结果。
