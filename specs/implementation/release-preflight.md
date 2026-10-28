# 发布预检

`tickwire-release` 验证三份规范对象、完整 Operation/member、实际文件长度与摘要、输入顺序、目标和回滚能力。SDK 的 ObserveOnly/FixedDemo 工厂实际执行恢复或迁移，再验证输出状态。FixedDemo 的已消耗动作保留，新命令必须绑定目标实例和同一动作键；默认恢复方法与内置 declared rebuild 均返回 Unsupported。

Host 在实际子进程通过 Registry 认证后接收 RestoreApplied，核对真实 PID/UID/GID、binary hash、实际 Bootstrap、固定来源和同步输出。随后启动独立回滚 probe；probe 只连接认证控制 socket，以目标刚输出的状态执行回滚工厂，没有行情、账户或意图 FD。Host 等待 probe 的成功退出，保存两份回执及 Completion，才返回 Applied。worker 校验该确认与完整闭包后开始回调，后续 HandoverSeal 使用 schema2 引用 Completion。

Linux worker、来源重放与独立回滚从 `/proc/self/exe` 打开内核指向的实际运行映像，在同一文件描述符上按二进制预算读取并计算完整长度和 SHA-256；恢复和回滚还要求该对象精确等于已核验的 `Role::Binary` 制品。封印 memfd 与普通磁盘 ELF 使用相同内容校验，不要求运行映像的路径等于部署制品路径，也不把 `current_exe()` 的 readlink 结果重新当成可打开路径。启动入口被替换或删除不改变实际映像的身份。

启动入口与恢复制品是两种用途：`Role::Binary` 仍必须是完整清单绑定的可读固定制品，继续接受原目录权限、长度、摘要和重读身份检查；其缺失或变化仍关闭恢复。需要修改启动入口时，部署 owner 保留独立的相同内容制品供恢复闭包使用。本改动不允许用运行映像隐式替代丢失的恢复依赖，也不扩大 typed 消息和恢复对象预算。

Loader 核对签名清单所指向的实际 HostConfig/Bootstrap、RecoveryTemplate 及模板全部实际依赖路径和内容，固定来源必须来自旧 Host 已认证并由真实 report cursor 确认过的封存。Host 最多保存 64 份来源 metadata；超出保留范围的旧来源需要重新生成。Prepare/Commit 要求 Operation schema2、实际完成对象、当前账户产品/仓位模式和仍需管理的订单类型。Prepare quiesce 后再次发检查点 challenge，要求当前业务状态字节与固定来源一致；变化即拒绝该 operation。READY 的当前账户 cut 与全局固定迁移根分别校验。

本地恢复和复制端共用 `tickwire-release::closure` 验证器，递归验证 Completion、两份回执、目标/回滚输出以及固定来源的完整账户回调。保留 v1 历史字节读取，新 Prepare 明确返回 PreflightRequired；exec 重启不会从历史文件重造活进程资格。单次闭包最多 256 个不同对象、沿来源最多 16 层，并受恢复闭包字节上限限制。

真实 Red/Green、定向编译和单元结果见验证记录。完整 schema2 进程场景由现有 lab 正在接入。泛型策略的行情/定时器/配置合并输入尾部重放尚未实现；当前内置策略使用 quiesce 后业务字节不变的明确条件，没有用账户追平代替完整尾部重放。
