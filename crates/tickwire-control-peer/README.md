# 控制共识服务

`tickwire-control-peer` 运行一个 `PeerService<Replica<GenerationStore>>`，以真实 mTLS 复制配置业务 WAL、快照和成员变更。业务扫描完成后才交付 Applied；网络线程与业务状态共用一个所有者。

```sh
cargo build -p tickwire-control-peer --bin tickwire-control-peer
target/debug/tickwire-control-peer --config /srv/tickwire/control/node-1.json
```

[示例配置](examples/node.json) 中的路径、地址、域、节点 ID、证书叶摘要均须替换为实际部署值。证书包含对应 cluster/group/node 的 URI、正确 server-name 和双向用途，CRL 必须有效；私钥只能有一个硬链接，且 group/other 没有任何权限。输入以不跟随末级符号链接、非阻塞的方式打开，再检查实际文件描述符的类型和大小；FIFO、目录和空文件拒绝。父目录及文件内容的修改权限须由部署管理。JSON 中未知字段、相对路径、重复地址/身份和超限配置拒绝。

首次三节点分别使用 `create`。新增身份使用 `join`，保留同一 `initial_voters`；它只描述原始引导身份，当前成员从持久 ConfState 读取。已有目录使用 `recover` 并提供合法 writer epoch。新节点尚未入组时不会投票、提案或完成安全读；受信目录中的新领导者可带它通过 Append 或快照加入。退休身份不能重新加入。

服务启动不会自动初始化配置或开放未认证命令端口。应用使用 `runner::assemble(&Configuration)` 取得同一个所有者，在 `poll` 之间经 `with_control` 处理已认证命令，并消费 `take_applied`。`api_runner::ApiOwner::assemble` 可在该所有者上组装 HTTPS；它依次驱动 peer 与已认证 HTTP 请求，Applied 仍由同一队列交付。`--turns N` 可限制驱动轮数；轮数结束不代表任何业务命令已提交。

服务循环每 256 个新 applied index 尝试本地压缩。CURRENT 绑定当前及上一完整代，应用快照先校验再安装；跨连接快照重传从头开始。单节点生命周期最多登记 16 个身份、64 项成员操作；容量耗尽拒绝新操作，已有结果继续可查。

接口、容量和执行证据见 [服务接口](../../docs/implementation/control-raft-service-interface.md) 与 [验证记录](../../docs/validation/control-raft-service/README.md)。
