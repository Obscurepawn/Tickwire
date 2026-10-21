# 配置服务启动组合

`tickwire-config-server` 读取 schema 1 启动配置，复用 `tickwire-control-peer::api_runner::ApiOwner`。每个进程只有一个持久 `Replica<GenerationStore>`；HTTP、peer 协议、业务 Applied 和快照由同一 owner 处理。启动不会调用业务 initialize，也不会绕过签名 bootstrap。

`config_runner::prepare` 验证有界配置、打开实际文件描述符并加载 peer/HTTP TLS、bootstrap trust、独立 evidence trust 和可选 `TWDSRC01` 资料包。所有 peer/HTTP/source 解析均在 listener 绑定前完成。`runner::Prepared` 保存已验证 trust 和固定配置，后续 assemble 不重读路径。TLS 私钥要求 group/other 权限为零、普通文件且只有一个硬链接；所有输入拒绝 leaf symlink、FIFO 与超长内容。包和 trust 被解析为 owned 内容，后续路径替换不修改已准备的 owner。签名 envelope 的当前资格仍在实际候选检查时由服务时钟核验，成功加载不授予部署权限。

`Prepared::assemble` 只消费已准备内容，创建唯一 `ApiOwner` 并安装可选 deployment provider。组装失败不会返回运行服务。正常 `--turns` 完成、STOP 标记和致命错误都执行 shutdown，取消 HTTP pending 请求并停止 peer I/O。循环有界消费 Applied，实际 applied 相对上次快照前进 256 后调用同 owner 的 compact_snapshot。

HTTP 客户端授权配置与 evidence issuer 配置分别加载。HTTP 正文不能选 trust key、资料包路径或替换 provider。证书只确定请求主体，当前 RBAC、第二次 ReadIndex、ETag 和 retained head 等资格沿既有 request 实现核验。

定向验证历史上覆盖：严格配置的编译成功行为 Red；实际文件加载失败、真实三节点 TLS 与 HTTPS 同 owner、二进制停止后恢复，以及资料包路径替换后的 owned 内容测试（原始日志曾存于已删除的 `docs/validation/config-server/`，当前以源码测试与 `cargo test` 为准）。没有为 CLI 新增共识状态机或形式模型。
