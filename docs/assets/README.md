# 设计图与复现

主设计直接嵌入 PNG，普通 Markdown 阅读器也能显示；SVG 用于放大或导出。所有图片描述的是拟建系统。

| 图 | 可编辑源 | 生成方式 |
|---|---|---|
| 多进程与共享内存架构 | [JSON](tickwire-architecture.json) | Archify 2.17，另交付可交互 HTML |
| 进程交互 | [Mermaid](process-topology.mmd) | mermaid-cli 11.17.0 |
| 订单状态 | [Mermaid](order-state.mmd) | mermaid-cli 11.17.0 |
| 恢复时序 | [Mermaid](recovery-sequence.mmd) | mermaid-cli 11.17.0 |
| 策略原位升级 | [Mermaid](strategy-rollout.mmd) | mermaid-cli 11.17.0 |
| 配置发布与本地持久化 | [Mermaid](config-release.mmd) | mermaid-cli 11.17.0 |

逻辑架构图以一个行情进程、两个策略实例和一个账户执行进程代表多实例部署。行情及 exec 参考价格使用 Disruptor 模型的多消费者共享广播 ring：payload 只存一份，每个消费者有独立 cursor / gating。recorder 通过独立 QoS / 采集中继接入，不作为交易 ring 的长期 gating 消费者。策略意图和订单回报分别使用有界共享内存 SPSC 通道；控制消息使用 Unix Domain Socket（UDS）。进程图补充 Binance 连接、独立 guardian 和 exec 内部 writer，guardian 的远端控制使用认证连接。图中指定了通道模型，没有指定共享内存库。

策略原位升级图只展示一个 exec 的交接，图中新的 generation 指意图与回报通道代次：旧 epoch 的撤销屏障持久化后，新策略载入检查点、订单和仓位快照并追平，最后取得新 epoch 授权。已有订单留在 exec 持续管理；跨多个 exec 的撤销与授权分别确认，不表示原子切换。

Mermaid 主题与字体配置保存于 [mermaid-config.json](mermaid-config.json)。运行目录为仓库根；浏览器配置文件需填写本机 Chrome/Chromium 的可执行路径。示例：

```bash
PUPPETEER_SKIP_DOWNLOAD=true npm exec --yes --package=@mermaid-js/mermaid-cli@11.17.0 -- mmdc \
  -i docs/assets/process-topology.mmd -o docs/assets/process-topology.svg \
  -p /tmp/tickwire-puppeteer.json -c docs/assets/mermaid-config.json -w 1800 -b white
```

将输出扩展名改成 `.png` 并加 `-s 2` 得到本次使用的 PNG；对其他 `.mmd` 使用同样步骤。本次浏览器配置如下，可在同一环境创建上述临时文件；其他机器替换 `executablePath`：

```json
{
  "executablePath": "/home/obscure/.cache/ms-playwright/chromium-1232/chrome-linux64/chrome",
  "args": ["--no-sandbox", "--disable-dev-shm-usage"]
}
```

Archify 在本机安装于 `/home/obscure/.codex/skills/archify`：

```bash
node /home/obscure/.codex/skills/archify/bin/archify.mjs validate architecture \
  docs/assets/tickwire-architecture.json --quality showcase --json
node /home/obscure/.codex/skills/archify/bin/archify.mjs deliver architecture \
  docs/assets/tickwire-architecture.json docs/assets/tickwire-architecture.html --quality showcase --json
ARCHIFY_CHROME=/home/obscure/.cache/ms-playwright/chromium-1232/chrome-linux64/chrome \
  node /home/obscure/.codex/skills/archify/bin/archify.mjs visual-check \
  docs/assets/tickwire-architecture.html --json
```

静态架构 SVG/PNG 由交付 HTML 的导出功能生成；不要截图浏览器工具栏充当架构图。`visual-check.*` 文件保存浏览器证据，不属于主设计插图。[资产清单](diagram-assets.manifest.json)记录最终各图的尺寸与 SHA-256，[视觉复核](tickwire-architecture.review.json)单独记录实际看图结果；自动检查回执中的 `visualReview: pending` 保持原样。详细结果见[设计交付核验](../design-review.md)。
