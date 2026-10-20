# 顶层索引合并条目

供 root 合入 `tla/MEMORY.md`、`tla/ROADMAP.md` 和总 Specs；本阶段没有修改这些顶层文件。

## book_state / L0_lifecycle — PASS (2026-10-20)

- Mode: faithful transcription（L1 明确使用准确快照与完整接受事件的输入契约）。
- Source: `crates/tickwire-book/src/lib.rs` SHA-256 `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`。
- Model SHA-256: `6da82cdf8a4f9b4a9a1416f9c78c668d32df6e5b665aad0609e161f9177bf4b3`。
- Property/output: [L0_lifecycle/05_output.md](L0_lifecycle/05_output.md)。
- Finding: none。
- Reviewed: root 独立源码对应审阅通过，无阻断项。
- TLC:
  - `L0_BookState.cfg`: generated 111457, distinct 1392, depth 6, PASS。
  - `L0_SpotScaling.cfg`: generated 3042646, distinct 8499, depth 6, PASS。
  - `L0_UsdM.cfg`: generated 115809, distinct 1880, depth 6, PASS。
  - `L0_UsdMScaling.cfg`: generated 2220496, distinct 10524, depth 6, PASS。
- Last synced: 2026-10-20；源码与锚点保持冻结。

## book_state / L1_coverage — PASS (2026-10-20)

- Mode: faithful transcription（L1 明确使用准确快照与完整接受事件的输入契约）。
- Source: `crates/tickwire-book/src/lib.rs` SHA-256 `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`。
- Model SHA-256: `c7e9ed09928e3faa69856693e8fdc2a68d537268a66a94a53f47a70a83c14e6e`。
- Property/output: [L1_coverage/05_output.md](L1_coverage/05_output.md)。
- Finding: none。
- Reviewed: root 独立源码对应审阅通过，无阻断项。
- TLC:
  - `L1_BookCoverage.cfg`: generated 45523, distinct 1471, depth 6, PASS。
  - `L1_Ask.cfg`: generated 45523, distinct 1471, depth 6, PASS。
  - `L1_AskScaling.cfg`: generated 585796, distinct 8244, depth 6, PASS。
  - `L1_BidScaling.cfg`: generated 519202, distinct 7050, depth 6, PASS。
- Last synced: 2026-10-20；源码与锚点保持冻结。

两层合计八组配置，所有 Next 动作 generated>0（L0 19，L1 15）。425 条源码行锚点一致。未来真实适配器与风险使用方集成独立评估；没有本阶段未完成配置。
