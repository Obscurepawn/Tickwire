#!/usr/bin/env python3
"""Fetch pinned official inputs and retain only relevant field inventories/hashes."""
import ast
import hashlib
import json
from pathlib import Path
import urllib.request

OUT = Path(__file__).resolve().parent
SPOT = "f4a9c0be68235f758b46f571f7313c63d1306b25"
FUTURES = "a54ec0b312c36706dc1d0b2cb33c658d55b72e89"
BASE = (
    "clients/derivatives_trading_usds_futures/src/"
    "binance_sdk_derivatives_trading_usds_futures/websocket_streams/models/"
)
MODELS = [
    "account_update.py",
    "account_update_a.py",
    "account_update_ab_inner.py",
    "account_update_ap_inner.py",
    "algo_update.py",
    "algo_update_o.py",
    "order_trade_update.py",
    "order_trade_update_o.py",
]


def fetch(url):
    with urllib.request.urlopen(url, timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {url}")
        body = response.read()
    if not body:
        raise RuntimeError(f"Empty official source: {url}")
    return body


def save(name, value):
    (OUT / name).write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


sources = []
for name in MODELS:
    url = f"https://raw.githubusercontent.com/binance/binance-connector-python/{FUTURES}/{BASE}{name}"
    body = fetch(url)
    fields = []
    for node in ast.parse(body).body:
        if not isinstance(node, ast.ClassDef):
            continue
        for statement in node.body:
            if (
                isinstance(statement, ast.AnnAssign)
                and isinstance(statement.target, ast.Name)
                and statement.target.id not in ("additional_properties", "__properties")
            ):
                fields.append(
                    {
                        "name": statement.target.id,
                        "type": ast.unparse(statement.annotation),
                    }
                )
    sources.append(
        {"url": url, "sha256": hashlib.sha256(body).hexdigest(), "fields": fields}
    )
save(
    "official-futures-fields.json",
    {
        "repository": "binance/binance-connector-python",
        "commit": FUTURES,
        "note": "字段名与类型由官方生成模型 AST 提取；未采用其所有字段 Optional 的宽松业务校验。sha256 是下载的完整原始文件哈希。",
        "sources": sources,
    },
)
sources = []
for name in ("rest-api.md", "web-socket-streams.md", "user-data-stream.md"):
    url = f"https://raw.githubusercontent.com/binance/binance-spot-api-docs/{SPOT}/{name}"
    body = fetch(url)
    sources.append({"url": url, "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)})
save(
    "official-spot-sources.json",
    {"repository": "binance/binance-spot-api-docs", "commit": SPOT, "sources": sources},
)
print("Pinned source fields and hashes written.")
