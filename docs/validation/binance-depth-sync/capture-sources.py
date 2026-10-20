#!/usr/bin/env python3
"""Hash only the two official synchronization documents; retain protocol facts."""
import hashlib
import json
from pathlib import Path
import urllib.request

sources = [
    {
        "product": "Spot",
        "url": "https://raw.githubusercontent.com/binance/binance-spot-api-docs/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-streams.md",
        "revision": "f4a9c0be68235f758b46f571f7313c63d1306b25",
        "facts": {
            "order": ["subscribe", "buffer first U", "request snapshot", "replay in receipt order"],
            "retry_snapshot": "L < first buffered U",
            "discard_buffered": "u <= L",
            "apply": "u > L and U <= L + 1",
            "snapshot_limit_per_side": 5000,
            "outside_snapshot": "Unknown unless individually observed; no complete-book claim",
        },
    },
    {
        "product": "USD-M",
        "url": "https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly.md",
        "revision": "Retrieved content hash; publisher does not supply immutable revision",
        "facts": {
            "order": ["subscribe public depth", "buffer", "request snapshot", "replay in receipt order"],
            "discard_buffered": "u < L",
            "first_apply": "U <= L <= u",
            "subsequent_apply": "pu == previous applied u",
            "snapshot_limit_in_example": 1000,
            "quantity": "absolute; zero deletes, including absent price",
        },
    },
]
for source in sources:
    with urllib.request.urlopen(source["url"], timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {source['url']}")
        body = response.read()
    if not body:
        raise RuntimeError("Empty source")
    source["bytes"] = len(body)
    source["sha256"] = hashlib.sha256(body).hexdigest()
Path(__file__).with_name("official-sources.json").write_text(
    json.dumps(sources, indent=2, ensure_ascii=False) + "\n"
)
print("Official synchronization source hashes recorded.")
