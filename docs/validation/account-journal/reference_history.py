#!/usr/bin/env python3
"""Independent fixed-format/history hash samples; does not run Rust."""
import hashlib
import json
import struct
from pathlib import Path

root = Path(__file__).resolve().parent
record = bytes.fromhex(
    "85777469636b776972652e6163636f756e742d7265636f7264015820"
    + "07" * 32
    + "0183057065787465726e616c2d6d696e696d756d8282010282c3507fffffffffffffffffffffffffffffff1826"
)
bootstrap = json.dumps({
    "format": "tickwire.account-journal",
    "version": 1,
    "account_codec_version": 1,
    "physical": {"stream_epoch": [2] * 16, "domain": [1] * 16,
                 "segment_max_bytes": 268435456},
    "baseline_digest": [7] * 32,
    "initial_writer_epoch": 4,
    "risk_domain_id": [1] * 16,
    "venue": 1,
    "account": 2,
    "environment": 2,
}, separators=(",", ":")).encode()


def digest(raw):
    return hashlib.sha256(raw).digest()


def frontier(count, chain):
    raw = (b"TWAF0001" + bytes([1]) * 16 + bytes([2]) * 16
           + bytes([7]) * 32 + struct.pack("<IQB", 1, 2, 2)
           + bytes(3) + struct.pack("<Q", count) + chain)
    assert len(raw) == 128
    return {"bytes": raw.hex(),
            "digest": digest(raw).hex()}


empty_chain = digest(b"tickwire.account-history-empty.v1" + bootstrap)
result = {"bootstrap": bootstrap.decode(), "record": record.hex(),
          "record_digest": digest(record).hex(),
          "empty": frontier(0, empty_chain)}
for writer in [4, 5]:
    chain = digest(b"tickwire.account-history-record.v1" + empty_chain
                   + struct.pack("<QQ", writer, 1) + digest(record))
    result[f"writer_{writer}"] = frontier(1, chain)
(root / "history-vectors.json").write_text(json.dumps(result, indent=2) + "\n")
print("wrote three 128-byte frontier samples and exact deterministic bootstrap")
