#!/usr/bin/env python3
"""Independent schema values and a recursive CBOR encoder; no Rust execution."""
from pathlib import Path
import copy
import hashlib
import struct

ROOT = Path(__file__).resolve().parent / "vectors"

def head(major, n):
    if n < 24:
        return bytes([(major << 5) | n])
    for width, info, fmt in [(1, 24, ">B"), (2, 25, ">H"), (4, 26, ">I"), (8, 27, ">Q")]:
        if n < 1 << (8 * width):
            return bytes([(major << 5) | info]) + struct.pack(fmt, n)
    raise ValueError(n)

def encode(value):
    if isinstance(value, int):
        n = value if value >= 0 else -1 - value
        if n < 1 << 64:
            return head(0 if value >= 0 else 1, n)
        return head(6, 2 if value >= 0 else 3) + encode(n.to_bytes((n.bit_length() + 7) // 8, "big"))
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return head(3, len(raw)) + raw
    if isinstance(value, bytes):
        return head(2, len(value)) + value
    if isinstance(value, list):
        return head(4, len(value)) + b"".join(encode(x) for x in value)
    raise TypeError(type(value))

def fixed(n):
    return [n, 8]

def units(n):
    return fixed(n * 100_000_000)

def asset(i, name):
    return [i, 1, name, -8]

instrument = [12, 13, 14, bytes([5]) * 32, "BTCUSDT", asset(1, "BTC"),
              asset(2, "USDT"), asset(2, "USDT"), [1, -2], [1, -5], [1]]
snapshot = [[bytes([1]) * 16, 1, 2, 2], 1, 0, [10, 1, 1, 1, 1], 1000,
            [[[1, 1], asset(1, "BTC"), units(1), units(0), units(0), 0, []],
             [[1, 2], asset(2, "USDT"), units(1000), units(0), units(0), 0, []]],
            [], []]
policy = [instrument, 1, 1, [[fixed(0), fixed(0), fixed(0)], 10, []],
          [], units(100), units(100), [[8, units(100)]]]
spot = ["tickwire.account-baseline", 1, snapshot, [[policy]]]
future = copy.deepcopy(spot)
future[2][7] = [[1, units(1000)]]
fp = future[3][0][0]
fp[0][10] = [2]
fp[1] = 2
fp[4] = [[1, 1, units(100), units(100), units(10), units(10),
          [[units(0), units(1_000_000), fixed(5_000_000), units(0), units(10)]],
          units(0), fixed(0), fixed(0), 0, units(0), units(0)]]
minimum = ["tickwire.account-record", 1, bytes([7])*32, 1,
           [5, "external-minimum", [[1, 2], [-(1 << 127), 38]]]]
unsorted = copy.deepcopy(spot)
unsorted[2][5].reverse()
ROOT.mkdir(exist_ok=True)
for name, value in [("spot-baseline", spot), ("futures-baseline", future),
                    ("minimum-cashflow", minimum), ("unsorted-baseline", unsorted)]:
    raw = encode(value)
    (ROOT / (name + ".hex")).write_text(raw.hex() + "\n")
    (ROOT / (name + ".sha256")).write_text(hashlib.sha256(raw).hexdigest() + "\n")
    print(f"{name}: {len(raw)} bytes {hashlib.sha256(raw).hexdigest()}")
