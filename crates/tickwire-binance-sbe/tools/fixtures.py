#!/usr/bin/env python3
"""Independent hand-transcribed sample layout; does not import the codec or XML generator."""
from pathlib import Path
import struct

ROOT = Path(__file__).resolve().parents[1] / 'tests/fixtures'
ROOT.mkdir(exist_ok=True)
def put(name, body):
    (ROOT / (name + '.hex')).write_text(body.hex() + '\n')
def header(block, template):
    return struct.pack('<HHHH', block, template, 1, 0)
symbol = b'\x07BTCUSDT'
trades = header(18,10000) + struct.pack('<qqbb',1000000,999999,-2,-3)
trades += struct.pack('<HI',25,2) + struct.pack('<qqqB',7,6500000,100,1) + struct.pack('<qqqB',8,6500100,200,0)
put('trades',trades+symbol)
put('quote',header(50,10001)+struct.pack('<qqbbqqqq',1000000,42,-2,-3,6500000,100,6500100,200)+symbol)
snapshot = header(18,10002)+struct.pack('<qqbb',1000000,42,-2,-3)
snapshot += struct.pack('<HHqqqq',16,2,6500000,100,6499900,200)
snapshot += struct.pack('<HHqqqq',16,2,6500100,300,6500200,400)
put('partial',snapshot+symbol)
diff = header(26,10003)+struct.pack('<qqqbb',1000000,43,44,-2,-3)
diff += struct.pack('<HHqq',16,1,6500000,0)+struct.pack('<HHqq',16,1,6500100,500)
put('diff',diff+symbol)
