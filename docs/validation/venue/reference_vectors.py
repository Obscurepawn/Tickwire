#!/usr/bin/env python3
"""Independent schema transcription; does not import or execute the Rust codec."""
from pathlib import Path
import hashlib
import struct
import copy

ROOT = Path(__file__).resolve().parent / "vectors"

def head(major, n):
    if n < 24: return bytes([(major << 5) | n])
    for width, ai, fmt in [(1, 24, ">B"), (2, 25, ">H"), (4, 26, ">I"), (8, 27, ">Q")]:
        if n < (1 << (width * 8)): return bytes([(major << 5) | ai]) + struct.pack(fmt, n)
    raise ValueError(n)

def cbor(value):
    if isinstance(value, int):
        n = value if value >= 0 else -1-value
        if n < 1 << 64: return head(0 if value >= 0 else 1, n)
        return head(6, 2 if value >= 0 else 3) + cbor(n.to_bytes((n.bit_length()+7)//8, "big"))
    if isinstance(value, str):
        b = value.encode(); return head(3,len(b))+b
    if isinstance(value, bytes): return head(2,len(value))+value
    if isinstance(value, list): return head(4,len(value))+b"".join(map(cbor,value))
    raise TypeError(type(value))

def decimal(n,e):
    if n == 0: return [0,0]
    while n % 10 == 0 and e < 38: n //= 10; e += 1
    return [n,e]

def asset(i,s): return [i,1,s,-8]

request=[bytes([1])*16,1,2,2,1,3,4,5,6,100,7]
strategy=[8,bytes([2])*16,1,9,10,11,50,bytes([3])*16,bytes(16),0,bytes([4])*32]
instrument=[12,13,14,bytes([5])*32,"BTCUSDT",asset(1,"BTC"),asset(2,"USDT"),asset(2,"USDT"),decimal(1,-2),decimal(1,-5),[1]]
context=[request,strategy,instrument,[[1,1,1,1,12,13,49]]]
order=["test-1",1,1,[1,decimal(1,-3)],[decimal(60000,0)],[[1]],1,[1],0,0,[],[],[]]
command=["tickwire.venue-command",1,context,[1,order]]
identity=[bytes([1])*16,1,2,2,1,12,13,9,[10],"trade-event-1",[1234],200]
order_identity=[bytes([9])*16,3,4,["test-1"],["000123"],[],[],[],[],[],[]]
observation=[order_identity,3,1,1,[decimal(1,-3)],[decimal(5,-4)],[],[],[decimal(60000,0)]]
fill=[observation,"0000456",decimal(5,-4),decimal(60000,0),[],[],1,[[asset(2,"USDT"),decimal(-2,-2)],[asset(3,"BNB"),decimal(1,-5)]]]
event=["tickwire.venue-event",1,identity,[2,fill]]
min_fill=fill[:-1]+[[[[2,1,"USDT",0],decimal(-(1<<127),0)]]]
min_event=["tickwire.venue-event",1,identity,[2,min_fill]]
cash_event=copy.deepcopy(event)
cash_event[3][1][0][6]=[decimal(300001,-4)]
cash_event[3][1][4]=[decimal(300001,-4)]
cash_event[3][1][5]=[[asset(2,"USDT"),decimal(-11,-4)]]
ROOT.mkdir(exist_ok=True)
for name,value in [("spot-limit",command),("multi-asset-fill",event),("minimum-i128-fee",min_event),("reported-cash",cash_event)]:
    raw=cbor(value)
    (ROOT/(name+".hex")).write_text(raw.hex()+"\n")
    (ROOT/(name+".sha256")).write_text(hashlib.sha256(raw).hexdigest()+"\n")
    print(f"{name}: {len(raw)} bytes {hashlib.sha256(raw).hexdigest()}")
