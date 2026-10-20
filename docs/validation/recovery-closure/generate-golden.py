#!/usr/bin/env python3
"""Independent CBOR/input encoder for the closure fixture; standard library only."""
import hashlib
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[3]
def sha(x): return hashlib.sha256(x).digest()
def head(major, n):
    if n < 24: return bytes([major * 32 + n])
    width = next(w for w in (1, 2, 4, 8) if n < 1 << (8*w))
    return bytes([major*32 + {1:24, 2:25, 4:26, 8:27}[width]]) + n.to_bytes(width, 'big')
def cbor(x):
    if isinstance(x, int): return head(0, x)
    if isinstance(x, bytes): return head(2,len(x)) + x
    return head(4,len(x)) + b''.join(cbor(v) for v in x)
assert cbor(23).hex() == '17' and cbor(24).hex() == '1818'
assert cbor(256).hex() == '190100' and cbor(2**32).hex() == '1b0000000100000000'
objects = []
for kind in range(1,12):
    raw = bytes([kind])*(kind+1)
    objects.append([kind, sha(raw), len(raw), 1, bytes([1])*16])
raw = b'TWINP001' + bytes([9])*16 + (1).to_bytes(8,'big') + (5).to_bytes(4,'big') + bytes(32)
last = bytes(32)
for seq, payload in enumerate((b'quote', b'fill', b'timer', b'config', b'model'), 1):
    row = seq.to_bytes(8,'big') + bytes([seq]) + len(payload).to_bytes(4,'big') + payload
    last = sha(b'tickwire.input-row.v1' + last + row)
    raw += row
objects.append([12,sha(raw),len(raw),1,bytes([1])*16])
binding = [bytes([2])*16,3,4,objects[0][1],5,6,7]
manifest = [b'tickwire.recovery-manifest',1,binding,1,bytes([3])*16,objects[3][1],objects[6][1],1,bytes([4])*32,bytes([5])*16,[bytes([9])*16,1,bytes(32),5,last],[objects,[[0,i] for i in range(1,12)]]]
encoded = cbor(manifest)
outputs = {'crates/tickwire-recovery/tests/manifest.hex': encoded.hex()+'\n',
           'docs/validation/recovery-closure/golden.sha256': sha(encoded).hex()+'  manifest.cbor\n'+sha(raw).hex()+'  inputs.bin\n'}
for name, content in outputs.items():
    path=ROOT/name
    if '--check' in sys.argv:
        assert path.read_text() == content, name
    else:
        path.write_text(content)
print(f'PASS: independent {len(encoded)}-byte manifest, {len(raw)}-byte ordered-input chunk')
