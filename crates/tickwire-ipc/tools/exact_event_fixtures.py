#!/usr/bin/env python3
"""Independent literal samples: explicit offsets, no runtime codec or JSON schema input."""
from pathlib import Path
import struct
OUT=Path(__file__).resolve().parents[1]/'tests/fixtures/exact-events'
def put(b,offset,fmt,value):struct.pack_into('<'+fmt,b,offset,value)
def record(kind):
 b=bytearray(512)
 for off,fmt,n in [(0,'Q',1),(8,'H',kind),(10,'H',1),(14,'H',128),(22,'H',384),(48,'Q',1),(56,'Q',1),(64,'Q',1),(72,'Q',1),(80,'Q',1),(128,'Q',1),(136,'Q',2),(148,'H',1),(384,'H',2)]:put(b,off,fmt,n)
 return b
def save(name,b): (OUT/(name+'.hex')).write_text(b.hex()+'\n')
b=record(264)
for off,fmt,n in [(192,'Q',42),(200,'q',9901),(208,'q',100),(388,'B',2),(389,'B',1)]:put(b,off,fmt,n)
save('trade',b)
b=record(265)
for off,fmt,n in [(176,'Q',8),(184,'Q',8),(224,'q',99),(232,'q',1),(240,'q',100),(248,'q',2)]:put(b,off,fmt,n)
save('quote',b)
b=record(266);put(b,152,'Q',32)
for off,n,e in [(256,1179415,-2),(280,1178462659091,-8),(352,-3,-4)]:b[off:off+16]=n.to_bytes(16,'little',signed=True);put(b,off+16,'b',e)
save('mark',b)
# Standalone immutable control payloads, including v2 preflight types 38–40.
b=bytearray(128);b[:16]=bytes([1])*16
for off,n in [(16,2),(24,3),(64,4),(72,5)]:put(b,off,'Q',n)
b[32:64]=bytes([6])*32;b[80:112]=bytes([7])*32;put(b,112,'H',2);put(b,114,'H',1);save('restore-applied',b)
b=bytearray(96);b[:16]=bytes([1])*16
for off,n in [(16,2),(24,3),(48,4),(56,5)]:put(b,off,'Q',n)
b[32:48]=bytes([6])*16;put(b,64,'H',1);put(b,66,'H',2);save('checkpoint-seal',b)
b=bytearray(256);b[:16]=bytes([1])*16;put(b,16,'Q',4);put(b,24,'H',1);put(b,26,'H',2);b[32:48]=bytes([2])*16
for off,n in [(48,3),(56,4),(64,5),(72,6),(80,7),(184,8),(192,9),(200,10)]:put(b,off,'Q',n)
for start,value in [(88,11),(120,12),(152,13)]:b[start:start+32]=bytes([value])*32
save('checkpoint-sealed',b)
