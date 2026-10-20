#!/usr/bin/env python3
"""Conservative schema size bound; sums optional fields even where validation excludes coexistence."""
u64, u32, i8 = 9, 5, 2
id16, hash32 = 17, 34
text64, text32 = 66, 34
decimal = 1 + 18 + i8  # array header + preferred i128 bignum + exponent
option = lambda n: 1+n
asset = 1+u64+u64+text32+i8
contract = 1+1+u64+decimal+1
instrument = 1+3*u64+hash32+text64+3*asset+2*decimal+contract
request = 1+id16+u32+1+u64+1+6*u64
strategy = 1+6*u64+3*id16+u32+hash32
trigger_ref = 1+7*u64
context = 1+request+strategy+instrument+1+8*trigger_ref
quantity = 1+1+decimal
tif = 1+1+u64
mode = 1+1+1+1+1
trigger = 1+decimal+1+1
trailing = 1+option(decimal)+1+(1+1+decimal)
order = 1+text64+1+1+quantity+option(decimal)+option(tif)+1+mode+1+1+option(trigger)+option(trailing)+option(decimal)
list_order = 1+id16+text64+1+(1+3*order)
command = 1+(1+len("tickwire.venue-command"))+1+context+(1+1+list_order)
identity = 1+id16+u32+1+u64+1+3*u64+option(u64)+text64+option(u64)+u64
order_identity = 1+id16+2*u64+6*option(text64)+2*option(id16)
observation = 1+order_identity+3+5*option(decimal)
delta = 1+asset+decimal
fill = 1+observation+text32+2*decimal+option(decimal)+option(delta)+1+(1+8*delta)
event = 1+(1+len("tickwire.venue-event"))+1+identity+(1+1+fill)
assert command < 16384 and event < 16384
print(f"Conservative command bound: {command} bytes")
print(f"Conservative event bound: {event} bytes")
print("Both are below the 16384-byte pre-decode hard cap.")
