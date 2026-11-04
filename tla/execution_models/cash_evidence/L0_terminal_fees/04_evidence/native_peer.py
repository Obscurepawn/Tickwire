#!/usr/bin/python3
"""Run the existing TLS exchange fixture with contradictory terminal reports."""
import os
from pathlib import Path
import sys
import textwrap

peer = Path(sys.argv[1])
assert peer.name == "exchange.py", peer
source = peer.read_text()
# Reuse the maintained HTTPS/WS encoders, credentials, UID checks, config, and real process.
# Until the acknowledged order, every byte from the original peer is unchanged.
source = source.replace("def data(target):", "def normal_data(target):", 1)
source = source.replace("def frame(sock, body):", '''
def data(target):
    body = normal_data(target)
    if filled_order is None:
        return body
    path, _, query = target.partition("?")
    args = parse_qs(query)
    if "after" in args:
        return body
    if bybit:
        if path == "/v5/order/history" or (path == "/v5/order/realtime" and "orderLinkId" in args):
            body["result"]["list"] = [dict(filled_order, orderStatus="Cancelled",
                                          cumExecQty="0", cumExecValue="0")]
    elif path in ("/api/v5/trade/order", "/api/v5/trade/orders-history"):
        body["data"] = [dict(filled_order, state="canceled", accFillSz="0",
                             fee="0", feeCcy="")]
    return body

def frame(sock, body):''', 1)
# No individual execution or cash bill. The account snapshot remains at the genesis balances.
source = source.replace('            if bybit:\n                executed = {',
                        '            if bybit:\n                unused_execution = {', 1)
source = source.replace('            else:\n                executed = {',
                        '            else:\n                unused_execution = {', 1)
source = source.replace('                frame(ws, {"topic": "execution.spot", "creationTime": int(at), "data": [executed]})\n', '')
a = source.index('                private_order = dict(')
b = source.index('            # A disconnect', a)
source = source[:a] + '''                frame(ws, {"arg":{"channel":"orders","instType":"SPOT"},
                           "data":[dict(filled_order, tradeId="", fillSz="0")]})
''' + source[b:]
a = source.index('            # A disconnect', a)
b = source.index('        except Exception as exc:', a)
observe = """
deadline = time.monotonic() + 22
while time.monotonic() < deadline:
    line = events.get(timeout=max(0.05, deadline - time.monotonic()))
    assert line is not None, logs
    logs.append(line)
    if "Unreconciled" in line:
        break
else:
    raise AssertionError(("missing rejected fee closure", logs))
assert not failures, failures
(directory / "stop").touch()
assert trader.wait(timeout=6) == 0
(directory / "stop").unlink()
deployment["policy"]["writer_epoch"] += 1
config_path.write_text(json.dumps(deployment))
reopen = subprocess.run([binary, "reconcile", str(config_path)], env=env,
                        capture_output=True, text=True, timeout=25)
assert reopen.returncode != 0 and "Unreconciled" in reopen.stderr, (reopen.stdout, reopen.stderr)
with open(os.environ["CASH_REPLAY_TRANSCRIPT"], "a") as out:
    print(rest_host, "running:", line, "reopen:", reopen.stderr.strip(), file=out)
"""
source = source[:a] + textwrap.indent(observe, " " * 12) + source[b:]
sys.argv = sys.argv[1:]
exec(compile(source, str(peer), "exec"), {"__name__": "__main__", "__file__": str(peer)})
