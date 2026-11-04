#!/usr/bin/env python3
"""Run one public spot publisher and two observation-only SDK strategy workers on Linux."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time

REPO = Path(__file__).resolve().parents[1]
MD_EXAMPLES = REPO / "crates/tickwire-md/examples"


def run(*args, **kwargs):
    return subprocess.run(args, cwd=REPO, check=True, **kwargs)


def headers(stream):
    lines = []
    while len(lines) < 64:
        line = stream.readline(4097)
        if not line or len(line) > 4096:
            raise ValueError("incomplete or excessive HTTP headers")
        if line == b"\r\n":
            return lines
        lines.append(line.decode("ascii").strip())
    raise ValueError("too many HTTP headers")


def read_frame(stream):
    head = stream.read(2)
    if len(head) != 2 or head[0] != 0x81 or head[1] & 128 == 0:
        raise ValueError("expected one masked text subscription")
    size = head[1] & 127
    if size > 125:
        raise ValueError("oversized subscription")
    key, body = stream.read(4), stream.read(size)
    if len(body) != size or len(key) != 4:
        raise ValueError("incomplete subscription")
    return json.loads(bytes(b ^ key[i % 4] for i, b in enumerate(body)))


def send_frame(conn, body):
    data = json.dumps(body, separators=(",", ":")).encode()
    size = len(data)
    prefix = bytes([0x81, size]) if size < 126 else b"\x81\x7e" + struct.pack(">H", size)
    conn.sendall(prefix + data)


def fixture(root, venue, stop):
    # The private CA belongs only to this invocation. Its leaf authenticates the
    # venue name to our loopback client without touching the machine trust store.
    host = {"bybit": "stream.bybit.com", "okx": "ws.okx.com"}[venue]
    ca_key, ca, key, csr, cert = [root / n for n in ("ca.key", "ca.pem", "leaf.key", "leaf.csr", "leaf.pem")]
    common = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    run("openssl", "req", "-new", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=Tickwire local demonstration", "-addext", "basicConstraints=critical,CA:TRUE",
        "-keyout", ca_key, "-out", ca, **common)
    run("openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=" + host,
        "-keyout", key, "-out", csr, **common)
    extensions = root / "leaf.ext"
    extensions.write_text(f"subjectAltName=DNS:{host}\nbasicConstraints=critical,CA:FALSE\nextendedKeyUsage=serverAuth\n")
    run("openssl", "x509", "-req", "-in", csr, "-CA", ca, "-CAkey", ca_key, "-CAcreateserial",
        "-days", "1", "-extfile", extensions, "-out", cert, **common)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    listener = socket.create_server(("127.0.0.1", 0))
    listener.settimeout(0.5)
    counts = {"connections": 0, "subscriptions": 0}
    ready = threading.Event()

    def serve():
        try:
            with listener:
                while not stop.is_set():
                    try:
                        tunnel, _ = listener.accept()
                        break
                    except TimeoutError:
                        continue
                else:
                    return
                with tunnel:
                    tunnel.settimeout(15)
                    with tunnel.makefile("rb", buffering=0) as plain:
                        if headers(plain)[0] != f"CONNECT {host}:443 HTTP/1.1":
                            raise ValueError("unexpected tunnel target")
                    tunnel.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    with context.wrap_socket(tunnel, server_side=True) as conn, conn.makefile("rb") as stream:
                        request = headers(stream)
                        key_line = next(line for line in request if line.lower().startswith("sec-websocket-key:"))
                        ws_key = key_line.split(":",1)[1].strip()
                        accept = base64.b64encode(hashlib.sha1((ws_key+"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
                        conn.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "+accept+"\r\n\r\n").encode())
                        args = ["orderbook.1.BTCUSDT"] if venue == "bybit" else [{"channel":"bbo-tbt","instId":"BTC-USDT"}]
                        sub = read_frame(stream)
                        if sub["op"] != "subscribe" or sub["args"] != args:
                            raise ValueError("unexpected public subscription")
                        counts.update(connections=1, subscriptions=1)
                        ack = ({"success":True,"op":"subscribe","req_id":"twbbo"} if venue == "bybit"
                               else {"id":"twbbo","event":"subscribe","arg":args[0],"connId":"lab"})
                        send_frame(conn, ack)
                        seq = 1
                        while not stop.is_set():
                            final = (root / "run/workers.ready").exists()
                            if final:
                                seq = 1_000_000
                            ts = time.time_ns() // 1_000_000
                            if venue == "bybit":
                                update = {"topic":args[0],"type":"snapshot","ts":ts,"data":{"s":"BTCUSDT","u":seq,"seq":seq,
                                          "b":[["100.0","0.01"]],"a":[["100.1","0.02"]]}}
                            else:
                                update = {"arg":args[0],"data":[{"bids":[["100.0","0.01","0","1"]],
                                          "asks":[["100.1","0.02","0","1"]],"ts":str(ts),"seqId":seq}]}
                            send_frame(conn, update)
                            ready.set()
                            if final:
                                stop.wait()
                                break
                            seq += 1
                            stop.wait(1.0)
        except (OSError, ValueError) as exc:
            if not stop.is_set():
                print(f"loopback exchange: {exc}", file=sys.stderr, flush=True)

    address = f"127.0.0.1:{listener.getsockname()[1]}"
    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    return address, ca, ready, counts


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--venue", choices=["bybit","okx"], default="bybit")
    p.add_argument("--live", action="store_true", help="read the venue's public WebSocket instead of local fixture data")
    p.add_argument("--proxy", help="HTTP CONNECT proxy IP:port for --live")
    p.add_argument("--keep", action="store_true", help="retain market, worker, group and result files")
    args = p.parse_args()
    if sys.platform != "linux" or (args.proxy and not args.live):
        p.error("requires Linux; --proxy requires --live")
    run("cargo", "build", "--locked", "-p", "tickwire-md", "-p", "tickwire-strategy-example", "--bins")
    built = run("cargo", "test", "--locked", "-p", "tickwire-app", "--test", "parameters_process",
                "--no-run", "--message-format=json", stdout=subprocess.PIPE, text=True)
    artifacts = (json.loads(line) for line in built.stdout.splitlines())
    controller = Path(next(a["executable"] for a in artifacts if a.get("executable") and
                           a["target"]["name"] == "parameters_process"))
    binaries = controller.parent.parent
    temp = Path(tempfile.mkdtemp(prefix="tw2-", dir="/tmp"))
    root = temp / "run"
    stop = threading.Event()
    md = None
    try:
        worker = temp / "tickwire-counting-worker"
        run("strip", "--strip-debug", "-o", worker, binaries / "tickwire-counting-worker")
        md_bin = binaries / "tickwire-md"
        prepare = [sys.executable, MD_EXAMPLES / "prepare-spot-market.py", args.venue, root, "--binary", md_bin]
        ready = None
        if not args.live:
            proxy, ca, ready, counters = fixture(temp, args.venue, stop)
            prepare += ["--proxy", proxy]
        elif args.proxy:
            prepare += ["--proxy", args.proxy]
        run(*prepare, stdout=subprocess.DEVNULL)
        config = root / "market.json"
        if not args.live:
            values = json.loads(config.read_bytes())
            values["source"]["ca_pem"] = str(ca)
            config.write_text(json.dumps(values))
        with (root/"market.log").open("w") as log:
            md = subprocess.Popen([md_bin, "--spot-market", config], cwd=REPO, stderr=log)
            until = time.monotonic() + 35
            while not (root/"control/spot-market.active").exists() or (ready and not ready.is_set()):
                if md.poll() is not None or time.monotonic() >= until:
                    raise RuntimeError("public market did not start: " + (root/"market.log").read_text()[-800:])
                time.sleep(0.05)
            env = dict(os.environ, TICKWIRE_SPOT_DEMO=str(root), TICKWIRE_SPOT_PID=str(md.pid),
                       TICKWIRE_PARAMETERS_WORKER=str(worker), TICKWIRE_PARAMETERS_KEEP="1")
            if not args.live:
                env["TICKWIRE_SPOT_FIXTURE_SEQUENCE"] = "1000000"
            else:
                env.pop("TICKWIRE_SPOT_FIXTURE_SEQUENCE", None)
            with (root / "host.log").open("w") as host_log:
                host = subprocess.Popen([controller, "external_spot::two_registered_workers_read_external_spot_market",
                                         "--exact", "--ignored", "--nocapture"], env=env, cwd=REPO,
                                        stdout=host_log, stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    if host.wait(timeout=140) != 0:
                        raise RuntimeError("worker host failed:\n" + (root / "host.log").read_text()[-2000:])
                finally:
                    # Kill the entire demo process group on timeout or interruption, including any
                    # worker spawned before the controller finished recording its PID.
                    if host.returncode != 0:
                        try:
                            os.killpg(host.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        host.wait()
        result = json.loads((root/"result.json").read_bytes())
        result.update(source="venue_public" if args.live else "loopback_fixture", venue=args.venue)
        if not args.live:
            result.update(counters)
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result), flush=True)
    finally:
        stop.set()
        if md and md.poll() is None:
            md.terminate()
            try:
                md.wait(timeout=3)
            except subprocess.TimeoutExpired:
                md.kill()
                md.wait()
        if args.keep:
            print(f"Files: {root}", file=sys.stderr)
        else:
            shutil.rmtree(temp)

if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        sys.exit(str(error))
