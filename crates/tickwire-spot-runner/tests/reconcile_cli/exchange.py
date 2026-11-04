"""Pinned loopback TLS peer for the real reconcile executable. No outside sockets."""

import base64
import collections
import hashlib
import hmac
import json
import os
import queue
import socket
from pathlib import Path
import socketserver
import ssl
import subprocess
import sys
import threading
import time
from urllib.parse import parse_qs, urlsplit

binary, directory, rest_host, ws_host = sys.argv[1:]
directory = Path(directory)
bybit = "bybit" in rest_host
calls = collections.Counter()
failures = []
state_lock = threading.Lock()
current_websocket = None
idle_rest_tls = {}
strategy_released_at = None
executed = None
filled_order = None
frame_lock = threading.Lock()
tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
tls.load_cert_chain(directory / "ca.pem", directory / "key.pem")


def accept_sni(sock, name, _context):
    assert name in (rest_host, ws_host), name
    sock.exchange_name = name


tls.set_servername_callback(accept_sni)


def digest(message):
    raw = hmac.digest(b"test-secret", message.encode(), "sha256")
    return raw.hex() if bybit else base64.b64encode(raw).decode()


def data(target):
    path, _, query = target.partition("?")
    if path == "/v5/user/query-api":
        return {"retCode": 0, "result": {"userID": 42, "apiKey": "test-key"}}
    if path == "/api/v5/account/config":
        return {"code": "0", "data": [{"uid": "42", "acctLv": "1"}]}
    if path == "/v5/account/info":
        return {"retCode": 0, "result": {"unifiedMarginStatus": 5, "marginMode": "REGULAR_MARGIN"}}
    if path == "/v5/spot-margin-trade/state":
        return {"retCode": 0, "result": {"spotMarginMode": "0"}}
    if path == "/v5/market/time":
        return {"retCode": 0, "result": {"timeNano": str(time.time_ns())}}
    if path == "/api/v5/public/time":
        return {"code": "0", "data": [{"ts": str(time.time_ns() // 1_000_000)}]}
    values = []
    if path in ("/v5/account/wallet-balance", "/api/v5/account/balance"):
        coins = []
        for currency, total in (
            [("BTC", "1.001"), ("USDT", "939.994")]
            if executed
            else [("BTC", "1"), ("USDT", "1000")]
        ):
            coins.append(
                {
                    "coin": currency,
                    "equity": total,
                    "walletBalance": total,
                    "locked": "0",
                    "spotBorrow": "0",
                    "borrowAmount": "0",
                }
                if bybit
                else {
                    "ccy": currency,
                    "eq": total,
                    "cashBal": total,
                    "availBal": total,
                    "frozenBal": "0",
                    "liab": "",
                }
            )
        values = [
            {
                "accountType": "UNIFIED",
                "totalInitialMargin": "0",
                "totalMaintenanceMargin": "0",
                "totalPerpUPL": "0",
                "coin": coins,
            }
            if bybit
            else {"details": coins, "uTime": "1700000000123"}
        ]
    elif (
        executed
        and not bybit
        and "after" not in parse_qs(query)
        and path
        in (
            "/api/v5/trade/orders-history",
            "/api/v5/trade/order",
            "/api/v5/trade/fills",
            "/api/v5/account/bills",
        )
    ):
        if path in ("/api/v5/trade/orders-history", "/api/v5/trade/order"):
            values = [filled_order]
        elif path == "/api/v5/trade/fills":
            values = [executed]
        else:
            values = [
                {
                    "billId": "90801",
                    "type": "2",
                    "subType": "1",
                    "instType": "SPOT",
                    "instId": "BTC-USDT",
                    "ordId": "801",
                    "tradeId": "701",
                    "ts": executed["ts"],
                }
            ]
    elif executed and path in (
        "/v5/order/history",
        "/v5/order/realtime",
        "/v5/execution/list",
        "/v5/account/transaction-log",
    ):
        params = parse_qs(query)
        if path == "/v5/order/history" or (
            path == "/v5/order/realtime" and "orderLinkId" in params
        ):
            values = [filled_order]
        elif path == "/v5/execution/list":
            values = [executed]
        elif path == "/v5/account/transaction-log":
            values = [
                {
                    "id": "bill801",
                    "type": "TRADE",
                    "category": "spot",
                    "symbol": "BTCUSDT",
                    "orderId": "801",
                    "tradeId": executed["execId"],
                    "currency": "USDT",
                    "transactionTime": executed["execTime"],
                }
            ]
    elif path == "/v5/market/instruments-info":
        values = [{"settleCoin": "USDT"}]
    elif path not in {
        "/v5/order/realtime",
        "/v5/order/history",
        "/v5/execution/list",
        "/v5/position/list",
        "/v5/account/transaction-log",
        "/api/v5/trade/orders-algo-pending",
        "/api/v5/tradingBot/grid/orders-algo-pending",
        "/api/v5/tradingBot/recurring/orders-algo-pending",
        "/api/v5/trade/orders-pending",
        "/api/v5/trade/order",
        "/api/v5/trade/orders-history",
        "/api/v5/trade/fills",
        "/api/v5/account/bills",
    }:
        raise AssertionError(target)
    category = parse_qs(query).get("category", ["spot"])[0]
    return (
        {"retCode": 0, "result": {"category": category, "list": values, "nextPageCursor": ""}}
        if bybit
        else {"code": "0", "data": values}
    )


def frame(sock, body):
    if not isinstance(body, bytes):
        body = json.dumps(body).encode()
    header = (
        bytes([129, len(body)]) if len(body) < 126 else b"\x81\x7e" + len(body).to_bytes(2, "big")
    )
    with frame_lock:
        sock.sendall(header + body)


def read_frame(file):
    header = file.read(2)
    if len(header) < 2 or (header[0] & 15) == 8:
        return None
    assert header[0] == 129 and header[1] & 128
    size = header[1] & 127
    if size == 126:
        size = int.from_bytes(file.read(2), "big")
    assert size < 8192
    mask, raw = file.read(4), file.read(size)
    return bytes(x ^ mask[i % 4] for i, x in enumerate(raw))


def websocket(sock, file, headers):
    global current_websocket
    seed = headers["sec-websocket-key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    accept = base64.b64encode(hashlib.sha1(seed.encode()).digest()).decode()
    sock.sendall(
        (
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
            f"Connection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        ).encode()
    )
    message = read_frame(file)
    if message is None:
        return
    login = json.loads(message)
    if bybit:
        key, expires, signature = login["args"]
        assert login["op"] == "auth" and key == "test-key"
        assert signature == digest(f"GET/realtime{expires}")
        frame(sock, {"op": "auth", "success": True, "conn_id": "fixture"})
    else:
        arg = login["args"][0]
        assert (
            login["op"] == "login"
            and arg["apiKey"] == "test-key"
            and arg["passphrase"] == "test-pass"
        )
        assert arg["sign"] == digest(arg["timestamp"] + "GET/users/self/verify")
        frame(sock, {"event": "login", "code": "0", "connId": "fixture"})
    message = read_frame(file)
    if message is None:
        return
    sub = json.loads(message)
    assert sub["op"] == "subscribe"
    if bybit:
        assert set(sub["args"]) == {"order.spot", "execution.spot", "wallet"}
        frame(
            sock,
            {"op": "subscribe", "success": True, "conn_id": "fixture", "req_id": sub["req_id"]},
        )
    else:
        assert len(sub["args"]) == 2
        for arg in sub["args"]:
            frame(sock, {"id": sub["id"], "event": "subscribe", "arg": arg, "connId": "fixture"})
    with state_lock:
        calls["ws_login"] += 1
        current_websocket = sock
    while (message := read_frame(file)) is not None:
        if bybit:
            assert json.loads(message)["op"] == "ping"
            frame(sock, {"op": "pong", "conn_id": "fixture", "args": ["test"]})
        else:
            assert message == b"ping"
            frame(sock, b"pong")


class Peer(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            with (
                tls.wrap_socket(self.request, server_side=True) as sock,
                sock.makefile("rb") as file,
            ):
                sock.settimeout(40)
                handshaken_at = time.monotonic()
                if sock.exchange_name == rest_host:
                    with state_lock:
                        idle_rest_tls[id(sock)] = handshaken_at
                try:
                    first = file.readline(8192)
                finally:
                    with state_lock:
                        idle_rest_tls.pop(id(sock), None)
                if not first:
                    return
                method, target, http = first.decode().strip().split()
                assert method in {"GET", "POST"} and http == "HTTP/1.1", first
                headers = {}
                while True:
                    line = file.readline(8192)
                    assert line
                    if line == b"\r\n":
                        break
                    k, v = line.decode().split(":", 1)
                    headers[k.lower()] = v.strip()
                if headers.get("upgrade", "").lower() == "websocket":
                    assert sock.exchange_name == ws_host
                    return websocket(sock, file, headers)
                assert sock.exchange_name == rest_host
                uri = urlsplit(target)
                if method == "POST":
                    assert sock.exchange_name == rest_host
                    body = file.read(int(headers["content-length"])).decode()
                    terms = json.loads(body)
                    if bybit:
                        assert (
                            target == "/v5/order/create" and headers["x-bapi-api-key"] == "test-key"
                        )
                        preimage = (
                            headers["x-bapi-timestamp"]
                            + "test-key"
                            + headers["x-bapi-recv-window"]
                            + body
                        )
                        signature = headers["x-bapi-sign"]
                        required = {
                            "category": "spot",
                            "symbol": "BTCUSDT",
                            "side": "Buy",
                            "price": "60000",
                            "qty": "0.001",
                            "orderType": "Limit",
                            "timeInForce": "GTC",
                            "orderLinkId": "liveorder801",
                        }
                        response = {
                            "retCode": 0,
                            "result": {"orderId": "801", "orderLinkId": "liveorder801"},
                        }
                    else:
                        assert (
                            target == "/api/v5/trade/order"
                            and headers["ok-access-key"] == "test-key"
                        )
                        assert (
                            headers["ok-access-passphrase"] == "test-pass"
                            and headers["x-simulated-trading"] == "1"
                        )
                        preimage = headers["ok-access-timestamp"] + "POST" + target + body
                        signature = headers["ok-access-sign"]
                        required = {
                            "tdMode": "cash",
                            "instId": "BTC-USDT",
                            "side": "buy",
                            "px": "60000",
                            "sz": "0.001",
                            "ordType": "limit",
                            "clOrdId": "liveorder801",
                        }
                        response = {
                            "code": "0",
                            "data": [
                                {
                                    "ordId": "801",
                                    "clOrdId": "liveorder801",
                                    "sCode": "0",
                                    "sMsg": "",
                                }
                            ],
                        }
                    assert signature == digest(preimage)
                    assert strategy_released_at is not None and handshaken_at < strategy_released_at
                    assert all(terms.get(k) == v for k, v in required.items()), terms
                    with state_lock:
                        calls["native_buy"] += 1
                        assert calls["native_buy"] == 1
                    ack = json.dumps(response).encode()
                    sock.sendall(
                        f"HTTP/1.1 200 OK\r\nContent-Length: {len(ack)}\r\n\r\n".encode() + ack
                    )
                    assert not file.read(1), "unexpected second execution request"
                    return
                if not target.endswith("/time"):
                    if bybit:
                        assert headers["x-bapi-api-key"] == "test-key"
                        msg = (
                            headers["x-bapi-timestamp"]
                            + "test-key"
                            + headers["x-bapi-recv-window"]
                            + uri.query
                        )
                        assert headers["x-bapi-sign"] == digest(msg)
                    else:
                        assert (
                            headers["ok-access-key"] == "test-key"
                            and headers["x-simulated-trading"] == "1"
                        )
                        assert headers["ok-access-passphrase"] == "test-pass"
                        assert headers["ok-access-sign"] == digest(
                            headers["ok-access-timestamp"] + "GET" + target
                        )
                body = json.dumps(data(target)).encode()
                with state_lock:
                    calls[uri.path] += 1
                sock.sendall(
                    f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body
                )
        except (ssl.SSLEOFError, ConnectionResetError, BrokenPipeError):
            # A gateway opened for a potential write may close without sending HTTP.
            pass
        except Exception as exc:
            with state_lock:
                failures.append(repr(exc))


class Server(socketserver.ThreadingTCPServer):
    daemon_threads = True


with Server(("127.0.0.1", 0), Peer) as server:
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    env = dict(
        os.environ,
        TW_TEST_REST=rest_host,
        TW_TEST_WS=ws_host,
        TW_TEST_PORT=str(server.server_address[1]),
        SSL_CERT_FILE=str(directory / "ca.pem"),
        LD_PRELOAD=str(directory.parent / "local.so"),
        TICKWIRE_SPOT_KEY="test-key",
        TICKWIRE_SPOT_SECRET="test-secret",
        TICKWIRE_SPOT_PASSPHRASE="test-pass",
    )
    for name in (
        "TICKWIRE_SPOT_KEY_FILE",
        "TICKWIRE_SPOT_SECRET_FILE",
        "TICKWIRE_SPOT_PASSPHRASE_FILE",
    ):
        env.pop(name, None)
    proofs = []
    config_path = directory / "config.json"
    deployment = json.loads(config_path.read_text())
    for restart in range(2):
        if restart:
            # The existing journal requires an independently approved, strictly newer writer.
            stale = subprocess.run(
                [binary, "reconcile", str(config_path)],
                env=env,
                capture_output=True,
                text=True,
                timeout=20,
            )
            assert (
                stale.returncode != 0
                and "spot_account_reconciled" not in stale.stdout
                and "epoch" in stale.stderr.lower()
            ), (stale.stdout, stale.stderr)
            deployment["policy"]["writer_epoch"] = 2
            config_path.write_text(json.dumps(deployment))
        try:
            proc = subprocess.run(
                [binary, "reconcile", str(directory / "config.json")],
                env=env,
                capture_output=True,
                text=True,
                timeout=20,
            )
        except subprocess.TimeoutExpired as exc:
            raise AssertionError((calls, failures, exc.stdout, exc.stderr)) from exc
        assert proc.returncode == 0, (calls, failures, proc.stdout, proc.stderr)
        (proof,) = [
            j
            for line in proc.stdout.splitlines()
            if (j := json.loads(line))["event"] == "spot_account_reconciled"
        ]
        assert not failures, failures
        proofs.append(proof)
    assert (
        proofs[0]["private_epoch"] > 0 and proofs[1]["private_epoch"] > proofs[0]["private_epoch"]
    ), proofs
    assert proofs[1]["private_sequence"] > proofs[0]["private_sequence"], proofs
    assert proofs[1]["account_sequence"] > proofs[0]["account_sequence"] > 0, proofs
    assert calls["ws_login"] >= 2
    for resource in (
        ("/v5/account/wallet-balance", "/v5/order/history", "/v5/execution/list")
        if bybit
        else ("/api/v5/account/balance", "/api/v5/trade/orders-history", "/api/v5/trade/fills")
    ):
        assert calls[resource] >= 4, calls
    # Same-process recovery exercises the stop/freeze path, per-socket durable epoch,
    # fresh signed REST scans and the newly acquired cut. The profile admits no orders.
    deployment["policy"]["writer_epoch"] = 3
    config_path.write_text(json.dumps(deployment))
    with subprocess.Popen(
        [binary, "live", str(directory / "config.json")],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as live:
        events = queue.Queue()

        def lines():
            for line in live.stdout:
                events.put(line.strip())
            events.put(None)

        threading.Thread(target=lines, daemon=True).start()
        qualified = []
        lost = False
        logs = []
        deadline = time.monotonic() + 25
        try:
            while len(qualified) < 2:
                line = events.get(timeout=max(0.01, deadline - time.monotonic()))
                assert line is not None, (live.poll(), calls, failures, logs)
                logs.append(line)
                if not line.startswith("{"):
                    continue
                event = json.loads(line)
                if event.get("event") == "spot_private_gap" and qualified:
                    assert event["private_epoch"] == qualified[0]["private_epoch"]
                    lost = True
                if event.get("event") == "spot_account_reconciled":
                    if qualified:
                        assert lost and event["private_epoch"] > qualified[0]["private_epoch"], logs
                        assert event["account_sequence"] > qualified[0]["account_sequence"], logs
                    qualified.append(event)
                    if len(qualified) == 1:
                        with state_lock:
                            current_websocket.shutdown(socket.SHUT_RDWR)
            (directory / "stop").touch()
            assert live.wait(timeout=6) == 0
        except Exception as exc:
            live.kill()
            raise AssertionError((calls, failures, logs)) from exc
    assert calls["ws_login"] >= 4 and not failures, (calls, failures)
    (directory / "stop").unlink()
    deployment["policy"]["writer_epoch"] = 4
    deployment["policy"]["mode"] = "trading"
    until = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW) + 60_000_000_000
    deployment["policy"]["owner_lease"] = {
        "lease_epoch": 1,
        "valid_until_ns": until,
        "isolation_record": [7] * 32,
    }
    deployment["policy"]["marks_per_feed"] = 128
    deployment["policy"]["strategy_grants"] = [
        {
            "logical_strategy_id": 8,
            "instance_id": [8] * 16,
            "strategy_generation": 1,
            "authorization_epoch": 1,
            "config_revision": 1,
            "valid_until_ns": until,
            "actions": {"submit": True, "cancel": False, "query": False, "amend": False},
            "required_feeds": [
                {
                    "key": {"topic_id": 101, "instrument_id": 12},
                    "instrument_version": 13,
                    "max_age_ns": 1_000_000_000,
                    "max_sequence_lag": 127,
                }
            ],
        }
    ]
    deployment["market_sources"] = [json.loads((directory / "market-source.json").read_text())]
    deployment["strategies"] = [
        {
            "logical_strategy_id": 8,
            "directory": str(directory / "trading-queue"),
            "first_sequence": 1,
        }
    ]
    config_path.write_text(json.dumps(deployment))
    trader_started_at = time.monotonic()
    with subprocess.Popen(
        [binary, "live", str(config_path)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as trader:
        (directory / "new-pid").write_text(str(trader.pid))
        (directory / "new-pid").rename(directory / "trader-pid")
        events = queue.Queue()

        def trader_lines():
            for line in trader.stdout:
                events.put(line.strip())
            events.put(None)

        threading.Thread(target=trader_lines, daemon=True).start()
        logs = []
        try:
            deadline = time.monotonic() + 18
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError("native buy deadline")
                line = events.get(timeout=max(0.01, deadline - time.monotonic()))
                assert line is not None, (trader.poll(), logs, calls, failures)
                logs.append(line)
                if not line.startswith("{"):
                    continue
                msg = json.loads(line)
                if msg.get("event") == "spot_account_reconciled":
                    # A fresh strategy may arrive well after the TLS handshake deadline.
                    # Observe the ready outbound socket before publishing any descriptor.
                    limit = time.monotonic() + 4
                    while time.monotonic() < limit:
                        with state_lock:
                            ready = any(
                                trader_started_at <= t < time.monotonic() - 0.05
                                for t in idle_rest_tls.values()
                            )
                        if ready:
                            break
                        time.sleep(0.01)
                    assert ready, "outbound execution TLS was not ready before the first order"
                    strategy_released_at = time.monotonic()
                    (directory / "strategy-go").touch()
                elif msg.get("event") == "submission":
                    assert msg["intent"] == 801 and "Queued" in msg["result"], logs
                elif msg.get("event") == "request_reply":
                    assert (
                        msg["intent"] == 801
                        and msg["member"] == 8
                        and "Acknowledged" in msg["reply"]
                    ), msg
                    break
            assert calls["native_buy"] == 1, logs
            at = str(time.time_ns() // 1_000_000)
            with state_lock:
                ws = current_websocket
            if bybit:
                executed = {
                    "category": "spot",
                    "symbol": "BTCUSDT",
                    "orderLinkId": "liveorder801",
                    "orderId": "801",
                    "execType": "Trade",
                    "execId": "0ab1bdf7-4219-438b-b30a-32ec863018f7",
                    "execPrice": "60000",
                    "execQty": "0.001",
                    "execValue": "60",
                    "execTime": at,
                    "execFee": "0.006",
                    "feeCurrency": "USDT",
                    "extraFees": "",
                    "isMaker": True,
                    "side": "Buy",
                }
                filled_order = {
                    "category": "spot",
                    "symbol": "BTCUSDT",
                    "orderLinkId": "liveorder801",
                    "orderId": "801",
                    "orderType": "Limit",
                    "timeInForce": "GTC",
                    "orderStatus": "Filled",
                    "price": "60000",
                    "qty": "0.001",
                    "cumExecQty": "0.001",
                    "cumExecValue": "60",
                    "side": "Buy",
                    "createdTime": at,
                    "updatedTime": at,
                    "isLeverage": "0",
                    "stopOrderType": "",
                }
                frame(ws, {"topic": "execution.spot", "creationTime": int(at), "data": [executed]})
                frame(ws, {"topic": "order.spot", "creationTime": int(at), "data": [filled_order]})
            else:
                executed = {
                    "instType": "SPOT",
                    "instId": "BTC-USDT",
                    "clOrdId": "liveorder801",
                    "ordId": "801",
                    "tradeId": "701",
                    "billId": "90801",
                    "subType": "1",
                    "side": "buy",
                    "execType": "M",
                    "fillPx": "60000",
                    "fillSz": "0.001",
                    "fillTime": at,
                    "fee": "-0.006",
                    "feeCcy": "USDT",
                    "ts": at,
                    "tradeQuoteCcy": "USDT",
                }
                filled_order = {
                    "instType": "SPOT",
                    "instId": "BTC-USDT",
                    "clOrdId": "liveorder801",
                    "ordId": "801",
                    "ordType": "limit",
                    "state": "filled",
                    "px": "60000",
                    "sz": "0.001",
                    "accFillSz": "0.001",
                    "side": "buy",
                    "cTime": at,
                    "uTime": at,
                    "tdMode": "cash",
                    "algoId": "",
                    "fee": "-0.006",
                    "feeCcy": "USDT",
                }
                private_order = dict(
                    filled_order,
                    tradeId="701",
                    fillPx="60000",
                    fillSz="0.001",
                    fillTime=at,
                    fillFee="-0.006",
                    fillFeeCcy="USDT",
                    execType="M",
                )
                frame(
                    ws, {"arg": {"channel": "orders", "instType": "SPOT"}, "data": [private_order]}
                )
            # A disconnect after the private fill forces fresh REST checks against the now
            # changed cash balance, matching order, individual execution and fee bill.
            time.sleep(0.2)
            ws.shutdown(socket.SHUT_RDWR)
            gap = False
            deadline = time.monotonic() + 20
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError("post-fill reconciliation deadline")
                line = events.get(timeout=max(0.01, deadline - time.monotonic()))
                assert line is not None, (trader.poll(), logs, calls, failures)
                logs.append(line)
                if not line.startswith("{"):
                    continue
                msg = json.loads(line)
                if msg.get("event") == "spot_private_gap":
                    gap = True
                if gap and msg.get("event") == "spot_account_reconciled":
                    break
            (directory / "stop").touch()
            assert trader.wait(timeout=6) == 0
            assert calls["native_buy"] == 1 and not failures, (calls, failures)
        except Exception as exc:
            trader.kill()
            raise AssertionError((calls, failures, logs)) from exc
    server.shutdown()
