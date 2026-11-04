#!/usr/bin/env python3
"""Prepare a fresh, observation-only Linux demo for tickwire-md --spot-market."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import uuid


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("venue", choices=["bybit", "okx"])
    parser.add_argument("directory", type=Path, help="new, private directory")
    parser.add_argument("--binary", required=True, type=Path, help="built tickwire-md executable")
    parser.add_argument("--proxy", help="optional HTTP CONNECT proxy IP:port")
    args = parser.parse_args()
    template = Path(__file__).resolve().parent
    source = json.loads((template / f"{args.venue}-spot-quote.json").read_text())
    if args.proxy:
        source["proxy_addr"] = args.proxy
    arch = {"x86_64": 1, "aarch64": 2}[platform.machine()]
    binary = args.binary.resolve(strict=True)
    with binary.open("rb") as stream:
        build = hashlib.file_digest(stream, "sha256").digest()
    boot = uuid.UUID(Path("/proc/sys/kernel/random/boot_id").read_text().strip()).bytes
    root = args.directory.resolve()
    os.umask(0o077)
    root.mkdir(mode=0o700)
    (root / "admissions").mkdir()
    venue_id = {"bybit": 2, "okx": 3}[args.venue]
    resource = json.loads((template / "topic-101.json").read_text())
    resource.update(topic_id=source["instrument_id"], resource_id=list(uuid.uuid4().bytes),
                    channel_id=list(uuid.uuid4().bytes), capacity=256, arena_classes=[[65536, 32]])
    resource["identity"].update(instance=list(uuid.uuid4().bytes), host_boot=list(boot),
                                namespace=list(uuid.uuid4().bytes), build_hash=list(build), arch=arch)
    write_json(root / "resource.json", resource)
    # These are local example economics. The deployed operator pins its own approved catalog.
    catalog = {
        "schema": 1, "venue": venue_id, "environment": "Production", "market": "Spot",
        "catalog_version": "1",
        "assets": [{"id": "1", "version": "1", "symbol": "BTC", "exponent": -8},
                   {"id": "2", "version": "1", "symbol": "USDT", "exponent": -8}],
        "instruments": [{"id": str(source["instrument_id"]), "version": str(source["instrument_version"]),
                         "symbol": source["symbol"], "product": "Spot", "base": "1", "quote": "2",
                         "settlement": "2", "price_tick": source["price_tick"],
                         "quantity_step": source["quantity_step"], "contract": {"kind": "Cash"}}],
    }
    catalog_path = root / "catalog.json"
    write_json(catalog_path, catalog)
    digest = hashlib.sha256(catalog_path.read_bytes()).hexdigest()
    config = {
        "resource": str(root / "resource.json"), "clock_generation": 1,
        "source_id": 1, "instrument_set_version": 1, "source": source,
        "catalog": {"path": str(catalog_path), "reference": f"artifact://demo/catalog/{args.venue}@sha256:{digest}"},
        "server": {"socket": str(root / "md.sock"), "admission_directory": str(root / "admissions"),
                   "control_directory": str(root / "control"), "control_stream": list(uuid.uuid4().bytes),
                   "control_domain": list(uuid.uuid4().bytes), "control_writer_epoch": 1},
    }
    write_json(root / "market.json", config)
    print(root / "market.json")


if __name__ == "__main__":
    main()
