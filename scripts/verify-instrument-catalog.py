#!/usr/bin/env python3
"""Independently verify economic catalog vectors; no I/O beyond the vector file."""
import copy
import hashlib
import json
from decimal import Decimal
from pathlib import Path
import re
import unicodedata

ROOT = Path(__file__).resolve().parents[1]
DOMAIN = b"tickwire.instrument-catalog.v1\0"
MAX_BYTES = 2 * 1024 * 1024
MAX_DEPTH = 16
MAX_ASSETS = 4096
MAX_INSTRUMENTS = 16384
U64_MAX = (1 << 64) - 1
I128_MAX = (1 << 127) - 1
BODY_FIELDS = ("schema", "venue", "environment", "market", "catalog_version", "assets", "instruments")
ASSET_FIELDS = ("id", "version", "symbol", "exponent")
ENTRY_FIELDS = ("id", "version", "symbol", "product", "base", "quote", "settlement", "price_tick", "quantity_step", "contract")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def fields(value, keys):
    require(type(value) is dict and set(value) == set(keys), "exact object fields")


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate object key")
        result[key] = value
    return result


def reject_number(_):
    raise ValueError("floating point and nonfinite JSON numbers are forbidden")


def integer(text):
    require(text != "-0", "negative zero is not an integer representation")
    return int(text)


def bounded_depth(raw):
    level, quoted, escaped = 0, False, False
    for byte in raw:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            level += 1
            require(level <= MAX_DEPTH, "maximum nesting")
        elif byte in (93, 125):
            level -= 1
            require(level >= 0, "unbalanced object")


def identity(value):
    require(type(value) is str and re.fullmatch(r"[1-9][0-9]{0,19}", value), "identity spelling")
    number = int(value)
    require(number <= U64_MAX, "identity range")
    return number


def symbol(value, maximum):
    require(type(value) is str and 0 < len(value.encode("utf-8")) <= maximum, "symbol size")
    require(all(not c.isspace() and unicodedata.category(c) != "Cc" for c in value), "symbol text")


def decimal(value):
    require(type(value) is str and 0 < len(value) <= 79, "decimal text")
    require(re.fullmatch(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value), "decimal syntax")
    fraction = value.split(".")[1] if "." in value else ""
    require(len(fraction) <= 38, "decimal scale")
    require(int(value.replace(".", "")) <= I128_MAX, "decimal coefficient")
    number = Decimal(value)
    require(number > 0, "positive decimal")
    plain = format(number, "f")
    if "." in plain:
        plain = plain.rstrip("0").rstrip(".")
    require(plain == value, "canonical decimal")


def ordered(value, keys):
    return {key: value[key] for key in keys}


def canonicalize(raw):
    require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES, "raw size")
    bounded_depth(raw)
    body = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_pairs,
                      parse_int=integer, parse_float=reject_number, parse_constant=reject_number)
    fields(body, BODY_FIELDS)
    require(type(body["schema"]) is int and body["schema"] == 1, "schema")
    require(type(body["venue"]) is int and 0 < body["venue"] <= (1 << 32) - 1, "venue")
    require(body["environment"] in ("Production", "Test"), "environment")
    require(body["market"] in ("Spot", "Usdm"), "market")
    identity(body["catalog_version"])
    require(type(body["assets"]) is list and 0 < len(body["assets"]) <= MAX_ASSETS, "asset count")
    require(type(body["instruments"]) is list and 0 < len(body["instruments"]) <= MAX_INSTRUMENTS, "instrument count")
    assets, asset_symbols = {}, set()
    for asset in body["assets"]:
        fields(asset, ASSET_FIELDS)
        asset_id = identity(asset["id"])
        identity(asset["version"])
        symbol(asset["symbol"], 32)
        require(type(asset["exponent"]) is int and -38 <= asset["exponent"] <= 38, "asset exponent")
        require(asset_id not in assets and asset["symbol"] not in asset_symbols, "unique asset")
        assets[asset_id] = ordered(asset, ASSET_FIELDS)
        asset_symbols.add(asset["symbol"])
    instruments, instrument_symbols = {}, set()
    for entry in body["instruments"]:
        fields(entry, ENTRY_FIELDS)
        instrument_id = identity(entry["id"])
        identity(entry["version"])
        symbol(entry["symbol"], 64)
        require(instrument_id not in instruments and entry["symbol"] not in instrument_symbols, "unique instrument")
        for key in ("base", "quote", "settlement"):
            require(identity(entry[key]) in assets, "known asset")
        require(entry["base"] != entry["quote"], "distinct pair")
        decimal(entry["price_tick"])
        decimal(entry["quantity_step"])
        contract = entry["contract"]
        require(type(contract) is dict and "kind" in contract, "contract")
        kind = contract["kind"]
        if kind == "Delivery":
            fields(contract, ("kind", "expiry_utc_ns"))
            identity(contract["expiry_utc_ns"])
            contract = ordered(contract, ("kind", "expiry_utc_ns"))
        else:
            fields(contract, ("kind",))
        require((body["market"], entry["product"], kind) in (
            ("Spot", "Spot", "Cash"),
            ("Usdm", "LinearPerpetual", "Perpetual"),
            ("Usdm", "LinearFuture", "Delivery")), "market/product/contract")
        canonical_entry = ordered(entry, ENTRY_FIELDS)
        canonical_entry["contract"] = contract
        instruments[instrument_id] = canonical_entry
        instrument_symbols.add(entry["symbol"])
    result = ordered(body, BODY_FIELDS)
    result["assets"] = [assets[key] for key in sorted(assets)]
    result["instruments"] = [instruments[key] for key in sorted(instruments)]
    canonical = json.dumps(result, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    require(len(canonical) <= MAX_BYTES, "canonical size")
    return canonical


def reject(raw):
    try:
        canonicalize(raw)
    except (ValueError, UnicodeError, TypeError):
        return
    raise AssertionError("accepted invalid catalog")


def verify():
    path = ROOT / "specs/contracts/instrument-catalog-v1.json"
    contract = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_pairs,
                          parse_int=integer, parse_float=reject_number, parse_constant=reject_number)
    fields(contract, ("schema", "domain_hex", "limits", "vectors", "rejections"))
    require(contract["schema"] == 1, "vector schema")
    require(bytes.fromhex(contract["domain_hex"]) == DOMAIN, "domain bytes")
    require(contract["limits"] == {"max_bytes":MAX_BYTES,"max_depth":MAX_DEPTH,"max_assets":MAX_ASSETS,"max_instruments":MAX_INSTRUMENTS}, "limits")
    for case in contract["vectors"]:
        fields(case, ("name", "raw", "artifact_sha256", "canonical", "catalog_sha256"))
        raw = case["raw"].encode("utf-8")
        canonical = canonicalize(raw)
        require(hashlib.sha256(raw).hexdigest() == case["artifact_sha256"], "artifact SHA")
        require(canonical == case["canonical"].encode("utf-8"), "canonical bytes")
        require(hashlib.sha256(DOMAIN + canonical).hexdigest() == case["catalog_sha256"], "semantic SHA")
    for case in contract["rejections"]:
        fields(case, ("name", "raw", "artifact_sha256"))
        raw = case["raw"].encode("utf-8")
        require(hashlib.sha256(raw).hexdigest() == case["artifact_sha256"], "rehashed invalid input")
        reject(raw)
    raw = contract["vectors"][0]["raw"].encode("utf-8")
    canonicalize(raw + b" " * (MAX_BYTES - len(raw)))
    reject(raw + b" " * (MAX_BYTES + 1 - len(raw)))
    reject(b"[" * (MAX_DEPTH + 1) + b"0" + b"]" * (MAX_DEPTH + 1))
    base = json.loads(raw)
    base["assets"] = [{"id":str(i),"version":"1","symbol":f"A{i}","exponent":-8} for i in range(1, MAX_ASSETS + 1)]
    canonicalize(json.dumps(base).encode())
    base["assets"].append({"id":str(MAX_ASSETS + 1),"version":"1","symbol":"overflow","exponent":-8})
    reject(json.dumps(base).encode())
    base = json.loads(raw)
    base["instruments"] = [copy.deepcopy(base["instruments"][0]) for _ in range(MAX_INSTRUMENTS + 1)]
    reject(json.dumps(base).encode())
    print(json.dumps({"result": "PASS", "vectors": len(contract["vectors"]),
                      "rehashed_rejections": len(contract["rejections"]),
                      "capacity_boundaries": "PASS"}, sort_keys=True))


if __name__ == "__main__":
    verify()
