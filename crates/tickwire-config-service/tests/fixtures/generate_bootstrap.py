"""Independent wire and Ed25519 fixture; requires OpenSSL 3 (not Rust code)."""
import hashlib
from pathlib import Path
import struct
import subprocess
import tempfile

OPENSSL = "/opt/homebrew/opt/openssl@3/bin/openssl"
ROOT = Path(__file__).parent
cluster = bytes([19]) * 16
namespace = bytes.fromhex("00000000000040008000000000000001")
name = b"tickwire-qualification"
object_key = bytes([6]) * 16
nonce = bytes([10]) * 16
candidate_hash = hashlib.sha256(b"candidate").digest()
with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    key_file = root / "test-key.der"
    # RFC 8410 PKCS#8, deliberately fixed test seed.
    key_file.write_bytes(bytes.fromhex("302e020100300506032b657004220420") + bytes([31]) * 32)
    public_der = subprocess.check_output([
        OPENSSL, "pkey", "-in", str(key_file), "-inform", "DER", "-pubout", "-outform", "DER"
    ])
    assert public_der[:12] == bytes.fromhex("302a300506032b6570032100")
    public_key = public_der[12:]
    trust = hashlib.sha256(
        b"tickwire.control.bootstrap-trust.v1" + cluster + namespace
        + struct.pack("<I", len(name)) + name + public_key
    ).digest()
    message = b"tickwire.control.bootstrap.v1" + trust + object_key + candidate_hash + nonce
    (root / "message").write_bytes(message)
    signature = subprocess.check_output([
        OPENSSL, "pkeyutl", "-sign", "-rawin", "-inkey", str(key_file),
        "-keyform", "DER", "-in", str(root / "message"),
    ])
    assert len(signature) == 64
    init = b"TWCF0001" + cluster + b"\x05" + struct.pack("<IIQ", 16, 4, 32*1024*1024) + trust
    command = b"TWCF0001" + cluster + b"\x06" + object_key + candidate_hash + nonce + signature
    for filename, value in [
        ("initialize-trusted.hex", init), ("bootstrap.hex", command),
        ("bootstrap-signing.hex", message), ("bootstrap-public.hex", public_key),
    ]:
        (ROOT / filename).write_text(value.hex() + "\n")
