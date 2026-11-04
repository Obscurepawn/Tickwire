"""Run a local SHM/policy witness using the production Sources and an authenticated JOIN."""
import os
from pathlib import Path
import subprocess
import tempfile

here = Path(__file__).resolve().parent
root = next(p for p in here.parents if (p / "crates/tickwire-spot-runner").is_dir())
runner = root / "crates/tickwire-spot-runner"
fixture_source = runner / "tests/reconcile_cli/market.rs"
fixture = fixture_source.read_text().split("#[test]", 1)[0]
fixture = fixture.replace(
    '../../../tickwire-md/examples/topic-101.json',
    str(root / "crates/tickwire-md/examples/topic-101.json"),
)
before = "                let trigger = publish(&mut topic, now);"
after = """                if root.join("unavailable").exists() && !root.join("sent-gap").exists() {
                    topic.spot_quote_unavailable_at(i.instrument_id, 1, now, now).unwrap();
                    fs::write(root.join("sent-gap"), b"").unwrap();
                    next = Instant::now() + Duration::from_millis(25);
                    continue;
                }
                let trigger = publish(&mut topic, now);
                if root.join("sent-gap").exists() {
                    fs::write(root.join("published-after-gap"), b"").unwrap();
                }"""
assert fixture.count(before) == 1, "fixture changed; recheck the publisher injection"
fixture = fixture.replace(before, after)
deps = [
    "account-codec", "app", "binance-clock", "book", "clock", "join", "market-publisher",
    "market-source", "md", "runtime", "runtime-policy", "shm", "snapshot", "venue",
]
manifest = ['[package]', 'name = "quote-proof"', 'version = "0.1.0"', 'edition = "2024"',
            '[workspace]', '[dependencies]', 'serde_json = "1"']
for name in deps:
    path = root / f"crates/tickwire-{name}"
    manifest.append(f'tickwire-{name} = {{ path = "{path}" }}')
env = os.environ.copy()
env["TW_PROBE_BASELINE"] = str(runner / "examples/baseline-template.cbor")
env["CARGO_TARGET_DIR"] = str(root / "target")
with tempfile.TemporaryDirectory(prefix="quote-proof-") as scratch:
    tmp = Path(scratch)
    (tmp / "Cargo.toml").write_text("\n".join(manifest) + "\n")
    (tmp / "Cargo.lock").write_bytes((root / "Cargo.lock").read_bytes())
    (tmp / "src").mkdir()
    (tmp / "src/lib.rs").write_text("#[cfg(test)] mod probe;")
    (tmp / "src/probe.rs").write_bytes((here / "native_probe.rs").read_bytes())
    (tmp / "src/fixture.rs").write_text(fixture)
    subprocess.run(["cargo", "test", "--offline", "--manifest-path", str(tmp / "Cargo.toml"),
                    "--lib", "--", "--nocapture"], cwd=root, env=env, check=True)
