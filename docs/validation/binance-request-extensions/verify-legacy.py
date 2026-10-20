#!/usr/bin/env python3
"""Run the pre-extension schema-1 encoder/decoder from a fixed repository commit."""
from pathlib import Path
import os
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
BASE = "1d2743d7315456816d328354140ec2105754175f"
EVIDENCE = Path(__file__).resolve().parent


def old(path):
    return subprocess.check_output(["git", "show", f"{BASE}:{path}"], cwd=ROOT).decode()


with tempfile.TemporaryDirectory(prefix="tickwire-old-venue-") as temp:
    project = Path(temp)
    src = project / "src"
    src.mkdir()
    for name in ("lib.rs", "types.rs", "codec.rs", "validate.rs", "cbor.rs"):
        (src / name).write_text(old(f"crates/tickwire-venue/src/{name}"))
    (src / "common.rs").write_text(old("crates/tickwire-venue/tests/common/mod.rs"))
    (project / "Cargo.toml").write_text(
        '[package]\nname="tickwire-old-venue-decoder"\nversion="0.0.0"\n'
        'edition="2024"\n[dependencies]\nsha2="=0.10.9"\n'
    )
    fixtures = old("crates/tickwire-venue/tests/commands.rs")
    fixtures = fixtures[:fixtures.index("#[test]\nfn spot_conditional")]
    fixtures = fixtures.replace("use tickwire_venue::*;", "use tickwire_old_venue_decoder::*;")
    fixtures = fixtures.replace("#[test]\nfn all_ten_command_branches_preserve_complete_content()", "fn main()")
    start, end = fixtures.index("fn roundtrip("), fixtures.index("fn derivative()")
    fixtures = fixtures[:start] + '''fn roundtrip(c:Command) {
        let p=CanonicalCommand::new(c).unwrap();
        let hex=|b:&[u8]|b.iter().map(|v|format!("{v:02x}")).collect::<String>();
        println!("{} {}",hex(&p.digest()),hex(p.bytes()));
    }
    ''' + fixtures[end:]
    (src / "common.rs").write_text((src / "common.rs").read_text().replace("tickwire_venue", "tickwire_old_venue_decoder"))
    (src / "main.rs").write_text(fixtures)
    env = dict(os.environ, RUSTUP_TOOLCHAIN="1.99.1", CARGO_TARGET_DIR=str(ROOT / "target/legacy-capture"))
    command = ["cargo", "run", "--offline", "--quiet", "--manifest-path", str(project / "Cargo.toml")]
    encoded = subprocess.check_output(command, env=env)
    assert encoded == (EVIDENCE / "legacy-commands.txt").read_bytes()
    print(f"PASS: fixed commit {BASE}, all 10 old command bytes and digests")
    (src / "main.rs").write_text('''use tickwire_old_venue_decoder::{CanonicalCommand,Error};
fn main(){let text=std::fs::read_to_string(std::env::args().nth(1).unwrap()).unwrap();
for(n,line)in text.lines().enumerate(){let b:Vec<_>=(0..line.len()).step_by(2).map(|i|u8::from_str_radix(&line[i..i+2],16).unwrap()).collect();
let e=CanonicalCommand::decode(&b).unwrap_err();assert_eq!(e,Error::Enum);println!("PASS: tag11 subcommand {} -> {:?}",n+1,e);}}
''')
    subprocess.run(command + ["--", str(EVIDENCE / "tag11-commands.txt")], env=env, check=True)
