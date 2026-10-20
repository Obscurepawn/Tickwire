#!/usr/bin/env python3
"""Instrument the fixed baseline in a temporary crate; never edit production code."""
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
parser = argparse.ArgumentParser()
parser.add_argument("--revision", default="df8bd2d")
parser.add_argument("--samples", type=int, default=100_000)
parser.add_argument("--lookup", choices=["suffix", "full", "hybrid"], default="suffix")
args = parser.parse_args()
cargo = shutil.which("cargo") or str(Path.home() / ".cargo/bin/cargo")
source = subprocess.check_output(
    ["git", "show", f"{args.revision}:crates/tickwire-book/src/lib.rs"], cwd=ROOT
).decode()
print("baseline_source_sha256=" + hashlib.sha256(source.encode()).hexdigest(), flush=True)
start = source.index("fn prepare_side")
end = source.index("\nfn locate(", start)
section = source[start:end]
if args.lookup == "full":
    original = """if cursor < levels.len
            && compare_prices(side, levels.data[cursor].price, update.level.price).is_eq()
        {
            (cursor, true)
        } else {
            match locate(&levels.as_slice()[cursor..], side, update.level.price) {
                Ok(index) => (cursor + index, true),
                Err(index) => (cursor + index, false),
            }
        }"""
    replacement = """match locate(levels.as_slice(), side, update.level.price) {
            Ok(index) => (index, true),
            Err(index) => (index, false),
        }"""
    assert section.count(original) == 1
    section = section.replace(original, replacement)
    section = section.replace("    let mut cursor = 0;\n", "")
    section = section.replace("        cursor = update.index + usize::from(update.exists);\n", "")
elif args.lookup == "hybrid":
    original = """match locate(&levels.as_slice()[cursor..], side, update.level.price) {
                Ok(index) => (cursor + index, true),
                Err(index) => (cursor + index, false),
            }"""
    replacement = """match locate(levels.as_slice(), side, update.level.price) {
                Ok(index) => (index, true),
                Err(index) => (index, false),
            }"""
    assert section.count(original) == 1
    section = section.replace(original, replacement)
section = section.replace(
    ") -> Result<SidePlan, BookError> {",
    ") -> Result<SidePlan, BookError> {\n    let d0 = std::time::Instant::now();",
)
section = section.replace(
    "    updates.sort_unstable_by",
    "    let d1 = std::time::Instant::now();\n    updates.sort_unstable_by",
)
section = section.replace(
    "    let mut removed = 0;",
    "    let d2 = std::time::Instant::now();\n    let mut removed = 0;",
)
section = section.replace(
    "    let new_len = (levels.len - removed)",
    "    let d3 = std::time::Instant::now();\n    let new_len = (levels.len - removed)",
)
section = section.replace(
    "    Ok(SidePlan {",
    """    let d4 = std::time::Instant::now();
    DIAG.with(|totals| {
        let mut totals = totals.borrow_mut();
        for (i, delta) in [d1-d0, d2-d1, d3-d2, d4-d3].iter().enumerate() {
            totals[i] += delta.as_nanos();
        }
        totals[4] += 1;
    });
    Ok(SidePlan {""",
)
source = source[:start] + section + source[end:]
source = source.replace("#![no_std]", "")
source += """
std::thread_local! {
    static DIAG: std::cell::RefCell<[u128; 5]> = const { std::cell::RefCell::new([0; 5]) };
}
pub fn diagnostic_reset() { DIAG.with(|d| *d.borrow_mut() = [0; 5]); }
pub fn diagnostic_totals() -> [u128; 5] { DIAG.with(|d| *d.borrow()) }
"""
harness = (ROOT / "crates/tickwire-book/benches/sparse_apply.rs").read_text()
harness = harness.replace(
    "        let start = Instant::now();",
    "        if i == 10_000 { diagnostic_reset(); }\n        let start = Instant::now();",
)
harness = harness.replace(
    "    summarize(\n        &format!(",
    """    let d = diagnostic_totals();
    println!("stages,{N},{},{},{:.2},{:.2},{:.2},{:.2}",
        updates * 2, if churn { "churn" } else { "replace" },
        d[0] as f64 / count as f64, d[1] as f64 / count as f64,
        d[2] as f64 / count as f64, d[3] as f64 / count as f64);
    summarize(
        &format!(""",
)
print(f"lookup_experiment={args.lookup}", flush=True)
print("stages,depth,updates_total,kind,copy_ns,sort_ns,locate_plan_ns,final_plan_ns", flush=True)
with tempfile.TemporaryDirectory(prefix="tickwire-book-stages-") as temporary:
    path = Path(temporary)
    (path / "src").mkdir()
    (path / "benches").mkdir()
    (path / "src/lib.rs").write_text(source)
    (path / "benches/stages.rs").write_text(harness)
    (path / "Cargo.toml").write_text("""[package]
name = "tickwire-book"
version = "0.1.0"
edition = "2024"
[[bench]]
name = "stages"
harness = false
[profile.release]
overflow-checks = true
lto = "thin"
codegen-units = 1
""")
    subprocess.run(
        [cargo, "bench", "--bench", "stages"],
        cwd=path,
        env={**os.environ, "TICKWIRE_BENCH_SAMPLES": str(args.samples)},
        check=True,
    )
