#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --all -- --check
TICKWIRE_BENCH_SAMPLES=1000 cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
