#!/usr/bin/env bash
# Real Linux kernel tests, including memfd seals and independent consumer processes.
set -euo pipefail
cd "$(dirname "$0")/.."
repo_dir="$PWD"
mkdir -p "$repo_dir/.tools/linux-cargo-registry"
image='rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29'
docker run --rm --init \
  --mount "type=bind,source=$repo_dir,target=/work" \
  --mount "type=bind,source=$repo_dir/.tools/linux-cargo-registry,target=/usr/local/cargo/registry" \
  -w /work -e CARGO_TARGET_DIR=/work/target/linux-check \
  -e RUSTUP_TOOLCHAIN=1.99.1 \
  "$image" bash -c '
    set -euo pipefail
    uname -srmo
    getconf PAGESIZE
    rustc --version
    rustup component add rustfmt clippy --toolchain 1.99.1
    cargo fmt --all -- --check
    TICKWIRE_BENCH_SAMPLES=1000 cargo test --locked --workspace --all-targets
    cargo test --locked --workspace --doc
    cargo clippy --locked --workspace --all-targets -- -D warnings
  '
