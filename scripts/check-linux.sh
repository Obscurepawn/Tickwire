#!/usr/bin/env bash
# Real Linux kernel verification: memfd seals, AF_UNIX SCM_RIGHTS, /proc/<pid>/task/<tid>/status
# housekeeping_cpus taskset-escape & CPU-isolation checks, and full 42-crate workspace gates.
set -euo pipefail
cd "$(dirname "$0")/.."
repo_dir="$PWD"
if [[ -d "$HOME/.cargo/bin" ]]; then
  export PATH="$HOME/.cargo/bin:$PATH"
fi

mode="auto"
if [[ $# -gt 1 ]]; then
  printf 'Usage: %s [--native|--docker]\n' "$0" >&2
  exit 2
elif [[ $# -eq 1 ]]; then
  case "$1" in
    --native) mode="native" ;;
    --docker) mode="docker" ;;
    *)
      printf 'Usage: %s [--native|--docker]\n' "$0" >&2
      exit 2
      ;;
  esac
fi

run_linux_checks() {
  uname -srmo
  getconf PAGESIZE
  rustc --version

  cargo fmt --all -- --check
  TICKWIRE_BENCH_SAMPLES=1000 cargo test --locked --workspace --all-targets
  cargo test --locked --workspace --doc
  cargo clippy --locked --workspace --all-targets -- -D warnings

  local runner=()
  if command -v taskset >/dev/null 2>&1; then
    runner=(taskset -c 0)
  fi
  "${runner[@]}" cargo test --locked -p tickwire-control-sys --test affinity -- --nocapture
  "${runner[@]}" cargo test --locked -p tickwire-transport --test housekeeping_affinity -- --nocapture
  "${runner[@]}" cargo test --locked -p tickwire-md --test stateless_join_and_affinity -- --nocapture
  "${runner[@]}" cargo test --locked -p tickwire-runtime --test cpu_isolation -- --nocapture
}

use_docker=0
if [[ "$mode" == "docker" ]] || [[ "${TICKWIRE_LINUX_DOCKER:-0}" == "1" ]]; then
  use_docker=1
elif [[ "$mode" == "auto" ]] && [[ "$(uname -s)" != "Linux" ]]; then
  use_docker=1
fi

if [[ "$use_docker" -eq 1 ]]; then
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
      runner=()
      if command -v taskset >/dev/null 2>&1; then
        runner=(taskset -c 0)
      fi
      "${runner[@]}" cargo test --locked -p tickwire-control-sys --test affinity -- --nocapture
      "${runner[@]}" cargo test --locked -p tickwire-transport --test housekeeping_affinity -- --nocapture
      "${runner[@]}" cargo test --locked -p tickwire-md --test stateless_join_and_affinity -- --nocapture
      "${runner[@]}" cargo test --locked -p tickwire-runtime --test cpu_isolation -- --nocapture
    '
else
  run_linux_checks
fi
