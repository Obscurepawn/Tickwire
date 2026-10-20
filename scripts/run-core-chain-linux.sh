#!/usr/bin/env bash
# Run the independent Linux processes on a dedicated Linux filesystem.
set -euo pipefail
cd "$(dirname "$0")/.."
repo_dir="$PWD"
scenario="${1:-spot-fill}"
if [[ $# -gt 1 ]] || [[ "$scenario" != spot-fill && "$scenario" != spot-handover ]]; then
  printf 'Usage: %s [spot-fill|spot-handover]\n' "$0" >&2
  exit 2
fi
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
container_name="tickwire-lab-$run_id"
volume_name="tickwire-lab-$run_id"
result_dir="$repo_dir/.tools/lab-results/$run_id"
image='rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29'

mkdir -p "$repo_dir/.tools/linux-cargo-registry" "$repo_dir/target/linux-check" "$result_dir"
docker volume create --label tickwire.task=core-chain "$volume_name" >/dev/null
cleanup() {
  docker rm -f "$container_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

printf '%s\n' "$image" > "$result_dir/image.txt"
printf '%s\n' "$scenario" > "$result_dir/scenario.txt"
printf 'Results: %s\nRetained Linux volume: %s\n' "$result_dir" "$volume_name"
docker run --rm --init --name "$container_name" --label tickwire.task=core-chain \
  --mount "type=bind,source=$repo_dir,target=/work,readonly" \
  --mount "type=bind,source=$repo_dir/target/linux-check,target=/target" \
  --mount "type=bind,source=$repo_dir/.tools/linux-cargo-registry,target=/usr/local/cargo/registry" \
  --mount "type=bind,source=$result_dir,target=/results" \
  --mount "type=volume,source=$volume_name,target=/lab" \
  -w /work -e "TICKWIRE_LAB_SCENARIO=$scenario" -e CARGO_TARGET_DIR=/target -e RUSTUP_TOOLCHAIN=1.99.1 \
  "$image" bash -c '
    set -euo pipefail
    save_logs() {
      if test -d /lab/run/logs; then cp -a /lab/run/logs /results/; fi
      for name in run-result.json account-report.json fixture.jsonl; do
        if test -f "/lab/run/$name"; then cp "/lab/run/$name" /results/; fi
      done
    }
    trap save_logs EXIT
    {
      sha256sum Cargo.toml Cargo.lock
      find crates specs/contracts -type f -print0 | sort -z | xargs -0 sha256sum
    } > /results/workspace-source-sha256.txt
    uname -srmo
    rustc --version
    stat -f -c %T /lab
    cargo build --locked -p tickwire-lab -p tickwire-app -p tickwire-md \
      -p tickwire-strategy-sdk -p tickwire-replica -p tickwire-lifecycle --bins
    mkdir /lab/bin
    for name in tickwire-lab tickwire-app tickwire-md tickwire-strategy-worker tickwire-replica-receiver tickwire-lifecycle; do
      install -m 0755 "/target/debug/$name" "/lab/bin/$name"
      strip --strip-debug "/lab/bin/$name"
    done
    sha256sum /lab/bin/* > /results/deployment-binaries-sha256.txt
    /lab/bin/tickwire-lab generate --scenario "$TICKWIRE_LAB_SCENARIO" --out /lab/run --bin-dir /lab/bin
    /lab/bin/tickwire-lab run --config /lab/run/scenario.json
  ' 2>&1 | tee "$result_dir/console.log"
