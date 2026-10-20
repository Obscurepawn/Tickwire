#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
repo_dir="$PWD"
jar="$repo_dir/tla/tools/tla2tools.jar"
[[ "$(shasum -a 256 "$jar" | cut -d ' ' -f 1)" == 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88 ]]
models="$repo_dir/tla/execution_models/recovery_closure/L0_publish_verify/02_models"
runs="$repo_dir/tla/execution_models/recovery_closure/L0_publish_verify/runs"
for config in L0_RecoveryClosure L0_Conflict L0_MultipleDependencies; do
  scratch="$(mktemp -d)"
  trap 'rm -rf "$scratch"' EXIT
  mkdir -p "$scratch/java" "$scratch/states"
  (cd "$models" && java -Xmx2g -Djava.io.tmpdir="$scratch/java" -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers 1 -coverage 1 -metadir "$scratch/states" -config "$config.cfg" L0_RecoveryClosure.tla) > "$runs/$config.log" 2>&1
  tail -7 "$runs/$config.log"
  rm -rf "$scratch"
  trap - EXIT
done
