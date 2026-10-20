#!/usr/bin/env bash
# Reproduce the submitted finite models using the pinned upstream TLC jar.
set -euo pipefail
cd "$(dirname "$0")/.."
repo_dir="$PWD"
jar="$repo_dir/tla/tools/tla2tools.jar"
expected=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
if [[ ! -f "$jar" ]]; then
  mkdir -p "$(dirname "$jar")"
  curl --proto '=https' --tlsv1.2 -fL --retry 3 https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar -o "$jar"
fi
actual="$(shasum -a 256 "$jar" | cut -d ' ' -f 1)"
[[ "$actual" == "$expected" ]] || { echo 'TLC jar checksum mismatch' >&2; exit 1; }
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
while IFS= read -r dir; do
  # Every directory has one model and its primary/scaling configurations.
  model="$(find "$dir" -maxdepth 1 -name '*.tla' ! -name '*_TTrace_*' -print)"
  [[ -n "$model" && "$model" != *$'\n'* ]] || { echo "Ambiguous model directory: $dir" >&2; exit 1; }
  group="$(mktemp -d "$scratch/model.XXXXXX")"
  # Every cfg runs the same frozen source even if the working tree is edited.
  cp "$model" "$group/$(basename "$model")"
  while IFS= read -r cfg; do
    cp "$cfg" "$group/$(basename "$cfg")"
  done < <(find "$dir" -maxdepth 1 -name '*.cfg' -print | sort)
  logs=()
  while IFS= read -r cfg; do
    run="$(mktemp -d "$group/run.XXXXXX")"
    mkdir -p "$run/states" "$run/java"
    (cd "$group" && java -Djava.io.tmpdir="$run/java" -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers 1 -coverage 1 -metadir "$run/states" -config "$(basename "$cfg")" "$(basename "$model")") | tee "$run/tlc.log"
    logs+=("$run/tlc.log")
    rm -rf "$run/states" "$run/java"
  done < <(find "$group" -maxdepth 1 -name '*.cfg' -print | sort)
  # Every cfg must finish successfully. Only final coverage blocks count;
  # boundary configurations may exercise complementary action entries.
  python3 scripts/tlc_coverage.py --model "$(basename "$model" .tla)" --model-file "$group/$(basename "$model")" "${logs[@]}"
  rm -rf "$group"
done < <(find tla/execution_models -path '*/02_models/*.cfg' -exec dirname {} \; | sort -u)
