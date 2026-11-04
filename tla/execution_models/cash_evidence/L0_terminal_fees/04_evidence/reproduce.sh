#!/usr/bin/env bash
# Linux/glibc. Reuse Tickwire's signed loopback TLS process fixture at 9b2ed63.
set -eu
evidence=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$evidence" rev-parse --show-toplevel)
cd "$repo"
test "$(git rev-parse HEAD:crates/tickwire-spot-runner/tests/reconcile_cli/exchange.py)" = "$(git rev-parse 9b2ed63:crates/tickwire-spot-runner/tests/reconcile_cli/exchange.py)"
run=$(mktemp -d)
trap 'rm -rf "$run"' EXIT
testbin=$(cargo test --locked --offline -p tickwire-spot-runner --test reconcile_process --no-run --message-format=json | python3 -c 'import json,sys; print(next(j["executable"] for line in sys.stdin if (j:=json.loads(line)).get("executable") and j["target"]["name"]=="reconcile_process"))')
ln -s "$evidence/native_peer.py" "$run/python3"
CASH_REPLAY_TRANSCRIPT="$run/result" PATH="$run:$PATH" "$testbin" real_reconcile_process_qualifies_and_reopens_both_native_accounts --nocapture
cat "$run/result"
