#!/usr/bin/env bash
# Record `every-type.jsonl` and `every-type.states.json`: a log that writes
# every task event type, and the states `GET /v1/tasks` gives after it.
# Dummy workers only. Needs a fresh daemon:
#   CORK_HOME=/tmp/cork-fx cork serve --port 7431 --db /tmp/cork-fx/board.db &
#   CORK_HOME=/tmp/cork-fx crates/cork-server/ui/test/record-fixture.sh
set -euo pipefail
CORK=${CORK:-cork}
cd "$(dirname "$0")"
id() { "$CORK" --json tasks create "$@" | sed -n 's/.*"id": *"\([^"]*\)".*/\1/p' | head -1; }

a=$(id work/rust "Claim, renew, complete")
b=$(id work/rust "Waits on a" --after "$a")
c=$(id work/rust "Released, then lease ends")
d=$(id work/docs "Blocked, then unblocked")
e=$(id work/docs "Canceled, then reopened")
f=$(id work/rust "Fails")
g=$(id work/rust "Waits on f, which fails" --after "$f")
h=$(id work/rust "Replaced by a retry")
"$CORK" post work/rust "A plain thread, not a task" >/dev/null

"$CORK" --as rust-1 tasks claim "$a" >/dev/null
"$CORK" --as rust-1 tasks renew "$a" >/dev/null
"$CORK" --as rust-1 reply "$a" "usage" --label usage=attempt \
  --data "{\"task\":\"$a\",\"profile\":\"rust-1\",\"pool\":\"rust\",\"model\":\"sonnet\",\"cost_usd\":0.25}" >/dev/null
"$CORK" --as rust-1 tasks complete "$a" >/dev/null       # DependenciesMet on b

"$CORK" --as rust-2 tasks claim "$c" >/dev/null
"$CORK" --as rust-2 tasks release "$c" >/dev/null
"$CORK" --as rust-2 tasks claim "$c" --lease 1s >/dev/null
sleep 2.5                                                 # the lease timer writes LeaseExpired

"$CORK" --as docs-1 tasks claim "$d" >/dev/null
"$CORK" --as docs-1 tasks block "$d" "Which port?" >/dev/null
"$CORK" tasks unblock "$d" "7420" >/dev/null

"$CORK" tasks cancel "$e" >/dev/null
"$CORK" tasks reopen "$e" >/dev/null

"$CORK" --as rust-3 tasks claim "$f" >/dev/null
"$CORK" --as rust-3 tasks fail "$f" "tests failed" >/dev/null   # DependencyFailed on g

i=$(id work/rust "The retry" --replaces "$h")                 # TaskReplaced on h
"$CORK" --as rust-1 tasks claim "$i" --lease 30m >/dev/null

"$CORK" export > every-type.jsonl
"$CORK" --json tasks list | python3 -c '
import json, sys
d = json.load(sys.stdin)
json.dump({t["id"]: t["state"] for t in d["tasks"]}, open("every-type.states.json", "w"), indent=1)'
echo recorded
