#!/usr/bin/env bash
# Fill a board with a small, realistic mix of tasks for trying `cork inspect`.
# Every worker is a dummy shell command: no agent runs and nothing costs money.
# The costs shown are made up and written by the dummy commands themselves.
#
#   CORK_HOME=/tmp/cork-demo cork serve --port 7421 &
#   CORK_HOME=/tmp/cork-demo scripts/demo-board.sh
#   CORK_HOME=/tmp/cork-demo cork inspect
set -euo pipefail
CORK=${CORK:-cork}
id() { "$CORK" --json tasks create "$@" | sed -n 's/.*"id": *"\([^"]*\)".*/\1/p' | head -1; }

# A dummy worker: claims one task, writes a usage record, exits with $2.
work() { # <profile> <exit> <pool> <model> <cost> <task>
  CORK_POOL=$3 CORK_MODEL=$4 "$CORK" --as "$1" tasks run "$6" -- \
    sh -c "printf '{\"cost_usd\":$5,\"input_tokens\":12000,\"output_tokens\":900,\"turns\":4}' > \"\$CORK_USAGE\"; exit $2" \
    >/dev/null 2>&1 || true
}

a=$(id work/rust "Trim export memory")
b=$(id work/rust "Stream export in pages")
c=$(id work/docs "Document the stats route")
d=$(id work/rust "Fix lease renew race")
e=$(id work/rust "Add channel wildcards to tail")
f=$(id work/docs "Write the board view guide" --after "$c")
g=$(id work/rust "Release 0.3.0 notes" --after "$a,$b")
h=$(id work/rust "Check export on 1,000 tasks" --after "$b")
i=$(id lead "Pick the default lease")
j=$(id work/docs "Tidy the README badges")
k=$(id work/rust "Benchmark the fold")
id work/rust "Try a SQLite page cache" >/dev/null
id work/docs "Spell-check HANDOFF" >/dev/null

work rust-1 0 rust sonnet 0.42 "$a"
work docs-1 0 docs haiku 0.08 "$c"
work rust-2 1 rust sonnet 0.31 "$d"   # fails once, back to ready
work docs-2 0 docs haiku 0.05 "$j"
"$CORK" tasks cancel "$k" >/dev/null

# Held right now: a long lease, so the cards sit in "In progress".
"$CORK" --as rust-2 tasks claim "$b" --lease 30m >/dev/null
"$CORK" --as rust-3 tasks claim "$e" --lease 10m >/dev/null
"$CORK" --as lead-1 tasks claim "$i" >/dev/null
"$CORK" --as lead-1 tasks block "$i" "Should the default lease be 10 or 15 minutes?" >/dev/null
"$CORK" --as docs-1 tasks claim "$f" --lease 20m >/dev/null
echo "demo board ready: $CORK inspect"
