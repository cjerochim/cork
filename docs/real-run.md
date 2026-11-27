# A real run with Claude Code workers

This run tested the Milestone 1 build and `skills/cork/SKILL.md` with real
Claude Code workers (`claude -p`). It used a throwaway Python repo, and the
board ran on its own `CORK_HOME`. It was done on 2026-11-27.

## Setup

The lead created three tasks in `work/demo`, each with an idempotency key.
Two of the tasks were plain work, adding `slugify` and `truncate` with tests.
The third, adding `greet(name)`, left the greeting's language open on purpose
and told the worker to ask rather than guess. Creating a task again with
`demo-1`'s key but a different body was rejected (`idempotency_mismatch`).

Two workers started, each in its own shell:

```bash
cork --as worker$N tasks run --ready work/demo --loop --wait 20s -- \
  claude -p 'You were started by `cork tasks run`. Do the Cork task described in $CORK_TASK_TEXT, following the Cork skill in your system prompt.' \
  --append-system-prompt "$(cat skills/cork/SKILL.md)" \
  --allowedTools "Bash,Edit,Write,Read,Glob,Grep"
```

## What happened

| Time (UTC) | Event |
| --- | --- |
| 02:10:45 | `worker1` claims `slugify` and `worker2` claims `truncate`. The two claims are 13 ms apart, and each worker gets a different task. |
| 02:11:04–05 | Both post `cork progress` with `{"branch", "commit"}` data. |
| 02:11:11–12 | Both complete. Each result is the worker's `$CORK_RESULT` summary, naming its branch, commit and worktree. |
| 02:11:11 | `worker2` loops and claims `greet`. |
| 02:11:21 | `worker2` runs `cork tasks block` with a precise question. The wrapper sees the block came from its own claim, settles nothing, and loops. |
| ~02:11:40 | Both workers' `--wait 20s` ends with nothing ready, so they exit 4. |
| 02:12:01 | Chris (the person) answers with `cork reply`, then `cork tasks unblock`. |
| 02:12:01 | `worker3` claims `greet` as attempt 2. |
| 02:12:25 | `worker3` completes. Its summary says it used Chris's answer from attempt 1, which it read in `CORK_PREVIOUS`. |

Each worker followed the skill without being told anything else:

- It worked in its own worktree on `cork/<task>/<attempt>`. Attempt 2's
  branch ends in `/2`.
- It committed, and did not push.
- It posted progress with branch and commit data.
- It wrote its result to `$CORK_RESULT`.
- When the task was unclear, it asked instead of guessing.

The whole log is 19 events. `tasks history` shows every claim, the block, the
unblock and each settle, with its claim id.

## What the run changed

- `cork wait --label board.task=completed` now matches the task's own events.
  Before, `wait` matched only posted messages, so the lead's wait in the skill
  would never have returned.
- `cork read <channel>` now names a reply's thread. It used to indent replies
  under whichever root came last.
- `cork tasks show` shows a question only while the task is blocked.
