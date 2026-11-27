# Worker fleets and keywords

This is the user guide for HANDOFF sections 16 and 17. The spec says why; this page says how.

Everything here is client side. The board stores and filters plain labels, as in Milestone 1, and knows nothing about models. Every fleet command works against an unmodified Milestone 1 daemon.

## The model catalog

`models.toml` lists the keyword vocabulary and what each model can do. A file ending in `.json` with the same shape is read too.

```toml
[keywords]
rust = "Writes and reviews Rust"
docs = "Prose: READMEs, guides, changelogs"

[aliases]
documentation = "docs"

[model.sonnet]
command = ["claude", "-p", "Do the Cork task in $CORK_TASK_TEXT", "--model", "sonnet"]
tier = 2        # 1 is the weakest; a model also covers every tier below its own
cost = 3        # relative, for choosing the cheapest model and for reports
keywords = ["rust", "docs"]
```

Cork looks for the catalog in this order:

1. `--models FILE`, or `CORK_MODELS`.
2. The catalog named by the fleet config: `--config FILE`, `CORK_WORKERS`, or `~/.config/cork/workers.toml`.
3. `~/.config/cork/models.toml` (or `.json`).

Rules the catalog enforces:

- Keywords are lowercase `[a-z0-9_-]`. Aliases must point at a keyword.
- `tier-N` keywords are made from each model's `tier`. They cannot be declared, and a model cannot list them.
- A model that names an unknown keyword is refused, with the closest match.
- Unknown fields are refused, so a typo such as `keyword =` is caught.

## Asking for keywords

```bash
cork tasks create work "Port the parser to Rust" --needs rust,large-context
cork tasks create work "Redesign storage" --needs rust,architecture,tier-3
```

`--needs a,b` adds the labels `needs.a=yes` and `needs.b=yes`. Each word is checked against the catalog first:

- An unknown word is refused (exit 2), naming the closest match: `unknown keyword 'typscript'; did you mean 'typescript'?`
- An alias is stored as its keyword: `--needs ts` becomes `needs.typescript=yes`.

A task with no `needs.*` labels can go to any pool. Labels written another way (raw `--label`, the API) are not checked; `workers match` and `workers status` report them as unmatched.

## Who can take a task

```bash
cork workers keywords                 # the vocabulary, aliases, and which models cover each keyword
cork workers match --needs rust       # before the task exists
cork workers match <task-id>          # an existing task, against this machine's fleet
```

```text
$ cork workers match --needs rust
haiku: missing rust
sonnet: covers
opus: covers (costs 5x sonnet)
reviewer: missing rust
```

With a fleet config, `match` also judges each pool and names the one the fleet would give the task to. That answer comes from the same function the dispatcher uses. A pool covers a task when:

- its `select` matches the task's channel,
- the task's owner is the fleet's person, or the pool's `from` (`*` for anyone),
- the task has no `--for` assignee (those are for a named agent, not a fleet),
- every `needs.*` keyword is in the pool's list or tiers, and none is unknown,
- and, for an `exclusive = true` pool, the task asks for the pool's top tier.

Among the pools that cover a task, the cheapest wins: by `cost` (a model without a cost comes after any with one), then tier, then pool name. `match` exits 3 when nothing covers the task.

## Watching a feature's children

`cork wait` takes `--type` to wake on task events instead of messages:

```bash
cork wait work/ --label parent=$F --type TaskCompleted --type TaskFailed --type TaskBlocked --type LeaseExpired
```

## Running a fleet

`workers.toml` says which pools run on this machine. It lives at `~/.config/cork/workers.toml`, or comes from `--config FILE` or `CORK_WORKERS`. A `.json` file in the same shape works too.

```toml
[fleet]
name = "dev"               # in profile names and the control socket
models = "models.toml"     # relative to this file
max_workers = 12           # cap across all pools (default 16)

[pool.fast]
model = "haiku"
select = "work/"           # channel selector
count = 5                  # initial size
min = 0                    # bounds for scale and autoscale (max defaults to max_workers)
max = 10
lease = "10m"
max_runtime = "2h"
# command = [...]          # override the model's; {model} and {pool} are filled in
# keywords = ["docs"]      # narrow what this pool takes
# env = { KEY = "v" }      # added to the command's environment
# cwd = "../repo"          # relative to this file
# from = "*"               # whose tasks: default the fleet's person, "*" for anyone
# exclusive = true         # only tasks that ask for this model's top tier

[pool.fast.autoscale]      # optional
target_ready_per_worker = 2
idle_after = "5m"
```

```bash
cork workers up [--config FILE] [--detach]   # foreground by default; --detach logs to ~/.config/cork/workers/<fleet>.log
cork workers status [--json]                 # pools, workers and their tasks, restarts, pauses, unmatched tasks
cork workers scale fast +3                   # or -2, or 4; within min and max and max_workers
cork workers drain fast                      # finish current tasks, then stop the pool's workers
cork workers reload                          # re-read both files
cork workers down [--now]                    # drain and stop; --now releases every task and stops
```

The control commands find the fleet by `--fleet NAME`, else by the config's name, else by the only fleet running. They talk to `~/.config/cork/workers/<fleet>.sock` (mode `0600`), not to the board.

### How dispatch works

- The supervisor follows `/events` for its pools' channels, and also looks every 2 s.
- Each round lists ready tasks oldest first. Each task goes to a free worker of the cheapest pool that covers it (see "Who can take a task").
- A worker is one child: `cork --as <fleet>-<pool>-<n> tasks run <id> -- <command>`. The command gets `CORK_MODEL`, `CORK_POOL`, `CORK_TIER` and `CORK_FLEET`, on top of what `tasks run` sets.
- `tasks history` shows which pool, and so which model, did each attempt.

The supervisor handles each child's exit by its code:

| Exit | What the supervisor does |
| --- | --- |
| 0, 8, 9, 10, 5 | The worker is free again. |
| 7, or 1 for a race (`invalid_state` and the like) | Someone else got the task first. The worker tries the next one. |
| 11 | The daemon was lost. Dispatch pauses with backoff, from 1 s doubling to 30 s. |
| A signal, another error, or exit 8 less than 2 s after the start | A crash. The worker is replaced after a backoff from 0.5 s doubling to 30 s, and the task is left alone for 5 s. |

- **Crash loop:** 5 crashes in 60 s pause the pool. `status` shows the last error, and one message goes to `cork/fleet`. `scale` or `reload` resumes the pool.
- **Scaling down:** idle workers stop at once. Busy workers are drained: they get no new task, so they finish and settle their current one, and nothing is released or failed.
- **Reload:** a change of a pool's model, command, env, cwd, lease or max runtime drains the old workers and starts new ones. Tasks in progress finish on the model that started them. A config with an error is refused, and the running config stays.
- **No orphans (Linux):** children get `SIGTERM` when the supervisor dies, so each wrapper releases its task and exits. The command under `tasks run` also gets `SIGTERM` if its wrapper dies. A first `SIGTERM` or Ctrl-C drains; a second stops every child now.
- **Two fleets, one board:** fleets on one or many machines can share a board. The single writer gives each task one winner; the loser exits 7 and moves on.

## Escalating a struggling task

```bash
cork tasks escalate <id> [--needs large-context] [--max 2]
```

This makes a **retry** one tier up. Labels are fixed when a task is created, so the old task is never changed. The retry:

- keeps the old task's channel, lease, max attempts and other labels, such as `parent`;
- needs the old keywords, plus any given with `--needs`, plus `tier-<N+1>`. N is the highest of the tier the task asked for, the tier of the model that last claimed it (when this machine's fleet knows the profile), and the tier of the cheapest model that covers it;
- carries `retry_of=<old>` and `escalation=<n>`;
- has a text that names the earlier branches (`cork/<old>/<attempt>`), the last failure, and the claimers' progress notes, followed by the original task text.

Then `tasks escalate`:

- cancels the old task if it is ready or blocked;
- replies in the old thread with a link to the retry.

Every write uses an idempotency key made from the old id, so running it again after a crash returns the same retry. It exits 3, creating nothing, when:

- there is no higher tier,
- no model covers the new needs, or
- the work was already escalated `--max` times (default 2).

## Leading a feature

A lead is one more pool, on the strongest model, selecting only `features/`. Worker pools never select `features/`, so only the lead takes features:

```toml
[pool.lead]
model = "opus"
select = "features/"
count = 1
lease = "30m"
command = ["claude", "-p", "Lead the Cork feature in $CORK_TASK_TEXT. Follow skills/cork-lead/SKILL.md.", "--model", "{model}"]
```

```bash
cork tasks create features "<brief>" --label kind=feature
```

The lead links children to the feature with plain labels (`parent=<feature id>`), and creates them under keys `<feature>-<n>`, so a restarted lead duplicates nothing. See [skills/cork-lead/SKILL.md](../skills/cork-lead/SKILL.md), and [lead-experiment.md](lead-experiment.md) for how it will be measured.
