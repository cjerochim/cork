# Observability: usage, cost and `cork stats`

This is the user guide for HANDOFF section 18 (Milestone 3). The spec says why; this page says how.

Two kinds of data, kept apart:

- **Flow** is what happened to the work: claims, attempts, lease expiries, escalations. It is already in the log, so `cork stats` reads it by replay.
- **Usage** is tokens, cost and turns per attempt. Only the agent knows these. The wrapper posts them as ordinary replies, so the core knows nothing about usage: no new event types, fields or tables.

## Capturing usage

`cork tasks run` sets `$CORK_USAGE` to a file. Before it exits, the command may write a JSON object there. Every field is optional, and unknown fields are kept:

```json
{"model": "claude-sonnet-4-5", "input_tokens": 182340, "output_tokens": 9120,
 "cache_read_tokens": 140000, "cache_write_tokens": 0, "cost_usd": 0.71, "turns": 23}
```

For Claude Code, let the wrapper read the usage from its output instead:

```bash
cork tasks run --ready work/ --usage claude-json -- \
  claude -p "Do the Cork task in \$CORK_TASK_TEXT" --output-format json
```

The wrapper parses the final JSON object Claude Code prints: total cost, tokens summed over every model, turns, the model that did most of the work, and a per-model breakdown. The `result` text becomes the task's result, so results stay readable. If the command also writes `$CORK_RESULT` or `$CORK_USAGE`, those files win.

Just before it settles, the wrapper posts **one reply per attempt** in the task's thread, labelled `usage=attempt`. Its `data` holds the agent's fields plus:

| Field | What it holds |
| --- | --- |
| `task`, `attempt`, `claim_id` | Which attempt this was. |
| `profile`, `pool` | Who ran it. `pool` comes from `$CORK_POOL`, which the fleet supervisor sets. |
| `model` | The catalog name from `$CORK_MODEL` under a fleet; otherwise the agent's own. The agent's name moves to `agent_model` when they differ. |
| `exit` | The command's exit status, or `null` if it was stopped. |
| `outcome` | `completed`, `failed`, `blocked`, `max_runtime`, `released` or `lost`. |
| `duration_ms` | How long the command ran. |
| `source` | `file`, or the parser's name. |
| `cost_source` | `agent` when the agent reported the cost. |

Rules:

- A usage file that is missing, empty or broken, or a parser that finds nothing, never changes how the task settles.
- Without a usage file and without `--usage`, nothing is posted.
- The reply text holds numbers and names only, never prompts, code or output.
- A record over 32 KiB keeps only the known fields.
- When the wrapper itself is killed (`kill -9`, a dead machine), that attempt's usage is lost. `cork stats` counts it as unknown. It does not guess.

## Prices

When an agent reports tokens but no cost, `cork stats` estimates the cost from a price table in `models.toml`, in dollars per million tokens:

```toml
[model.sonnet.price]
input_per_mtok = 3.0
output_per_mtok = 15.0
cache_read_per_mtok = 0.3
cache_write_per_mtok = 3.75
```

The real bill is the provider's. The report says how many costs came from the agent and how many from prices.

## `cork stats`

```bash
cork stats [--since 7d] [--channel work/] [--label k=v] [--feature <id>]
           [--by model|pool|profile|channel|label:<key>|day] [--json | --csv]
           [--reasons] [--file export.jsonl] [--models FILE] [--config FILE]
```

It replays the log through `/events`, or reads an export with `--file`, so it never writes and runs on any machine that can reach the board. The same log always gives the same report.

```text
model   tasks  done  failed  success  p50 claim  p50 work  p95 work  p50 done  attempts/task  1st ok  expired   escalated  tokens in/out  turns  unknown    cost  cost/done
haiku       2     1       1      50%       10ms      13ms      1.0s      1.1s           1.50      0%        1           0     2.0k / 200      4        1  $0.30?     $0.40?
sonnet      1     1       0     100%        8ms       7ms       7ms      15ms           1.00    100%        0  1 (1 done)     1.0k / 100      2        0   $0.50          -
all         3     2       1      67%        9ms       7ms      1.0s      15ms           1.33     33%        1  1 (1 done)     3.0k / 300      6        1  $0.80?     $0.40?
? 1 attempts have unknown usage (no usage reply, such as a wrapper killed with kill -9) and 0 have no cost; costs shown are a lower bound
cost of 3 attempts: 3 from the agent. Estimates; the real bill is the provider's.

cost per successful task, by the model it started on and the model that finished it
started  finished  done    cost  cost/done
haiku       haiku     1  $0.20?     $0.20?
haiku      sonnet     1   $0.60      $0.60

queue: peak 1 ready, mean 0.07, 0 ready at the end
```

That is the test run in `crates/cork-cli/tests/stats.rs`: haiku fails a task and sonnet completes its escalated retry, and one haiku wrapper is killed with `kill -9`.

### What each column means

- **tasks, done, failed**: tasks in the group by their state now. **success** is done / (done + failed).
- **p50 claim**: how long a task was ready before a claim. Every time a task becomes ready counts: created, reopened, unblocked, released, or after a lease expiry.
- **p50 / p95 work**: from a claim to the attempt's end (a settle or a lease expiry).
- **p50 done**: from creation to completion.
- **attempts/task** and **1st ok**: the share of tasks whose first attempt completed.
- **expired**: lease expiries, a sign of crashes or slow machines.
- **escalated**: tasks that are a retry (`retry_of`), and how many of them completed.
- **tokens, turns, cost**: summed over the group's attempts. **unknown** counts finished attempts with no usage reply.
- **cost/done**: the headline number. See below.

A `?` marks a cost that leaves out attempts of unknown cost, so it is a lower bound.

### Cost per successful task

A **chain** is a task and every retry made from it with `cork tasks escalate` (the `retry_of` label). A chain's cost is everything spent on it: failed attempts, lease expiries and every retry. **cost/done** is the cost of the chains that started in a group, divided by how many of those chains completed.

A chain is charged to where it **started**. With `--by model`, that is the model of the first attempt of its first task. So "start on haiku and escalate" can be compared with "start on sonnet": a cheap model that fails half the time and needs a retry on a strong one can cost more per success than starting strong. The second table splits each completed chain by the model that finished it.

The per-group **cost** column is different: it counts each attempt where it ran.

### Grouping

- `--by model` (the default) uses the `model` in each usage reply. For an attempt with no usage reply, it reads the pool from the profile name (`<fleet>-<pool>-<n>`) and the pool's model from `workers.toml`, or takes the pool name if a model in `models.toml` has that name. Otherwise the model is `unknown`.
- `--by pool` and `--by profile` work the same way.
- Task-level numbers (tasks, success, time to done) use the task's first attempt; attempt-level numbers (work time, usage) use each attempt's own model, pool or profile. A task never claimed is `(unclaimed)`.
- `--by channel`, `--by label:<key>` and `--by day` (UTC creation date) group by the task.

### Filters

- `--since 7d`: tasks created in the last seven days.
- `--channel work/`: tasks in those channels.
- `--label k=v`: tasks with that label; repeatable.
- `--feature <id>`: the feature task and its children (`parent=<id>`), retries included.

### Formats

`--json` prints the whole report, including every percentile, the cost sources and the queue depth. `--csv` prints one row per group and an `all` row, in milliseconds and dollars. `--reasons` adds failure reasons grouped by their first line. Without it, `cork stats` prints no message text.

The queue line reports the most tasks ready at once, the time-weighted mean, and how many are ready at the end of the log. It covers every task the events include, before the label and feature filters.

## Live metrics

### The daemon: `GET /metrics`

The daemon serves Prometheus text at `/metrics` (next to `/v1`, not under it), behind the same Host check and token as the API:

```bash
curl -s -H "Authorization: Bearer $(jq -r .token ~/.config/cork/daemon.json)" \
  http://127.0.0.1:7420/metrics
```

It holds only what the daemon knows. No model or cost data: the daemon knows nothing of models.

| Metric | What it is |
| --- | --- |
| `cork_tasks{channel,state}` | Tasks by stored state. Counted from the store at start, then kept up to date from each commit. |
| `cork_events_total{type}` | Events committed since the daemon started, by type. |
| `cork_claims_total`, `cork_settles_total{op}`, `cork_lease_expiries_total` | The same counts by name: claims, settles (`complete`, `fail`, `release`, `block`) and lease expiries. |
| `cork_commits_total` | Commits that wrote events. |
| `cork_idempotency_hits_total` | Writes answered from a stored idempotency key. |
| `cork_write_seconds` | Histogram: time in the store's write transaction, per write. |
| `cork_commit_events` | Histogram: events per commit. |
| `cork_log_head` | The last committed position. |
| `cork_waiters`, `cork_streams` | Claims waiting for a task, and open event streams. |
| `cork_degraded` | 1 while the store refuses writes. |
| `cork_wal_bytes` | The size of the SQLite write-ahead log. |

Counters start at zero when the daemon starts, as Prometheus expects. `cork_tasks` is the stored state: a lease that has run out shows as claimed until the lease timer records it, which takes moments.

Everything is updated after a commit with atomics or a short lock, and a scrape never reads the store, so scraping does not slow writes.

`cork serve --no-metrics-channels` leaves channel names out: `cork_tasks` is then summed per state. Use it on boards whose channel names are private.

`cork-stress` checks `/metrics` against the log after every scenario: every `cork_events_total` counter against the events since the daemon started, and every `cork_tasks` gauge against the stored tasks.

### The fleet supervisor

`cork workers metrics` prints the supervisor's metrics over its control socket. To let Prometheus scrape it, set a loopback port in `workers.toml`:

```toml
[fleet]
name = "laptop"
models = "models.toml"
metrics_port = 9464   # serves http://127.0.0.1:9464/metrics
```

| Metric | What it is |
| --- | --- |
| `cork_fleet_workers{pool,model,state}` | Workers `busy`, `idle` and `draining`. |
| `cork_fleet_pool_target{pool}` | Workers each pool aims for. |
| `cork_fleet_pool_paused{pool}` | 1 while a pool is paused after a crash loop. |
| `cork_fleet_restarts_total{pool}`, `cork_fleet_crash_loops_total{pool}` | Crashes, and pauses for crash loops. |
| `cork_fleet_unmatched_tasks` | Ready tasks with needs no pool covers. |
| `cork_fleet_usage_attempts_total{pool,model}` | Attempts whose wrapper reported usage. |
| `cork_fleet_input_tokens_total`, `cork_fleet_output_tokens_total`, `cork_fleet_cost_usd_total`, `cork_fleet_cost_unknown_total` | Usage per pool and model, from the same records the wrappers post to the board. Cost is what the agents reported. |

The supervisor sets `$CORK_USAGE_REPORT` for each worker, and the wrapper leaves its usage record there as well as posting it. Like the daemon's, these counters start at zero when the supervisor starts; `cork stats` is the record that lasts.

### Dashboards

[`docs/grafana-dashboard.json`](grafana-dashboard.json) is an example Grafana dashboard for both: tasks by state, claims and settles, lease expiries, write latency, waiters, WAL size, and the fleet's workers, cost and tokens by model. Cork ships no alerting of its own.

## Not built

- **OpenTelemetry tracing** (HANDOFF 18.5) is marked "later and optional" in the spec, so it is not part of Milestone 3.
- **The real report** (18.7 item 7) needs the real lead runs of 17.5, which use paid agents and are still to do. Once they run, `cork stats --feature <id>` on that board gives the numbers for the write-up.
