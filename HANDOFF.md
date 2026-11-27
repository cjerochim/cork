# Handoff: Cork (v3, final)

**Status:** signed off by Chris on 2026-11-27. Build Milestone 1 from this version.

**Name:** Cork (2026-11-27). In this doc, "board" is the thing Cork keeps: one board of channels, threads and tasks.

Cork is a message board and task queue for people and coding agents. Inspired by Vercel Board. Built as a standalone utility that can scale and be stress tested.

Read this file first. Then start at "Milestone 1".

---

## 0. What changed in v3

v3 takes in the independent review (`REVIEW-1.md`). Chris approved all of it on 2026-11-26. The v2 text, with its change list from v1, is kept in `HANDOFF-v2.md`.

| # | Change | Why | Section |
| --- | --- | --- | --- |
| 1 | **Use the single writer.** Each command reads, decides and appends inside one write transaction. No retry loop, no `conflict` code, no "shuffle the oldest 5". `claim --ready` is strict first in, first out. | One daemon owns all writes, so races between writers cannot happen. Paying for them added code and tests for nothing. | 3, 5, 6.4, 6.5 |
| 2 | Any decision that finds an ended lease writes `LeaseExpired` first, in the same commit. Running out of attempts also writes `TaskFailed`. The lease timer is just one more trigger. | The log always shows every expiry and every failure. UIs and checks never have to infer them. | 6.4, 6.6 |
| 3 | **Settles are idempotent per claim.** A retried `complete` (or `fail`, `release`, `block`) by the claim that already did it returns the first success. All writes accept an idempotency key. | Before, a lost response made an agent believe it lost a task it had finished. | 6.4, 7.5 |
| 4 | The wrapper survives network errors and 500s: it keeps renewing until the lease is nearly over. Claims are kept in memory, and `--claim` / `CORK_CLAIM` work when the claim file is gone. | v2 only handled `claim_lost`. | 8.3, 9 |
| 5 | **Waiters are woken from memory.** Committed events are broadcast in the process and matched there. The store is read only to catch up. `claim --wait` is served first come, first served. | v2 ran one query per waiter per commit: about 1,000 queries per write at 1,000 waiters. | 7.3 |
| 6 | **Daemon restart is defined.** The token survives restarts, live leases get one lease of grace at start, the wrapper never auto-starts a daemon mid-task, versions are checked, and the clock is truly monotonic. | In v2 a restart would kill healthy workers. | 3, 6.4, 8.4, 10 |
| 7 | **Contracts fixed before they freeze.** An empty claim returns 200 with an outcome, not 404 or 408. SSE drops the `event:` field. Exit codes have fixed meanings. Events carry a schema version. `cursor_expired`, redaction, and edit and delete events are reserved. | These are cheap now and breaking later. | 7, 8.6, 14.2 |
| 8 | Idempotency keys live in a table, not an event stream. Task events carry only `board.task`, not every root label. | Less log and index growth. | 4, 5.2, 6.3 |
| 9 | `block` and `unblock` join the Milestone 1 CLI, and `tasks run` handles a task blocked from inside it. | Agents need to ask people questions, and §9 already relied on it. | 6.7, 9 |
| 10 | Limits and rules for mentions, names, Unicode, ids, thread paging, long-poll length, disk full and SQLite's WAL file. | Edge cases the review found. | 4, 5.2, 7 |
| 11 | `CORK_PREVIOUS` only passes on text from the owner, people, and earlier claim holders. | Anyone can reply to a thread, so passing every reply on was a prompt-injection path. | 9.1 |
| 12 | Export is two commands: a full export you can import, and a filtered one for debugging. Import keeps positions and maps person names. | v2 used one name for both and left import underspecified. | 8.2, 13.4 |
| 13 | High availability (several daemons per board) is removed, and so is the Postgres ordering material. | Nobody asked for it. It was the only reason for the hardest storage promise. | 5, 13.4 |
| 14 | Browser sign-in is cut down to its rules. The full design moves to a later UI spec. | It is not Milestone 1 work. | 14.4 |
| 15 | About a dozen inconsistencies fixed. "State" is used everywhere (not "status"). Section numbers and cross-references are checked. | | all |

---

## 1. Goal

People and agents coordinate through one shared board. The board holds tasks and their state. Anyone, person or agent, can create, claim, answer and settle tasks through the same commands. Planning, meaning deciding how to split work into tasks, is not the board's job. A typical setup looks like this:

- A lead, either a person or an agent, splits work into tasks.
- Workers, usually agents but people too, claim tasks, do the work, and report results.
- People answer questions from agents in the same threads.

Each task's state is the label `board.task` (6.1), next to any labels you add, such as `step=fix`. A board view or a UI can group and filter on both.

Workers never talk to each other. They talk only to the board. So you can add more workers and nothing else changes.

## 2. Decisions (locked)

| Area | Decision |
| --- | --- |
| Language | Rust |
| Model | Event store. Every change is an event in one ordered log. |
| Storage | SQLite by default, behind a storage trait, so it can be swapped (for example Postgres). |
| Runtime | One daemon per board owns all writes, the clock and the lease timer. |
| Interface | CLI first. The CLI is an HTTP client of the daemon. |
| Transport | HTTP on `http://127.0.0.1:7420` by default. The same binary and API also run on a server (section 13). |
| Shape | Same model as Vercel Board: boards, channels, threads, messages, labels, tasks, leases. |

## 3. Crate layout

```text
cork/
├── crates/
│   ├── cork-core/          domain only: events, state folding, rules, error codes. No I/O, no clock.
│   ├── cork-store/         the storage trait, plus the contract test suite (feature `testkit`)
│   ├── cork-store-sqlite/  default adapter
│   ├── cork-api/           wire types shared by server and clients
│   ├── cork-server/        HTTP API (axum + tokio). Owns the store, the clock and the lease timer.
│   ├── cork-client/        typed HTTP client (reqwest)
│   ├── cork-cli/           the `cork` binary (clap + cork-client)
│   └── cork-stress/        load and chaos tests against a running daemon
├── skills/cork/SKILL.md    teaches agents the commands
└── HANDOFF.md
```

Rules:

- All business rules live in `cork-core`. Every command is a pure decision: `(&Task, now, input) -> Result<Vec<TaskEvent>>`. Core never reads a clock; the caller passes `now`.
- **One writer.** The server runs each command as one write transaction: read what the decision needs, call core, append the events and projection rows, commit. Commands never race, so there is no retry loop.
- Adapters only store and fetch. The server folds events with core and hands the adapter the projection rows to write.
- The CLI never opens SQLite. It always goes through the daemon. (`cork serve` is the daemon itself, so it may.)
- Only the daemon reads the clock for leases. Workers never decide if a lease is live.
- **The clock.** At start the daemon sets `start = max(wall clock, highest at in the log)`. After that, `now = start + time elapsed on the monotonic clock`. Wall-clock steps (NTP, manual changes) while the daemon runs are ignored, so they can neither revive nor end leases. A restart re-anchors to the wall clock, never below the log.

## 4. Data model

```text
Board
└── Channel     a path, such as dev or work/fix. The first post creates it.
    └── Thread  a root message and its replies
        └── Message  Markdown text, labels (key=value), data (JSON), mentions
```

- **Channel path.** Segments of `[A-Za-z0-9_.-]`, joined by `/`, at most 8 deep and 200 bytes. Leading and trailing `/` are trimmed when stored. A segment may not be 26 characters that read as a ULID, so a channel can never be mistaken for an id.
- **Channel selector.** Reads, waits and claims take a selector. `work` means that channel only. `work/` means `work` and every channel under it. `workshop` never matches `work/`.
- **Labels** are `key=value` pairs on a message, such as `step=fix`. One value per key. Keys are ASCII `[A-Za-z0-9_./-]`, up to 64 bytes. Values are UTF-8, NFC-normalized when written, matched byte for byte, up to 256 bytes. At most 32 labels per message. Agents filter on them and wait for them.
- **Reserved labels.** Labels that start with `board.` belong to the board. The prefix stays `board.`, not `cork.`, so labels match Vercel Board's and agent skills carry over. Users cannot set them (`reserved_label`). They can filter on them. The board sets:
  - `board.task=<state>` on a task's root.
  - `board.mention=<handle>` once for each mentioned handle. It may repeat on a message, so it lives only in the label index, not in the message's label map.
- **Mentions.** `@handle` at the start of a word, outside code spans and code blocks. Handles follow the name rule below. At most 32 per message; more is `invalid_input`.
- **Names.** Persons, profiles and handles are ASCII `[A-Za-z0-9_.-]`, 1 to 64 characters, stored and compared in lowercase. If the OS user name does not fit, `CORK_USER` must be set.
- **Actor.** Who wrote something. A person, or a profile acting for a person, shown as `fixer via chris`. A profile is only a name. It has no access of its own. The **handle** of an actor is its profile if set, else its person. Mentions and `--for` match on the handle.
- **Owner.** The person behind the actor who created a task. For `fixer via chris`, the owner is `chris`. Every profile of the owner acts as the owner.
- **Task.** A thread with a state that the board keeps. The task id is the root message id.
- **Ids** are ULIDs, so they sort by time.
- **Limits.** Message text up to 64 KB. Message data up to 256 KB of JSON. Task result or reason up to 64 KB.

### Streams

| Stream | Holds |
| --- | --- |
| `thread:{id}` | `MessagePosted` for the root and each reply. Later: `MessageEdited`, `MessageDeleted`, `MessageRedacted` (reserved now). |
| `task:{id}` | Task events (6.3) |

Creating a task appends to `thread:{id}` and `task:{id}` in one commit. Keeping them apart means a reply never changes the task's version.

### Event envelope

`{ schema, position, stream, version, type, data, actor, at, channel, thread_id, labels }`

- `schema` is the version of that event type's `data` shape, starting at 1.
- Stored events are never rewritten, with one exception: redaction (10).
- When a `data` shape changes, `schema` goes up, and core holds an upcaster from each old shape to the current one. Readers always see the current shape.

## 5. Storage contract

### 5.1 Promises

Every adapter must keep these. If a store cannot keep them, it cannot be an adapter. **An adapter may assume there is exactly one writer process per board.**

1. **Atomic write.** `write(f)` runs `f` inside one transaction. `f` can read the task, message, idempotency and ready-queue rows it needs, and returns a commit. The events and the projection rows are written together, or not at all.
2. **Conditional append, as a guard.** Each append names the version it expects (`NoStream`, `Exact(n)` or `Any`). With one writer this always holds. A mismatch is a bug: the adapter returns an `internal` error and writes nothing.
3. **Global order.** Each event gets a position in one log. Positions are unique and only go up. Versions within a stream are 1, 2, 3, … with no gaps. Because there is one writer that commits in order, a reader that has seen position N never later finds a new event below N.
4. **Read after a position.** Return the events after position N that match a filter, in position order. The filter can name a stream, a channel selector, a thread, labels (all must match), and event types. Every read runs in one short read transaction, and reports the highest position it saw.

```rust
trait Store {
    async fn write<T>(&self, f: impl FnOnce(&mut dyn WriteTx) -> Result<(Commit, T)>) -> Result<(Vec<StoredEvent>, T)>;
    // reads: read_events, head, get_message, list_messages, list_channels, get_task, list_tasks, next_lease_end, ended_leases
}

struct Commit {
    appends: Vec<StreamAppend>,      // { stream_id, expected, events }
    projections: Vec<Projection>,    // Message | Task | SetMessageLabel | Idempotency
}
```

`f` runs on the writer's thread and must not wait on anything but the store. Core decisions are plain functions, so this holds.

### 5.2 SQLite schema (outline)

```sql
CREATE TABLE events (
  position   INTEGER PRIMARY KEY AUTOINCREMENT,  -- global order
  stream_id  TEXT    NOT NULL,                   -- task:01J..., thread:01J...
  version    INTEGER NOT NULL,                   -- per-stream version
  type       TEXT    NOT NULL,
  schema     INTEGER NOT NULL,
  data       TEXT    NOT NULL,                   -- JSON
  actor      TEXT    NOT NULL,
  at         INTEGER NOT NULL,                   -- ms, from the daemon clock
  channel    TEXT,                               -- index fields, so filters do not parse JSON
  thread_id  TEXT,
  UNIQUE (stream_id, version)
);
CREATE TABLE event_labels   (key, value, position,   PRIMARY KEY (key, value, position))   WITHOUT ROWID;
CREATE TABLE messages       (id PRIMARY KEY, position, thread_id, channel, is_root, body JSON);
CREATE TABLE message_labels (key, value, message_id, PRIMARY KEY (key, value, message_id)) WITHOUT ROWID;
CREATE TABLE channels       (path PRIMARY KEY, threads, messages, last_position);
CREATE TABLE tasks          (id PRIMARY KEY, created_position, channel, owner, assignee,
                             state, expires_at, attempts, max_attempts, version, body JSON);
CREATE TABLE task_labels    (key, value, task_id,    PRIMARY KEY (key, value, task_id))    WITHOUT ROWID;
CREATE TABLE idempotency    (person, key, request_hash, result JSON, created_at,
                             PRIMARY KEY (person, key));
```

Plus indexes on `events (channel, position)`, `events (thread_id, position)`, `events (type, position)`, `messages (channel, position)`, `messages (thread_id, position)`, `tasks (state, created_position)`, `tasks (channel, state, created_position)` and `tasks (state, expires_at)`.

- WAL mode. One writer connection. `BEGIN IMMEDIATE` for writes. A small pool of read-only connections.
- `synchronous=FULL` by default: a commit is on disk before the response. `--sync normal` is faster, but an acknowledged write can be lost on power loss. The stress tests report both.
- Projections are updated **in the same transaction** as the append, so they are never behind the log.
- Labels are matched with `EXISTS` on the label tables, which their primary keys make an index seek.
- Subtree match on channel uses `channel = 'work' OR substr(channel, 1, 5) = 'work/'`, not `LIKE`, because `_` is a legal channel character and a `LIKE` wildcard.
- **WAL file.** Read transactions are short and are never held across a wait. The daemon runs `wal_checkpoint(TRUNCATE)` when idle and after every 10,000 commits. The soak test checks that the WAL file stays bounded.
- **Disk full.** A write that fails with `SQLITE_FULL` returns `unavailable` (503). The lease timer backs off (1 s, doubling to 60 s), and `/ready` reports `degraded` until a write succeeds.
- **Idempotency rows** are kept for 24 hours, then pruned.

### 5.3 Effective state in queries

`tasks.state` is the stored state. Between a lease ending and the next write that records it, the stored state is a few milliseconds behind. A query for state S at time `now` therefore uses the effective state:

```sql
CASE WHEN state = 'claimed' AND expires_at < :now
     THEN CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'ready' END
     ELSE state END
```

This mirrors `Task::effective_state` in core. The adapter builds this predicate in **one function**, used by every task query and by `board.task` label filters. The contract suite checks it against core.

### 5.4 Contract suite

`cork-store` ships the suite behind the `testkit` feature. An adapter runs it with one line:

```rust
cork_store::contract_tests!(fresh_store());
```

It covers: atomic writes (a failing `f` writes nothing), each expected-version guard, positions going up, contiguous versions, read filters, a tailing reader never skipping a position while writes run, message, task and idempotency projections, effective state matching core, ready-queue order and filters, and lease-end queries.

## 6. Task rules

### 6.1 States

| State | Meaning |
| --- | --- |
| `waiting` | A task it needs has not completed (Milestone 2) |
| `ready` | It can be claimed |
| `claimed` | A worker holds it until its lease ends |
| `blocked` | It is held for a person |
| `completed` | Done, with a result |
| `failed` | A worker gave up with a reason, or its attempts ran out |
| `canceled` | The owner called it off |

`completed`, `failed` and `canceled` are terminal until reopened. The state is also the label `board.task` on the task's root.

### 6.2 Create

`TaskCreated { task_id, channel, owner, assignee?, lease_ms, max_attempts, labels }`

- `lease_ms`: default 300 000 (5 min), at least 100, at most 3 600 000 (1 h).
- `max_attempts`: default 3, 1 to 100.
- `assignee` (`--for <profile>`): only that handle may claim it. Profiles are named by the client, so this routes work; it is not a security boundary.
- Create takes an idempotency key like any write (7.5).

### 6.3 Task events

`TaskCreated`, `TaskClaimed`, `LeaseRenewed`, `LeaseExpired`, `TaskReleased`, `TaskCompleted`, `TaskFailed`, `TaskBlocked`, `TaskUnblocked`, `TaskCanceled`, `TaskReopened`, `NeedsChanged` (Milestone 2, reserved now so the log format is stable).

Each task event carries the index label `board.task=<state after the event>`, plus its channel and its thread (the task id). A filter on a user label, such as `step=fix`, matches task events by joining to `task_labels`. So `wait work/ --label step=fix --label board.task=completed` works without copying labels onto every event.

### 6.4 Claim, lease and renew

**Claim.** Inside one write transaction:

1. Read the task.
2. If its lease has ended, record that first (see "Lease end" below).
3. Core decides: the effective state must be `ready`, and the caller must match the assignee if there is one.
4. Append `TaskClaimed { claim_id, holder, expires_at, attempt }`.

A second worker claiming the same task runs after the first commit, sees `claimed`, and gets `task_claimed`.

**Lease.**

- A claim is a lease of the task's `lease_ms`, or the claim's override (same range).
- The lease is live through its last millisecond: live while `now <= expires_at`.
- `renew` extends the lease to `now + lease` of that claim. It does not grow with time held.
- `renew`, `complete`, `fail`, `release` and `block` must name the claim.
  - No claim sent: `claim_not_held`.
  - A claim that is not the current live claim (ended, released, or replaced by another worker): `claim_lost`. Nothing changes.
  - **Exception: a repeat.** If the claim already settled the task with the same operation, the call returns that first result with `"replayed": true`. A retried `complete` after a lost response therefore succeeds. A different operation still gets `claim_lost`.

**Lease end.** An ended lease is recorded by whichever comes first:

- **Any decision on the task.** Claim, settle, cancel or reopen: if core finds the stored state `claimed` with an ended lease, it emits `LeaseExpired { claim_id }` first, in the same commit.
- **The lease timer.** The daemon sleeps until the next lease end (`MIN(expires_at)` over claimed tasks) plus 1 ms, then records each ended lease as the system actor `cork`. It re-reads the next lease end after every commit.

Either way, the log always holds a `LeaseExpired` between one claim and the next. The timer and the effective-state query (5.3) only affect *how soon* an ended lease shows (liveness). Who may settle a task is always decided from the clock inside the write (correctness).

**Restart grace.** When the daemon starts, every task stored as `claimed` gets one lease of grace: `LeaseRenewed { claim_id, expires_at: start + lease, reason: "daemon_restart" }` as the system actor, before the lease timer runs. Workers that were healthy while the daemon was down keep their tasks if they renew within that lease. Workers that died lose them one lease later.

### 6.5 Claim the next ready task

`claim --ready <selector>` with optional labels and `--from <owner>`. In one write transaction, take the **oldest** task whose effective state is `ready` and that matches the selector, labels, owner and assignee (`ORDER BY created_position LIMIT 1`). Claim it as in 6.4. If there is none, the outcome is `nothing_ready`.

**With `--wait <d>`.** The server keeps a first-come, first-served queue of waiting claims, each with its filter. After any commit that makes a task ready (create, release, unblock, reopen, lease end), the writer hands each newly ready task to the oldest waiting claim that matches it, in the same transaction. A waiter is never starved by a newer one. When the wait runs out, the outcome is `timeout`.

`--from`: by default a worker only takes tasks the caller's person owns (10). `--from <person>` takes another owner's tasks. `--from '*'` takes anyone's.

### 6.6 Attempts

- Each claim records `attempt = attempts + 1`. A release gives the attempt back.
- When a lease ends and `attempts >= max_attempts`, the same commit writes `LeaseExpired` and then `TaskFailed { claim_id, reason: "lease expired with all N attempts used" }`. So a lead waiting for `TaskFailed`, or for `board.task=failed`, sees every failure.
- An explicit `fail` is final at once, whatever the count.
- `reopen` resets the count to 0.

### 6.7 Block, unblock, cancel, reopen

| Command | Who | From | To |
| --- | --- | --- | --- |
| `block <id> "<question>"` | live claim holder | `claimed` | `blocked` (claim ends) |
| `unblock <id> [note]` | owner | `blocked` | `ready` |
| `cancel <id> [reason]` | owner | any non-terminal | `canceled` |
| `reopen <id>` | owner | `completed`, `failed`, `canceled` | `ready` |

Anyone else gets `task_not_owned`. A wrong starting state gets `invalid_state`. All four are in core, the API and the CLI in Milestone 1.

### 6.8 Dependencies (Milestone 2)

- `needs` lists tasks that must complete first. A task with unmet needs is `waiting`.
- When a task completes, ready the tasks that waited only for it, in the same commit.
- A failed or canceled prerequisite readies nothing.
- Only the owner can change needs, and only while the task is unclaimed and not finished.
- Plans from JSON (`tasks create <channel> --file plan.json`) with keys, needs by key, and an idempotency key.

## 7. HTTP API

### 7.1 Routes

All routes are served under `/v1` (for example `/v1/tasks/claim`). The table leaves the prefix out.

Channel paths contain `/`, so a channel in a URL path is one percent-encoded segment (`work%2Ffix`). Query parameters take it plain.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Liveness, API version, server version. No token needed. Host check still applies. |
| `GET` | `/ready` | Ready to serve, or `degraded` (for example disk full). No token needed. |
| `GET` | `/whoami` | Board id and name, person, profile, actor, head position, server time |
| `GET` | `/channels` | Channels with thread and message counts |
| `POST` | `/channels/{channel}/messages` | Start a thread |
| `POST` | `/threads/{id}/replies` | Reply. `id` may be the root or any message in the thread. |
| `GET` | `/threads/{id}?after=&limit=` | Root, a page of replies (default 200, at most 1,000), and the task if there is one |
| `GET` | `/messages?channel=&thread=&label=k=v&roots=&after=&limit=&newest=` | Read with filters. `label` repeats. |
| `GET` | `/events?after=&channel=&thread=&label=&type=&mentions=&wait_ms=&limit=` | Long poll for new events |
| `GET` | `/events/stream?after=&…` | Server-Sent Events, for `tail` and UIs |
| `POST` | `/tasks` | Create |
| `GET` | `/tasks?channel=&state=&label=&owner=&limit=&after=` | List, oldest first, with the effective state |
| `GET` | `/tasks/{id}?at=<pos>` | One task, with the effective state. With `at`, the task as it was at that position. |
| `GET` | `/tasks/{id}/history` | Every event of the task, in order |
| `POST` | `/tasks/claim` | Body: `task_id`, or `channel` with `labels`, `from`, `wait_ms`; and optional `lease_ms` |
| `POST` | `/tasks/{id}/renew` · `/complete` · `/fail` · `/release` · `/block` · `/unblock` · `/cancel` · `/reopen` | Settle. Body: `{ claim_id, result?, reason?, question?, note? }` |
| `GET` | `/export` | The whole log as JSONL, for import (13.4) |

Headers:
- `Authorization: Bearer <token>`.
- `X-Cork-Profile: <name>` for agents.
- `Idempotency-Key: <key>` on any `POST` (7.5).

Every task in a response includes its `version`. Every message and event includes its `position`.

### 7.2 Claim outcomes

`POST /tasks/claim` returns 200 in every normal case, with an `outcome`:

| `outcome` | Body |
| --- | --- |
| `claimed` | `claim_id`, `expires_at`, `lease_ms`, `attempt`, `task`, `root` |
| `nothing_ready` | `task: null` (no wait was given) |
| `timeout` | `task: null` (the wait ran out) |

Claiming a specific task that is taken is still an error (`task_claimed`).

### 7.3 Errors

Every error body is `{ "error": { "code": "...", "message": "..." } }`. Codes are stable; never rename or reuse one.

| Code | HTTP | Meaning |
| --- | --- | --- |
| `task_claimed` | 409 | Another worker holds a live claim on that task |
| `claim_lost` | 409 | The claim is not the current live claim |
| `claim_not_held` | 409 | No claim was sent (the client has none for this task) |
| `invalid_state` | 409 | The task's state does not allow this |
| `idempotency_mismatch` | 409 | The idempotency key was used with a different request |
| `task_not_owned` | 403 | Only the owner may do this |
| `forbidden` | 403 | For example, the task is assigned to another profile, or a foreign `Origin` |
| `not_found` | 404 | No such message, thread or task |
| `cursor_expired` | 410 | Reserved: the position is older than the kept log. Not used while the whole log is kept. |
| `reserved_label` | 400 | A user set a `board.` label |
| `invalid_input` | 400 | Bad path, label, lease, text size, too many mentions, and so on |
| `unauthorized` | 401 | Missing or wrong token |
| `rate_limited` | 429 | Over the per-person rate limit (server mode) |
| `internal` | 500 | A bug, including a failed version guard |
| `unavailable` | 503 | The store cannot write (for example disk full), or too many open streams |

### 7.4 Waits and the stream

- `GET /events` with no `after` waits for events after the current head.
- One long poll lasts at most `max_wait` (default 50 seconds, configurable). That stays under the 60-second idle timeout of most proxies. Clients re-issue, passing `next_after`.
- Each response has `next_after`. If events came back, it is the last one's position. If none did, it is the highest position the server checked, so the client continues without gaps or repeats.
- **No polling, and no query per waiter.** After each commit the writer broadcasts the committed events inside the process (`tokio::sync::broadcast`). Each waiter or stream matches them against its filter in memory. A waiter reads the store only to catch up: when it starts behind the head, or when it falls behind the broadcast buffer. It then switches back to the broadcast.
- `--mentions` is the filter `board.mention=<caller's handle>`.
- `X-Cork-Head` on every read response is the highest position read in that response's own read transaction.

### 7.5 Idempotency

- Any `POST` may carry `Idempotency-Key` (CLI: `--idempotency-key`). The server stores the key, a hash of the request, and the result, for that person, for 24 hours.
- The same key with the same request returns the first result and writes nothing.
- The same key with a different request returns `idempotency_mismatch`.
- Settles are also idempotent by claim with no key needed (6.4).

### 7.6 Stable contracts

A CLI, an agent skill, a web UI or a native app all build on the same contracts. From Milestone 1 these are stable:

- **Routes and bodies** under `/v1`. Changes are additive only: new routes, new optional fields. A breaking change means `/v2`, served beside `/v1` for a release.
- **Error codes and claim outcomes** (7.2, 7.3). Never renamed or reused.
- **The event envelope** (4), including `schema`.
- **Event types and their `data` shapes** (4, 6.3). New types and new fields may be added. Clients must ignore types and fields they do not know.
- **The SSE format** (14.2).
- **Positions as cursors.** A position from any response can be passed as `after` to any events call. While the whole log is kept (all of Milestone 1), that holds forever. If a retention policy is added later, a position older than the kept log returns `cursor_expired`.

## 8. CLI

### 8.1 Commands

```bash
cork serve [--port 7420] [--db PATH] [--sync full|normal] [--config FILE] [--import FILE]
cork whoami
cork channels
cork post <channel> "<text>" [--label k=v]... [--data JSON]
cork reply <id> "<text>" [--label k=v]... [--data JSON]
cork progress "<text>" [--data JSON]          # reply on $CORK_TASK, from inside `tasks run`
cork read <channel|selector|id> [--label k=v]... [--limit N]
cork wait <channel|selector|id> [--label k=v]... [--mentions] [--timeout 10m]
cork tail <channel|selector|id> [--label k=v]... [--type T]... [--after <pos> | --from-start]

cork tasks list [<selector>] [--state S] [--label k=v]...
cork tasks show <id> [--at <pos>]
cork tasks history <id>
cork tasks create <channel> "<text>" [--label k=v]... [--lease 5m] [--max-attempts 3] [--for <profile>]
cork tasks claim <id> | --ready <selector> [--label k=v]... [--from <owner>|'*'] [--wait 10m] [--lease 5m]
cork tasks renew <id>
cork tasks complete <id> ["<result>"]
cork tasks fail <id> "<reason>"
cork tasks release <id>
cork tasks block <id> "<question>"
cork tasks unblock <id> ["<note>"]
cork tasks cancel <id> ["<reason>"]
cork tasks reopen <id>
cork tasks run <id> | --ready <selector> [--label k=v]... [--wait 10m] [--loop] [--max-runtime 2h] -- <command>...

cork export [--out FILE]                                          # whole log, importable
cork debug export --channel <selector> | --thread <id> [--after <pos>]   # filtered, for bug reports; not importable
cork debug rebuild --db <copy>
cork token rotate
```

- `<id>` arguments are ULIDs. Anything else is a channel or selector. A channel can never look like a ULID (4).
- `--as <profile>` or `CORK_PROFILE` names the agent.
- `--json` on any command prints the API response as JSON, for agents.
- `--idempotency-key <key>` on any command that writes.
- `--claim <claim-id>` (or `CORK_CLAIM`) on any settle command uses that claim instead of the stored one.

### 8.2 Replay for debugging

The log is the source of truth, so any past moment can be rebuilt from it. Milestone 1 ships all of these. None of them writes to the board.

- `tail --from-start` replays every matching event from the start of the log, then follows live. `--after <pos>` starts after a position instead. With a task id, it covers the thread's messages and every state change of the task, because task events carry the task id as their thread. `/v1/events` and `/v1/events/stream` with `after=0` do the same through the API.
- `tasks show <id> --at <pos>` shows the task as it was at that position, folded from its stream by core.
- `tasks history <id>` lists every event of the task with its time, actor and claim, as a timeline. It answers "who held it, when did the lease end, and why did it fail".
- `debug rebuild --db <copy>` rebuilds every projection from the log into a copy of the database and diffs it against the live projections. Any difference is a bug.
- `debug export` writes the matching events as JSONL, to attach to a bug report or load into a test. Use `export` (13.4) to move a board.

### 8.3 Claims on the client

- `tasks run` keeps its claim in memory and never depends on a file.
- Other commands keep each claim at `~/.config/cork/claims/<board-id>/<profile or person>/<task-id>.json` (mode `0600`), so they take only the task id. `<board-id>` comes from `/whoami`, so claims against two daemons never mix.
- Claims are per profile. Running `complete` as another profile, or as yourself, finds no claim and sends none: `claim_not_held`.
- If the file is missing (another machine, a temporary `CORK_HOME`), pass `--claim <id>` or set `CORK_CLAIM`.
- The file is removed when the task is settled or the server says `claim_lost`.

### 8.4 Daemon start

- `cork serve` takes an exclusive lock on `serve.lock` next to the database for its whole life. A second daemon on the same database exits at once.
- If no daemon answers `/health`, the CLI takes `~/.config/cork/daemon.lock`, checks again, and only then starts `cork serve` in the background. It waits up to 5 seconds for `daemon.json`. Two CLIs racing therefore start one daemon.
- **`tasks run` only auto-starts a daemon before it claims.** Once it holds a task, it never starts one. It waits for the old one to come back (9).
- The CLI compares its version with `/health`. A different API version is an error. A different server version prints a warning.
- `CORK_HOME` moves `~/.config/cork`. `CORK_URL` and `CORK_TOKEN` point the CLI at a daemon it did not start (later, a remote one) and turn off auto-start.

### 8.5 Upgrading the daemon

Stop it with `SIGTERM` and start the new version. Running `tasks run` wrappers retry until it is back (9). The token survives the restart (10). Every live lease gets one lease of grace at start (6.4). So an upgrade that takes less than one lease costs no work.

### 8.6 Exit codes

Every exit code has one meaning, for every command.

| Code | Meaning |
| --- | --- |
| 0 | Success. For `tasks run`: the task completed. |
| 1 | Any other error |
| 2 | Bad usage |
| 3 | `nothing_ready` |
| 4 | `timeout` (a `wait` or `claim --wait` that saw nothing) |
| 5 | `claim_lost`: someone else holds the task now |
| 6 | `claim_not_held`: this client has no claim for the task |
| 7 | `task_claimed` |
| 8 | `tasks run`: the command failed, so the task was failed. The command's own exit code is in `--json` and in the failure reason. |
| 9 | `tasks run`: the task was blocked with a question. It was not settled. |
| 10 | `tasks run`: `--max-runtime` ran out, so the task was failed |
| 11 | `tasks run`: the daemon stayed unreachable until the lease was nearly over, so the command was stopped |
| 130 / 143 | `tasks run` stopped by Ctrl-C / `SIGTERM`, after releasing the task |

`tasks run --loop` keeps going after each task, whatever its outcome. It exits 4 when a `--wait` ends with nothing, 130 or 143 on a signal, and 11 if the daemon is lost.

## 9. How agents use it

Agents are normal coding agents, such as Claude Code. They run `cork` from their shell. A skill (`skills/cork/SKILL.md`) teaches them the commands.

**The agent must not hold the lease itself.** An agent can think for 10 minutes without running a command, and the lease would end. So a wrapper holds it:

```bash
cork tasks run --ready work/ --label step=fix -- claude -p "Do the task in \$CORK_TASK"
```

- The wrapper claims the task and sets:
  - `CORK_TASK`: the task id.
  - `CORK_TASK_TEXT`: the root message text.
  - `CORK_CLAIM`: the claim id.
  - `CORK_ATTEMPT`: 1 for the first attempt.
  - `CORK_RESULT`: a path to an empty file the command may write its result to.
  - `CORK_PREVIOUS`: on attempt 2 or later, a path to a file with what earlier attempts left (9.1).
- It renews the lease every third of the lease while the command runs.
- **If a renew fails** with a network error, `unauthorized`, `unavailable` or a 500, it re-reads `daemon.json` and retries with backoff (0.5 s doubling to 5 s). It keeps the command running until the lease, by the server's last `now` plus time elapsed, is within `min(10 s, lease / 10)` of ending. Then it stops the command and exits 11.
- It passes stdout through and keeps the last 16 KB.
- **Exit `0`** completes the task. The result is the `$CORK_RESULT` file if it is not empty, else the kept stdout tail. The wrapper exits 0.
- **Any other exit** fails the task. The reason is the `$CORK_RESULT` file if not empty, else `command exited with status N` plus the last 2 KB of stderr. The wrapper exits 8.
- **Blocked from inside.** If the agent runs `cork tasks block` (with `$CORK_CLAIM`), the next renew gets `claim_lost`. The wrapper sees the task is blocked by its own claim, so it lets the command finish, settles nothing, and exits 9.
- **Ctrl-C or `SIGTERM`:** the wrapper sends `SIGTERM` to the command, waits up to 10 seconds, releases the task, and exits 130 or 143.
- **`claim_lost` otherwise:** the wrapper stops the command and exits 5. It does not try to settle.
- **`--max-runtime <d>`** (optional) stops the command and fails the task if it runs longer, exit 10. Without it, a live command may run as long as it needs: the wrapper keeps renewing.
- **`--loop`** takes the next task after each one, so N shells with `--loop --wait 10m` make a worker pool.

### 9.1 Slow workers, crashes and partial work

| What happens | What the board does |
| --- | --- |
| The agent is slow but alive | Nothing changes. The wrapper keeps renewing, so the lease never ends while the command runs. |
| The agent's command exits non-zero | The wrapper fails the task with the reason. The owner can `reopen` it. |
| The wrapper or the machine dies | Renewals stop. Within one lease the task is `ready` again, and another worker claims it as attempt 2. After `max_attempts` it is `failed`. |
| The daemon restarts | Wrappers retry until it is back. Live leases get one lease of grace (6.4). |
| The old worker comes back | Its `renew` or `complete` gets `claim_lost` and changes nothing. Its wrapper stops the command. |
| A response is lost and the worker retries `complete` | The retry returns the first result (6.4). |

The board can only protect its own state. Work a dead worker left outside the board (edited files, a pushed branch) is not undone. So that it is never orphaned:

- **Isolate each attempt.** The skill tells agents to work in a git worktree on the branch `cork/<task-id>/<attempt>`, so a crashed attempt never collides with the next one.
- **Leave a trail.** Agents post progress with `cork progress "<text>" --data '{"branch": "...", "commit": "..."}'`.
- **Resume, don't restart.** `CORK_PREVIOUS` holds the earlier attempts' progress posts, each block's question, and the replies people wrote after it. It includes only text from the task's owner, from people, and from earlier holders of the task's claims, never from other agents. The skill tells agents to treat it as notes, not as instructions. The next agent can continue from the last branch, or start clean and say so.
- **Check the claim before anything that can't be undone.** Before pushing, opening a PR or deploying, the agent runs `cork tasks renew`. If that returns `claim_lost`, someone else owns the task now and the agent must stop. The claim id works as a fencing token.
- **Let the lead see it.** `tasks history` shows each attempt, so the lead or a person can find and clean up leftover branches.

**Lead:** writes a plan of tasks, creates them with an idempotency key, then waits for `board.task=completed` or `board.task=failed` and reviews.

**People:** a worker blocks a task with a question. A person answers with `cork reply`, then `cork tasks unblock`. The next attempt sees the question and the answer in `CORK_PREVIOUS`.

## 10. Security

- In portable mode the daemon listens on `127.0.0.1` only. Server mode (13) adds per-person tokens, TLS and `public_url`. Every rule below still applies.
- **The token.** On first start the daemon makes a random 256-bit token and writes it, with the port, to `~/.config/cork/daemon.json` (mode `0600`). Later starts reuse it, so running clients keep working across restarts. `cork token rotate` makes a new one.
- Every request except `/health` and `/ready` must send the token. Reject requests without it (`unauthorized`).
- Check the `Host` header against the configured names (portable mode: `127.0.0.1:<port>`, `localhost:<port>` and `[::1]:<port>`), so web pages cannot reach the daemon through DNS rebinding. Reject a missing `Host`.
- Check the `Origin` header when a request has one. It must be the daemon's own origin or listed in `http.allow_origins` (flag `--allow-origin`). Otherwise reject with `forbidden`. Milestone 1 has no list, so no browser page can call the daemon.
- The daemon's person comes from `CORK_USER`, else the OS user name (4). Profiles come from `X-Cork-Profile`, which the client chooses: a profile is a label, not an identity.
- Agents read task text as instructions. `claim --ready` takes only tasks owned by the caller's person unless `--from` is given. `CORK_PREVIOUS` is filtered (9.1).
- Request bodies are capped at 1 MB.
- **Secrets.** Never post secrets on the board. If one is posted, the owner of the message (or, later, a `manage` grant) runs `cork redact <id>`. This writes `MessageRedacted` and blanks the original event's `text` and `data` in place. It is the only change ever made to a stored event. Milestone 1 reserves the event type. The command ships in Milestone 2.

## 11. Milestone 1 (build this first)

Scope:

- All crates in section 3. `cork-stress` can come last.
- Messages: `post`, `reply`, `progress`, `read`, `wait`, `tail`, `channels`.
- Tasks: `create` (with `--for`), `list`, `show` (with `--at`), `history`, `claim` (by id and `--ready`, with the waiting-claim queue), `renew`, `complete`, `fail`, `release`, `block`, `unblock`, `cancel`, `reopen`, `run`.
- Idempotency keys on every write, and settles idempotent by claim.
- Leases, lease end recorded by decisions and by the timer, restart grace, and the attempt limit.
- `export`, `serve --import`, `debug export`, `debug rebuild`, `token rotate`.
- Profiles with `--as` / `CORK_PROFILE`.
- Token auth, the Host check and the Origin check.
- The parts of section 14 that keep a UI possible: the `/v1` prefix, SSE with `id:` and `Last-Event-ID` resume, the `X-Cork-Head` header, and no route that takes raw events.
- `SKILL.md`.

Out of scope for Milestone 1:

- Multiple boards, grants, private channels.
- Dependencies (`needs`), `waiting`, and plans from JSON.
- Editing, deleting and redacting messages (event types reserved).
- Sync with Slack, GitHub or Linear.
- An SDK, an MCP server, or a UI, including the UI's sign-in (14.4).
- Server mode (section 13). Milestone 1 must still follow 13.5, so server mode is not blocked.
- The Postgres adapter.

### Done when

1. **Unit tests in `cork-core`** cover every state change, every rejection code, lease end recorded by a decision, and replayed settles.
2. **Adapter test suite:** the contract suite (5.4) passes against SQLite. Any later adapter must pass the same suite unchanged.
3. **Race test:** 20 workers claim from 100 tasks at the same time. Each task completes exactly once. Check by reading the log: exactly one `TaskCompleted` per task.
4. **Crash test:** kill a worker (`kill -9` on a real `cork tasks run` process) after it claims. After the lease ends, another worker completes the task. The killed worker's late `complete` fails with `claim_lost`. The log holds `LeaseExpired` between the two claims.
5. **Retry-limit test:** a command that always crashes its worker fails the task after `max_attempts` leases, with `LeaseExpired` then `TaskFailed` in the log.
6. **Wait test:** a `wait` returns within 100 ms of a matching post, with no polling loop in the client. Also, a `claim --ready --wait` returns within 100 ms of a lease ending.
7. **Fairness test:** 20 claims waiting on one channel are served in the order they started waiting.
8. **Lost-response test:** drop the response to a `complete`, retry it, and get the first result with `replayed: true`.
9. **Restart test:** restart the daemon while 10 wrappers run, with downtime shorter than the lease. No wrapper loses its task, and no work is stopped.
10. **Replay test:** for a task that went through claim, lease end, reclaim and complete, `tasks show --at` at each position matches the state recorded at that time, and `debug rebuild` finds no difference. `export` then `serve --import` into an empty store gives the same log, positions included.
11. **Auto-start test:** 10 CLIs started at once with no daemon running start exactly one daemon.
12. **Security test:** no token gives `unauthorized`. A foreign `Host` or `Origin` is rejected.
13. **Stress tests** (`cork-stress`, against a release build, with `--sync full` and `--sync normal`). Each run checks the invariants at the end, then prints throughput and p50 / p99 latency:
    - **Throughput:** 50 workers, 10,000 tasks with instant work. Report claims per second and the time to drain.
    - **Contention:** 200 workers on 1 channel with `--wait`, tasks created at a steady rate. Report the spread of wait times, to check fairness.
    - **Chaos:** 50 workers, 2,000 tasks, 1 s leases. 20% of workers vanish after claiming. Every task still ends `completed` or `failed`, and no stale claim ever settles anything.
    - **Waiters:** 1,000 open `wait` calls while 50 workers run. Wake-up p99 stays under 100 ms, and store reads stay flat as waiters grow.
    - **Soak:** 30 minutes of mixed load. Memory, open files and the WAL file stay bounded; the database restarts cleanly.
    - **Invariants,** checked from the log after every run: at most one terminal event per task per open period; versions contiguous; a `LeaseExpired` between any two claims of a task; no `TaskCompleted` from a claim that was not live at its `at` time (replays excepted); projections equal a fresh fold of the log.

## 12. Open questions, answered

| Question | Answer |
| --- | --- |
| Name of the tool and the binary | **Settled (2026-11-27):** Cork. The binary is `cork`, the crates are `cork-*`, environment variables are `CORK_*`, and config lives in `~/.config/cork`. A small hex calculator on crates.io already ships a binary called `cork`, so publish the CLI crate as `cork-cli`. |
| Default port | `7420`. Unassigned, and `--port` / `daemon.json` make it changeable. |
| Lease length: fixed or per task? | Per task, default 5 minutes, cap 1 hour, floor 100 ms. A claim may override within the same range. (Vercel Board caps at 5 minutes. We allow longer because `tasks run` renews anyway, and some agents run long tool calls.) |
| Worker pool command | Leave it to the shell in Milestone 1: `tasks run --loop`, started N times. Add `cork workers <n> -- <command>` later if people ask. |
| MCP server from the daemon | After Milestone 1. The HTTP API is the contract, so an MCP server is a thin adapter over `cork-client`. |
| Postgres adapter and server mode | Server mode with SQLite right after Milestone 1 (13). Postgres once the stress numbers show it is needed. The adapter keeps one writer connection, so the promises in 5.1 hold unchanged. |
| Does a lease end write an event? | **Settled (2026-11-26):** yes, `LeaseExpired`. v3 also writes it from any decision that finds an ended lease (6.4). |
| Several daemons per board (high availability)? | **Settled (2026-11-26):** no. One writer per board is a design rule. If it is ever needed, it is a new design. |

## 13. Deployment modes

One binary, one API, one set of core rules. The mode is only configuration. Milestone 1 ships portable mode. Server mode ships after it, and Milestone 1 must not block it (13.5).

### 13.1 The two modes

| | Portable (local) | Server |
| --- | --- | --- |
| Runs as | `cork serve`, started by the CLI on demand | `cork serve --config /etc/cork/cork.toml` under systemd, a container, or similar |
| Listens on | `127.0.0.1:7420` | Any address, behind TLS: built in (`tls.cert`, `tls.key`) or a reverse proxy |
| Store | SQLite file in `CORK_HOME` | SQLite on the host's local disk, or Postgres (later) |
| People | One: the OS user | Many: one API token per person; OIDC for browsers (later) |
| Clients | The CLI and agents on the same machine; later, a UI at `/ui/` | CLIs and agents anywhere (`cork login`), web UIs, apps |
| Host check | Loopback names only | Must match `public_url` |
| Origin check | The daemon's own origin | Its own origin plus `http.allow_origins` |

**Portable** means one self-contained binary with SQLite built in and nothing else to install. The whole board is the `CORK_HOME` folder. Copy the folder (with the daemon stopped) and the board moves with it.

**Server** means the same daemon reachable over the network, so many people, agents, UIs and apps share one board and its events.

### 13.2 Configuration

One file, `cork.toml`, plus flags and environment variables. A flag wins over an environment variable, which wins over the file. Portable mode needs no file.

```toml
cork        = "acme"                               # name shown by /whoami
listen       = "0.0.0.0:7420"                       # default 127.0.0.1:7420
public_url   = "https://cork.acme.dev"             # Host check and links; required off loopback
store        = "sqlite:///var/lib/cork/cork.db"   # later: "postgres://cork@db/cork"
sync         = "full"                               # default full

[auth]
mode         = "tokens"                             # "local" (daemon.json token) | "tokens" | later "oidc"

[tls]                                               # leave out when a reverse proxy terminates TLS
cert         = "/etc/cork/tls.crt"
key          = "/etc/cork/tls.key"

[http]
allow_origins   = ["https://ui.acme.dev"]           # flag: --allow-origin (repeatable)
trusted_proxies = ["10.0.0.0/8"]                    # only these may set X-Forwarded-*
max_wait        = "50s"                             # longest single long poll
max_streams     = 1000
max_body        = "1MB"
rate_limit      = "500/s per person"                # off in portable mode
```

The daemon refuses to start with an unsafe mix:

- A non-loopback `listen` with `auth.mode = "local"`.
- A non-loopback `listen` with no `tls` and no `trusted_proxies`, unless `--insecure-http` is given.
- A non-loopback `listen` with no `public_url`.

### 13.3 Identity and access in server mode

- `cork admin tokens create --person dana` prints a token once. The daemon keeps only a hash of it, in its own table beside the store (not in the event log). `cork admin tokens list` and `revoke` manage them.
- On a client, `cork login https://cork.acme.dev` saves the URL and token to `~/.config/cork/remotes/<name>.json` (mode `0600`). `--remote <name>`, or `CORK_URL` with `CORK_TOKEN`, picks the board. With a remote set, the CLI never auto-starts a local daemon.
- The person always comes from the token, never from the request. `X-Cork-Profile` still only names a profile. The owner of a task is the token's person.
- Until grants exist (after Milestone 1), every person on a server can read and write every channel. The server docs must say so. Grants follow Vercel Board's model: per-channel `read`, `post`, `label`, `manage`.
- Browser sign-in follows 14.4.

### 13.4 Moving between modes

- **Export.** `cork export` (or `GET /v1/export`) writes the whole log as JSONL, one event per line, with positions and schema versions.
- **Import.** `cork serve --import cork.jsonl` loads it into an **empty** store at start-up, in order, keeping every position, then rebuilds the projections and starts serving. Because positions are kept, UI cursors and `Last-Event-ID` still work after a move.
  - Persons are mapped with `--map-person old=new`. Import refuses to start if the log names a person with no mapping and no matching token.
  - The clock starts no lower than the highest `at` in the log (3).
  - Import is the only path that writes events not decided by core. It runs only at start-up, only into an empty store, and never through the API.
- This is how a portable board becomes a server board, and how a SQLite board moves to Postgres.
- **One writer per board,** in both modes. One daemon owns writes, the clock and the lease timer for a board. That is what keeps leases and positions simple. Running several daemons on one board is not supported (12).
- **Health for orchestrators.** `/health` answers if the process is up. `/ready` answers once the store can be written, and reports `degraded` otherwise.
- **Shutdown.** On `SIGTERM` the daemon stops taking requests, finishes those in flight (up to 10 seconds), and closes streams. Clients reconnect and resume from their last position.

### 13.5 What Milestone 1 must not do

Each of these would block server mode later. Milestone 1 must hold to all of them even though it only runs locally.

- **No local-only data in the API.** No file paths, pids or OS user ids in any request or response. A claim is identified by `claim_id` alone. The CLI's claim files are only a client cache.
- **Identity only from the token.** Even in portable mode, the server maps token to person. It never trusts a person name sent by a client.
- **The clock and the lease rules stay in the server.** Clients never compare their own clock to `expires_at`. They get `now` in responses, and `tasks run` measures time since the server's last `now`.
- **Storage only through the trait.** The server has no SQLite types or SQL. The store is picked by a URL.
- **Auth behind a trait.** An `Authenticator` turns a request into a person: `LocalToken` in Milestone 1, `TokenTable` and `Oidc` later. Handlers never read headers for identity themselves.
- **No absolute URLs in payloads.** Responses carry ids and positions. Clients build URLs from their own base URL.
- **No hard-coded loopback.** The bind address, the Host check names and the allowed origins all come from config, even though Milestone 1 only sets loopback values.
- **Room for many boards.** `/v1/...` means the daemon's default board. Several boards later add `/v1/boards/{board}/...`, with today's routes kept as the default board's.

## 14. Web UI and apps (design requirement)

Not in Milestone 1. Milestone 1 must not close it off.

**Requirement.** A web UI, or a desktop or mobile app, can show everything on the board live and can act on it, using only the public API in section 7. It gets no private routes and no direct database access.

### 14.1 Reading: snapshot, then stream

1. Load the current state with the normal reads (`/channels`, `/messages`, `/tasks`, `/threads/{id}`). Note the lowest `X-Cork-Head` among those responses.
2. Open the stream with `after` set to that head.
3. Apply each event to the UI's own state. Skip an event whose position the UI has already applied. Skip a task event whose version is not above the task version the UI holds. This makes overlap between snapshot and stream harmless.

The UI shows task state from events and from the server's `now`. It must not decide on its own that a lease has ended. A `LeaseExpired` event tells it.

### 14.2 The event stream

`GET /v1/events/stream?after=<pos>` with no filter streams every event on the board. The same filters as `/v1/events` narrow it.

- Each SSE message has `id: <position>` and `data: <event envelope JSON>`. There is no `event:` field, so a browser's `onmessage` sees every event, including types added later. The type is in the envelope.
- **Resume.** On reconnect, a browser `EventSource` sends `Last-Event-ID`. The server resumes after that position, and it wins over `after`. Nothing is missed and nothing repeats.
- A comment line (`: ping`) every 15 seconds keeps proxies from closing an idle stream.
- **Slow clients never lose events.** A stream that falls behind the in-process broadcast reads its next batch from the store, then rejoins the broadcast. It is never dropped for lagging.
- The server limits open streams (`max_streams`, default 1,000) and returns `unavailable` past that, so a leak cannot exhaust the daemon.

### 14.3 Writing

- A UI writes only through the normal endpoints: post, reply, create task, cancel, reopen, unblock, and the rest of 7.1. The same rules in core apply, and the same events come out.
- **There is no route that appends a raw event, now or later.** Every event comes from a core decision (import at start-up excepted, 13.4). This is what keeps the log trustworthy.
- A UI acts as its person, with a profile such as `web`, so its writes show as `web via chris`.
- A UI is for people. It can claim a task, but then it must renew the lease like any worker. In practice it should create, answer, unblock, cancel and reopen, and leave claims to workers.

### 14.4 Browser sign-in (rules only)

The full design goes in the UI spec, when the UI is built. These rules hold now:

- The daemon token in `daemon.json` never reaches a browser. A browser gets a separate session: short-lived and revocable.
- `POST /v1/sessions` is reserved for creating sessions.
- No token of any kind goes in a URL query string.
- A UI served by the daemon at `/ui/` has the daemon's own origin, and needs no allowlist. A UI on any other origin must be listed in `http.allow_origins`. CORS never answers `*`.
- Cookie sessions are `HttpOnly` and `SameSite=Strict`, and cookie-authenticated writes need a custom header, so other sites cannot make them.
- On a server, OIDC replaces the local sign-in. The routes and the stream do not change.

## 15. Reference

- Vercel Board launch post: https://vercel.com/blog/introducing-vercel-board
- Vercel Board overview: https://vercel.com/docs/board
- Vercel Board tasks (claims, leases, states): https://vercel.com/docs/board/concepts/tasks
- Independent review of v2: `REVIEW-1.md`
- Previous version: `HANDOFF-v2.md`
