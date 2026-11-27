# Handoff: Cork (v3, final)

**Status:** signed off by Chris on 2026-11-27. Build Milestone 1 from this version.

**Name:** Cork (2026-11-27). In this doc, "board" is the thing Cork keeps: one board of channels, threads and tasks.

Cork is a message board and task queue for people and coding agents. Inspired by Vercel Board. Built as a standalone utility that can scale and be stress tested.

Read this file first. Then start at "Milestone 1".

**Added 2026-11-27, planned for Milestone 2, not to be built yet:** worker fleets with keyword routing (16), and a lead agent with parent and child tasks (17). **Planned for Milestone 3:** observability of performance, cost and usage (18).

**Added 2026-11-27, planned, not built:** task dependencies and the `waiting` state, in the core (6.8). Signed off by Chris on 2026-11-27, including both decisions in 6.8.10.

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
| `waiting` | Waiting on tasks: at least one task it depends on has not completed, nor a task that replaced it (6.8, planned) |
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

`TaskCreated`, `TaskClaimed`, `LeaseRenewed`, `LeaseExpired`, `TaskReleased`, `TaskCompleted`, `TaskFailed`, `TaskBlocked`, `TaskUnblocked`, `TaskCanceled`, `TaskReopened`. For dependencies (6.8, planned): `TaskReplaced`, `DependenciesMet`, `DependencyFailed`, and `DependenciesChanged`, reserved now so the log format is stable. (`DependenciesChanged` was reserved as `NeedsChanged` until 6.8 was written; no log holds it.)

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

**With `--wait <d>`.** The server keeps a first-come, first-served queue of waiting claims, each with its filter. After any commit that makes a task ready (create, release, unblock, reopen, lease end, and dependencies met, 6.8), the writer hands each newly ready task to the oldest waiting claim that matches it, in the same transaction. A waiter is never starved by a newer one. When the wait runs out, the outcome is `timeout`.

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

### 6.8 Dependencies (planned, not built)

**Status (2026-11-27):** signed off by Chris. That covers the core rules, dependencies fixed at creation, replacement declared, not edited in (6.8.3), and both decisions in 6.8.10. Nothing here is built. The build waits for Chris to say go, and Chris picks the milestone.

**Why it is in the core.** Readiness is the board's call, so dependencies are too. A worker or a client cannot decide that a task is ready, the same way it cannot decide that a lease is live (3). Keyword routing (16) stays out of the core because it only chooses *which* worker takes a ready task. Dependencies decide *whether* a task is ready at all.

**Declarative, not explicit.** A task states what it waits for, and a retry states what it replaces. The board derives everything else from those facts: whether a task is waiting, when it becomes ready, and whether it is stuck. Nobody edits a waiting task, moves it by hand, or tidies up after a retry. Every rule below follows from this.

#### 6.8.1 The rule

- A task may declare that it **depends on** other tasks: `depends_on`, a list of task ids on the same board. It is fixed when the task is created.
- A dependency is **met** when it is `completed`, or when the task that replaced it is met (6.8.3). So `--after X` means "after X, or whatever replaced X, completes".
- **`waiting`** ("waiting on tasks") means one thing only: at least one of the task's dependencies is not met. It has no other reason.
- A waiting task is never offered to a worker. `claim --ready` skips it, the waiting-claim queue (6.5) never gets it, and the fleet dispatcher (16.4) never sees it, because it only lists ready tasks.
- **Only the board moves a task out of `waiting`.** When its last unmet dependency is met, the board writes `DependenciesMet` on the waiting task, in the same commit as the `TaskCompleted` that met it. The task is then `ready`. No command moves it there by hand.
- **`waiting` is not `blocked`.** `blocked` keeps its meaning: an agent asked a person something and is held until a person unblocks it. A task can never be both. A waiting task has no claim, and only a claimed task can be blocked.

**Other hold reasons are out of scope.** A scheduled start ("not before 9 am"), a manual hold, or a quota could later be reasons to hold a task back too. They would get their own state or field, not a second meaning for `waiting`. They are noted here only so the name stays free of them.

#### 6.8.2 Naming: `depends_on`, `--after` and `--replaces`

The field is `depends_on` and the CLI flag is `--after`. The word `needs` is not used for dependencies, because `--needs` and the `needs.<keyword>` labels already mean model keywords (16.6, built in Milestone 2). This settles the naming note in 16.6: the keyword labels keep `needs.`.

A retry declares the task it takes over with `replaces` (CLI `--replaces <id>`).

The reserved event `NeedsChanged` (6.3) has never been written to any log, since nothing emits it. It is renamed `DependenciesChanged` and stays reserved, with no plan to write it (6.8.11).

#### 6.8.3 Replacement: how a retry takes over

Escalation (16.7) creates a new task for the same work. Without help, every task waiting on the old one would be stuck. Instead of editing those tasks, the retry **declares** what it replaces, and the board resolves dependencies through it.

```bash
cork tasks create work "<retry text>" --replaces $OLD --needs typescript,tier-2 --label parent=F
```

- **The retry takes the old task's place.** It inherits the old task's `depends_on` exactly. `--after` together with `--replaces` is `invalid_input`. That is what keeps the graph acyclic with no check (6.8.9): the retry sits in the old task's slot, with the old task's edges.
- **Who and when.** Only the old task's owner may replace it (`task_not_owned`), and the retry has the same owner. The old task must not be `claimed` or `completed` (`invalid_state`). A live worker is never cut off, and finished work is never redone through a replacement. A task is replaced at most once. Replacing it again is `invalid_state`, naming the task that replaced it, so the lead replaces that one instead.
- **What the board writes,** in one commit: the retry's `TaskCreated { replaces: OLD, … }`, and `TaskReplaced { by: NEW }` on the old task. `TaskReplaced` ends the old task: its `board.task` becomes `canceled`, unless it was already `failed` or `canceled`, which it stays. A replaced task cannot be reopened (`invalid_state`); reopen its replacement instead.
- **Resolution.** A dependency on `X` is resolved by following `replaced_by` from `X` to the newest task in the chain, its **head**. A dependency is met when its head is `completed`. Waiting tasks keep the ids they declared. Nothing is written on them when a replacement happens.
- **Chains** are at most 16 replacements long (`invalid_state` beyond that), which keeps every lookup bounded. The lead skill's limit of 2 escalations per child (17.2) stays far below it.
- **Links both ways.** `tasks show` on the old task shows "replaced by NEW", and on the retry "replaces OLD". This replaces the cancel and the linking reply that 16.7 does by hand today, and the `retry_of=<old>` label, which can stay for old tools.

#### 6.8.4 Events

| Event | On which stream | Actor | Data | `board.task` after |
| --- | --- | --- | --- | --- |
| `TaskCreated` (schema 2) | the new task | the creator | adds `depends_on: [id]`, `replaces?: id`, and `open_dependencies: [id]`: the declared ids whose head was not `completed` at create time | `waiting` if `open_dependencies` is not empty, else `ready` |
| `TaskReplaced` | the old task | the creator of the retry | `{ by: <new id> }` | `canceled`, or unchanged if already `failed` or `canceled` |
| `DependenciesMet` | each dependent that this commit readies | `cork` | `{ met_by: <id of the task whose completion met the last dependency> }` | `ready` |
| `DependencyFailed` (Decision 1) | each waiting dependent | `cork` | `{ dependency: <declared id>, head: <id>, state: "failed" \| "canceled", reason? }` | `waiting` (unchanged) |
| `TaskReopened` (schema 2) | the reopened task | the owner | adds `open_dependencies: [id]`, decided at reopen time | `waiting` if not empty, else `ready` |
| `DependenciesChanged` | reserved, not written | | | |

Schema 1 of `TaskCreated` and `TaskReopened` upcasts to empty lists and no `replaces`, so every log written by Milestone 1 and 2 reads unchanged.

**Why the create and reopen events carry `open_dependencies`.** A task's state must come from folding its own stream alone, as it does today (6.3, 8.2). Whether a new task starts `waiting` depends on other tasks, so the writer derives it once and records the answer in the event. Replay, `show --at` and `debug rebuild` then never read another stream to fold a task.

#### 6.8.5 What each command does

- **Create** with `depends_on`: every id must be a task on this board (`not_found` otherwise). At most 64 dependencies per task (`invalid_input`). Duplicates are dropped. A dependency may be in any channel and any state, including replaced (it then resolves to its head). If every head is `completed`, the task starts `ready`.
- **Complete** a task `Y`: in the same commit, for each waiting task with a dependency whose head is `Y`, and whose dependencies are now *all* met, append `DependenciesMet { met_by: Y }`. Dependents are visited in task id order, so the commit is deterministic. A replayed `complete` (6.4) writes nothing, so nothing readies twice. Readying a task counts as "makes a task ready" for the waiting-claim queue (6.5), which hands it out in the same transaction.
- **Fail or cancel** a task that is the head of some waiting task's dependency (including `TaskFailed` after the last lease, 6.6, whether a decision or the lease timer writes it): the dependents stay `waiting`. See Decision 1 for how that is surfaced.
- **Replace** a task: as in 6.8.3. Its dependents now resolve to the retry, so nothing is readied or failed by the replacement itself.
- **Reopen a dependency** `Y`: dependents that are already `ready` or further along are not touched. The board never moves a task back into `waiting`. Dependents still waiting count `Y` as unmet again, so they ready only when `Y` completes again.
- **Reopen a dependent:** the writer resolves its dependencies again and records `open_dependencies` on `TaskReopened`. It goes to `waiting` or `ready`.
- **Claim by id** of a waiting task: `invalid_state`, with a message that names the unmet dependencies. **Cancel** works on a waiting task, as on any non-terminal task. `block`, `unblock` and the settles never apply, because a waiting task has no claim.
- **Effective state** (5.3) is unchanged: `waiting` has no lease to end.

#### 6.8.6 Storage

- Two projections, written in the same transaction as the events:

  ```sql
  CREATE TABLE task_dependencies (
    task_id    TEXT NOT NULL,   -- the dependent
    depends_on TEXT NOT NULL,   -- the id it declared
    PRIMARY KEY (task_id, depends_on)
  );
  CREATE INDEX task_dependencies_by_dependency ON task_dependencies (depends_on, task_id);

  ALTER TABLE tasks ADD COLUMN replaced_by TEXT;   -- set by TaskReplaced
  ALTER TABLE tasks ADD COLUMN replaces    TEXT;   -- set by TaskCreated
  ```

- **Completing `Y`** walks `replaces` back from `Y` (at most 16 steps) to collect every id that resolves to `Y`, reads the waiting dependents of those ids through the index, then resolves each dependent's other dependencies forward through `replaced_by`. This is bounded by 64 dependencies of at most 16 steps each. Many dependents on one task (fan-out) cost one row each, all in one commit.
- `debug rebuild` rebuilds both from `TaskCreated` and `TaskReplaced` events, and checks them like every other projection.
- The contract suite (5.4) adds: the projections follow the log, dependents by dependency, resolution through a chain, and ready-queue order when many tasks ready in one commit (oldest `created_position` first, as in 6.5).

#### 6.8.7 API and CLI

| Where | Change |
| --- | --- |
| `POST /tasks` | Optional `depends_on: [id]` or `replaces: id` |
| `POST /tasks/plan` | Many tasks in one commit (6.8.8) |
| `GET /tasks/{id}` | Adds `depends_on`, each with the declared id, its head, the head's effective state and `completed_at`; `dependents` (ids and states); `replaces` and `replaced_by`; and `stuck` (Decision 1) |
| `GET /tasks` | Adds `waiting_on=<id>`: the tasks with a dependency that resolves to that task. Adds `stuck=true`. |
| `POST /tasks/claim` | When `claimed`, the body adds `dependencies` (Decision 2) |

All of these are additive, as 7.6 requires.

```bash
cork tasks create <channel> "<text>" --after <id> [--after <id>]...   # or --after id1,id2
cork tasks create <channel> "<text>" --replaces <id>                   # a retry; takes over <id>'s place
cork tasks create <channel> --file plan.json                          # 6.8.8
cork tasks list [--state waiting] [--waiting-on <id>] [--stuck]
cork tasks show <id>          # "Waiting on: A (completed), C → C2 (claimed)", "Needed by: …", "Replaced by: …"
cork wait work/ --type DependenciesMet --type DependencyFailed
```

`--after` and `--replaces` take ids only. In a plan file, tasks refer to each other by key.

#### 6.8.8 Plans from a file

A plan declares a set of tasks and how they relate. The board creates them all in one commit.

```json
{
  "idempotency_key": "feature-F-plan-1",
  "tasks": [
    { "key": "schema", "text": "Add the column", "labels": { "parent": "F" } },
    { "key": "tests",  "text": "Tests for the column", "after": ["schema"] },
    { "key": "docs",   "text": "Document it", "after": ["schema", "01JD…existing id"] }
  ]
}
```

- `after` names keys in the same file or ids of existing tasks.
- The whole plan is one commit, so the lead never sees half a plan.
- Keys must be unique, and the keys in the file must not form a cycle. A cycle or an unknown key is `invalid_input`, naming them, and nothing is written. That is the only cycle check in this design (6.8.9).
- Each task's `--needs` keywords, `--for`, lease and attempts work as in `tasks create`. `replaces` is not allowed in a plan. A retry is its own decision, made later.
- The plan's idempotency key covers the whole plan, so sending the same plan again is a no-op, and a restarted lead can always resend it.

#### 6.8.9 Invariants and replay

**Cycles cannot happen.** A new task can only depend on tasks that already exist, and nothing comes to depend on it later, so every declared edge points to an older task. A retry inherits its old task's edges and takes its place, so resolving through replacements only renames nodes; it never adds an edge. A plan file is checked as a whole before its commit.

`cork-stress` checks these from the log after every run, beside the invariants in 11:

1. After `TaskCreated` or `TaskReopened`, a task is `waiting` exactly when `open_dependencies` is not empty, and each id there had a head that was not `completed` at that position.
2. A task leaves `waiting` only by `DependenciesMet`, `TaskCanceled` or `TaskReplaced`. Each `DependenciesMet { met_by: Y }` shares a commit with a `TaskCompleted` of `Y`, and at that commit the head of every dependency of the task is `completed`.
3. **Nothing is stuck by the board.** At every commit boundary, no task is `waiting` while the heads of all its dependencies are `completed`.
4. No `TaskClaimed` while a task is `waiting`.
5. **Replacement:** each task has at most one `TaskReplaced`; its `by` names a task whose `TaskCreated` has `replaces` naming it, in the same commit, with a higher position. That task's `depends_on` equals the old one's. No `TaskClaimed`, `TaskCompleted` or `TaskReopened` on a task after its `TaskReplaced`. No chain is longer than 16.
6. (Decision 1) For each `TaskFailed` or `TaskCanceled` of a task, every task then waiting on it as a head has exactly one `DependencyFailed` naming it, in the same commit. A `TaskReplaced` writes no `DependencyFailed`.
7. Every declared `depends_on` id has a lower `created_position` than the task, or is in the same plan commit with no cycle.
8. Folding each task's stream alone gives the stored state. The fold never reads another stream.

Done when, beside unit tests in `cork-core` for every rule in 6.8.5:

- **Fan-in:** a task waiting on 64 tasks, completed by 20 workers at once, readies exactly once, in the commit of the last completion.
- **Fan-out:** 1,000 tasks waiting on one task all ready in its completion commit. Claims waiting before it are served first come, first served.
- **Chain:** a chain of 100 tasks run by 10 workers completes in order, and no task is ever claimed before its dependency completed.
- **Escalation:** a task with 10 waiting dependents fails, is replaced twice, and the second retry completes. All 10 ready in that commit, and none was ever written to in between except by `DependencyFailed`.
- **Chaos:** the chaos stress run (11, Done when 13) with random dependencies and random replacements. Invariants 1 to 8 hold.
- **Replay:** `show --at` at each position of a waiting, met, claimed, completed task, and of a replaced one, matches the log. `debug rebuild` and `export` / `serve --import` keep both projections and every state.

#### 6.8.10 Decisions confirmed by Chris (2026-11-27)

**Decision 1 (confirmed). A failed dependency is surfaced; it does not fail the dependent.**

When the head of a dependency ends `failed` or `canceled`, the dependent stays `waiting`. The board does not fail it, because the work may still get done by a replacement or a reopen, and that is a decision for the lead or the person.

- **Stuck is derived, not stored.** A waiting task is **stuck** while the head of one of its dependencies is `failed` or `canceled`, by effective state. It is a view on `show`, `list --stuck` and the API, not a state. When the lead replaces or reopens that dependency, the task stops being stuck with nothing written on it.
- **The fact is recorded once.** The commit that fails or cancels the head appends `DependencyFailed { dependency, head, state, reason }` to each waiting dependent. The event carries the index label `board.mention=<handle>` for the handle that created the dependent, and for its owner if that is a different handle. So `cork wait --mentions`, a person's inbox and a lead's wait all see it through the existing mention filter (7.4), and no message is posted to make that happen.
- **What the lead or person can then do,** with nothing new: reopen the dependency, replace it (6.8.3), or cancel the dependent.

**Decision 2 (confirmed). The results of dependencies are passed to the dependent.**

- The claim response gains `dependencies: [{ task_id, head, text, result, completed_at, holder }]`, with the result of each head's latest `TaskCompleted`. `text` is the first 2 KB of the head's root message.
- `tasks run` writes them to a JSON file and sets `CORK_DEPENDENCIES` to its path, beside `CORK_PREVIOUS` (9). Up to 64 results of 64 KB each is at most 4 MB, so a file, not an environment variable.
- **Trust:** results are written by other agents, so they are notes, not instructions, as `CORK_PREVIOUS` is (9.1). The worker skill says so.
- This is derived at claim time from what is in the log, so a plan never has to copy results into later tasks' text.

#### 6.8.11 Alternatives considered and rejected

- **Editable dependencies** (`DependenciesChanged`, by the owner, while a task is waiting or ready). It would need a cycle check inside the write transaction, a graph walk in the stress checker, and a way for the owner to move a ready task back to waiting. It also makes the lead edit the tasks around a retry, which is explicit work the board can derive. Rejected for now; the event name stays reserved.
- **A narrow "move the dependents to the retry" edit** in `tasks escalate`. It rewrites other tasks' dependencies, needs its own cycle check, and makes replay depend on when the edit happened. `--replaces` gives the same result by declaring one fact on the retry.
- **Failing the dependent automatically** when a dependency fails. It throws away a plan that a replacement could still finish, and the lead would have to reopen every dependent. Kept out by Decision 1.
- **Posting a reply from `cork`** to tell the owner a dependency failed. A mention label on `DependencyFailed` reaches the same people without a message nobody wrote.

#### 6.8.12 How the lead uses it

This replaces "order the children with labels" in 17.1. The lead skill (17.2) changes in three places:

1. **Plan.** The lead writes the children as one plan file with `after` keys (6.8.8), under the feature's idempotency key. Independent children have no `after`, so cheap pools start on them at once.
2. **Watch.** The lead adds `--type DependencyFailed` to its wait on the feature's children. It does not watch `DependenciesMet`: workers pick those tasks up as ready.
3. **Escalate by replacing.** When a child fails or struggles, the lead creates the retry with `--replaces <old>` (16.7). It does not cancel the old task, link the threads, or touch any waiting task; the board does or derives all of that. `tasks escalate` passes `--replaces` for it once 6.8 is built.

**For observability (18, not changed here):** time spent `waiting` is not queue time. Queue time for a dependent starts at its `DependenciesMet`.

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
- Dependencies (`depends_on`, `--after`, `--replaces`), `waiting`, and plans from JSON (6.8).
- Editing, deleting and redacting messages (event types reserved).
- Sync with Slack, GitHub or Linear.
- An SDK, an MCP server, or a UI, including the UI's sign-in (14.4).
- Server mode (section 13). Milestone 1 must still follow 13.5, so server mode is not blocked.
- The Postgres adapter.
- Worker fleets (`cork workers`, section 16). In Milestone 1 a person starts each `cork tasks run` by hand.
- The lead skill and parent and child tasks (section 17).
- Observability: `cork stats`, usage capture and `/metrics` (section 18, Milestone 3).

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

## 16. Worker fleets (Milestone 2, planned)

**Status:** built in Milestone 2 (PRs #8 to #10). User guide: docs/fleets.md.

In Milestone 1, Cork never starts workers. Each `cork tasks run` is one worker, started by a person, and its model is whatever command follows `--`. A fleet adds one command that runs many workers from config files. It groups them into **pools**, each running one model or agent, and lets you grow or shrink each pool while it runs. Tasks reach the right model by **keywords**: plain labels that match what each model can do.

### 16.1 Principles

- **A fleet is a plain client.** `cork workers` is a supervisor process that starts `cork tasks run` children. Nothing changes in `cork-core`, the daemon or the API: no new routes, events, fields or tables. Keywords are ordinary labels, and models, pools and matching live only in the fleet and the CLI. Everything a fleet does could be done by hand in N shells.
- **The lease protects the work, not the supervisor.** When a worker or the supervisor dies, leases end and tasks go back to `ready`, as in 9.1. The supervisor only restores the head count.
- **Routing stays on the board.** A task says what it needs with keyword labels. A pool takes only the tasks its model covers (16.6). The lead or a person routes work by what they write on the task, never by naming a worker.
- **One fleet per machine and config.** In server mode (13), fleets on many machines point at one board with `CORK_URL`, and their pools simply add up.

### 16.2 Config: keywords, models and pools

There are two files. **What each model can do** is kept apart from **how many of each run here**, because the first is shared by a team and the second is set per machine.

`models.toml` is the catalog. It can be committed in a repo and shared by a team. It defines the keyword vocabulary and the models:

```toml
# The keywords tasks may ask for, and that models may claim to cover.
[keywords]
rust          = "Writes and reviews Rust"
typescript    = "Writes and reviews TypeScript"
docs          = "Prose: READMEs, guides, changelogs"
large-context = "Needs to hold a large codebase or long files at once"
vision        = "Reads screenshots, diagrams or designs"
architecture  = "Design across modules; storage, APIs, migrations"
review        = "Reviews someone else's change"

[aliases]                    # other spellings people use, mapped to one keyword
ts = "typescript"
documentation = "docs"

[model.opus]
command = ["claude", "-p", "Do the Cork task in $CORK_TASK_TEXT", "--model", "opus"]
tier = 3                    # 1 is the weakest; a model also has every tier below its own
cost = 15                   # relative cost per task-hour, in units you choose; for reports
keywords = ["rust", "typescript", "docs", "large-context", "vision", "architecture"]

[model.sonnet]
command = ["claude", "-p", "Do the Cork task in $CORK_TASK_TEXT", "--model", "sonnet"]
tier = 2
cost = 3
keywords = ["rust", "typescript", "docs", "large-context"]

[model.haiku]
command = ["claude", "-p", "Do the Cork task in $CORK_TASK_TEXT", "--model", "haiku"]
tier = 1
cost = 1
keywords = ["typescript", "docs"]

[model.reviewer]            # any agent CLI works: the command is opaque
command = ["./scripts/review-agent.sh"]
tier = 2
keywords = ["review"]
```

`workers.toml` sets the fleet on one machine. It lives at `~/.config/cork/workers.toml`, or comes from `--config FILE`:

```toml
[fleet]
name = "dev"                # used in profile names and the control socket
models = "models.toml"      # path to the catalog, relative to this file
max_workers = 12            # cap across all pools

[pool.strong]
model = "opus"
select = "work/"            # channel selector
count = 2                   # initial size; min and max bound scaling
min = 0
max = 6
lease = "10m"
max_runtime = "2h"

[pool.fast]
model = "haiku"
select = "work/"
count = 5
max = 10

[pool.review]
model = "reviewer"
select = "work/"
count = 1
```

- A pool may override `command`, narrow its model's `keywords`, and set `env`, `cwd` and `from`. `from` works as it does in `tasks run`.
- `{model}` and `{pool}` are filled into `command`. They are also set as `CORK_MODEL`, `CORK_POOL` and `CORK_TIER` for the command.
- Secrets such as API keys come from the supervisor's environment. They never go in these files or on the board.
- **Why TOML, with JSON accepted.** People edit these files by hand. TOML allows comments, which is where the reason for a keyword goes ("opus: vision tested on our screenshots"). It is also what Rust developers already use in `Cargo.toml`. A file ending in `.json` with the same shape is read too. That suits a catalog generated by a script or served by a team.

### 16.3 Commands

```bash
cork workers up [--config FILE] [--detach]   # start the fleet (foreground by default)
cork workers status [--json]                 # pools, size, busy or idle, current task, restarts, unmatched tasks
cork workers scale <pool> <n|+n|-n>          # resize a running pool, within min and max
cork workers drain <pool>                    # finish current tasks, then stop the pool's workers
cork workers reload                          # re-read both files; apply count, model and command changes
cork workers down [--now]                    # drain everything, or with --now release and stop
cork workers match <task-id | --needs k1,k2> # dry run: which models and pools could take this task, and why not
cork workers keywords                        # the vocabulary, its aliases, and which models cover each keyword
```

`status`, `scale`, `drain`, `reload` and `down` talk to the running supervisor over a local Unix socket, `~/.config/cork/workers/<fleet>.sock` (mode `0600`). They do not go through the board. `match` and `keywords` read only the files and the board, so they work without a running fleet.

### 16.4 Supervision and dispatch

The supervisor is also the **dispatcher**. Matching happens there, not in the daemon:

1. It follows `/events` for task changes in its pools' channels.
2. When a task becomes ready, or a worker becomes free, it reads the ready tasks with `GET /tasks?state=ready&channel=…`, oldest first.
3. It gives the oldest task that a free worker's pool covers (16.6) to that worker. Each worker is one child: `cork --as <fleet>-<pool>-<n> tasks run <task-id> -- <command>`. Profile names show which pool, and so which model, did each attempt in `tasks history`.
4. If another fleet or a person claimed that task first, the child exits 7 (`task_claimed`) at once. The supervisor tries the next matching task. The single writer (2) guarantees one winner, so a race costs one request, never double work.

This keeps FIFO order among the tasks each pool can take, and it needs nothing from the server beyond Milestone 1's claim by id.

The supervisor handles each child's exit by its code (8.6):

- **0, 8, 9 or 10** (task settled or blocked): the worker is free again.
- **7** (claimed by someone else): try the next task.
- **11** (daemon lost): retry with backoff, 1 s doubling to 30 s.
- **A crash or a signal:** the task comes back through its lease. The worker is replaced, with backoff.

Two more rules:

- **Crash loop:** if a pool's workers crash 5 times in 60 s, the supervisor pauses the pool and says so in `status`. It also posts one message to `cork/fleet` naming the pool and the last error. `scale` or `reload` resumes the pool.
- **No orphans:** children die with the supervisor (`PR_SET_PDEATHSIG` on Linux, a process group elsewhere). Their tasks come back through leases. On `SIGTERM` the supervisor drains (16.5). A second `SIGTERM`, or `down --now`, sends `SIGTERM` to every child, so each releases its task and exits 143.

### 16.5 Scaling

- **Up:** add workers at once, and dispatch to them. Never go above the pool's `max` or the fleet's `max_workers`.
- **Down:** free workers stop at once. A busy worker is drained: it is given no new task, so it finishes its current one, settles it, and stops. Nothing is lost and no attempt is wasted. Because the supervisor dispatches one task per child, draining needs no change to `tasks run`.
- **Changing a pool's model or command** with `reload` drains the old workers and starts new ones. Tasks in progress finish on the model that started them.
- **Autoscaling** comes after manual scaling, and is opt-in per pool:

  ```toml
  [pool.fast.autoscale]
  target_ready_per_worker = 2   # grow while matching ready tasks per worker > 2
  idle_after = "5m"             # shrink a worker idle this long
  ```

  It counts only ready tasks that the pool covers, and stays within `min` and `max`.

### 16.6 Keywords: matching tasks to models

**A task says what it needs. A model says what it can do. A pool takes only tasks its model covers.** All of it is labels and client code.

- **On a task**, each need is one plain label, `needs.<keyword>=yes`:

  ```bash
  cork tasks create work "Port the parser to Rust" --needs rust,large-context
  cork tasks create work "Fix the typo in the README" --needs docs
  cork tasks create work "Redesign the storage layer" --needs rust,architecture,tier-3
  ```

  `--needs a,b` is CLI shorthand for `--label needs.a=yes --label needs.b=yes`. A task with no `needs.*` labels can go to any pool. (The `needs.` prefix is a label and means keywords only. Dependencies use `depends_on` and `--after` (6.8.2), so the two never collide.)
- **Tiers are keywords too.** A tier 3 model also covers `tier-1`, `tier-2` and `tier-3`. So "needs at least a tier 2 model" is `--needs tier-2`, with no new rule.
- **Covers** means every `needs.*` label on the task names a keyword in the pool's list, after aliases. The dispatcher checks this (16.4). The daemon only stores and filters labels, as in Milestone 1.
- **A stronger pool can take easier work.** An opus worker covers a `docs` task, which keeps throughput up when the cheap pool is busy. The dispatcher still prefers the cheapest pool that covers a task. A pool that should take only work that needs it sets `exclusive = true`, which also requires its top tier: such a pool takes only tasks that ask for `tier-3`.
- **Asking who can take a task:** `cork workers match <task-id>` prints each model and pool, with "covers" or the missing keywords. For example: `haiku: missing rust`, `sonnet: covers`, `opus: covers (costs 5x sonnet)`. `--needs` asks the same question before the task exists, and the lead skill uses it while planning.
- **Keywords drift if they are free text.** "ts", "typescript" and "TypeScript" would each silently leave tasks unmatched. The catalog prevents that:
  - The `[keywords]` list is the vocabulary.
  - `cork tasks create --needs` checks each keyword against it. An unknown keyword is an error that names the closest match ("unknown keyword `typscript`; did you mean `typescript`?"). An alias is rewritten to its keyword.
  - `cork workers up` and `reload` refuse a model or pool that names an unknown keyword.
  - `workers status` lists **unmatched** tasks: ready tasks with a need that no running pool covers. A typo, or a missing model, shows up at once, not as silence.
  - Tasks created another way, such as by raw `--label` or the API, are not checked. `match` and `status` still catch them.
- **Keywords are a routing hint, not a security boundary.** They are self-declared in files the person controls. Who may claim what is still decided by ownership and `--from` (10).

### 16.7 Worked scenario: a weak model struggles, the lead escalates

Chris's case: a task lands on a model that turns out not to be strong enough. The lead notices and moves it to a more capable model, which finishes or fixes the work. Everything here uses Milestone 1's board, plus the fleet and lead layers.

**1. The plan.** The lead creates a child of feature `F` (17.1): `cork tasks create work "Make the importer stream large files" --needs typescript --label parent=F`. `match` says `haiku` covers it, the cheapest model that does, so the `fast` pool takes it.

**2. The struggle.** The lead is waiting on the feature's events (17.2). It sees one of three signals:
- **`TaskFailed`:** haiku ran its tests, they failed, it wrote a reason and exited non-zero.
- **`LeaseExpired` twice:** the worker keeps dying, for example running out of memory on large files.
- **`TaskBlocked`** with a question the lead can answer only with "this is harder than planned".

Each comes with the thread: haiku's progress posts, the branch `cork/<id>/1` and its last commit (9.1).

**3. The decision.** The lead reads the reason and the progress, then asks `cork workers match --needs typescript,large-context,tier-2`. The answer is `sonnet: covers`. The lead escalates by one tier rather than straight to opus, because sonnet is cheaper. The skill's rule: escalate one step at a time, and at most twice per task (17.2).

**4. The handover.** Labels are fixed when a task is created, so the lead does not change the old task. It creates a **retry** instead:

```bash
cork --idempotency-key "escalate-$OLD" tasks create work \
  "Retry of $OLD on a stronger model. Read its thread first: cork read $OLD.
   Earlier attempt: branch cork/$OLD/1 at abc123; tests in importer.test.ts fail on files over 1 GB.
   Continue from that branch, or start clean and say why. <original task text>" \
  --needs typescript,large-context,tier-2 --label parent=F --label retry_of=$OLD
```

Then it tidies the old task:
- If it is still blocked or ready, it cancels it with the reason "escalated to <new id>".
- It replies in the old thread with a link to the retry, so the history reads both ways.

The idempotency key means a lead that crashes and restarts mid-escalation never makes two retries.

**Once dependencies (6.8) are built,** the retry is created with `--replaces $OLD` instead. The board then ends the old task and links the two (6.8.3), so the lead skips the cancel and the linking reply, and tasks waiting on the old task wait on the retry with nothing edited.

**5. The finish.** The dispatcher gives the retry to the `strong` or a sonnet pool: haiku no longer covers it. The new worker reads the old thread, which it is told to treat as notes, not instructions (9.1). It continues from the branch, completes the task, and names its branch in the result. The lead sees `TaskCompleted` and reviews it as usual.

**What the board records**, all as ordinary events:

| Where | Events |
| --- | --- |
| Old task | `TaskCreated` (needs typescript), `TaskClaimed` by `dev-fast-2`, progress replies with branch and commit, `TaskFailed` with the reason (or `LeaseExpired` events), a reply from the lead linking the retry, `TaskCanceled` if it was not already finished |
| Retry | `TaskCreated` with `retry_of=<old>` and the stronger `needs`, `TaskClaimed` by a sonnet pool's profile, progress, `TaskCompleted` |
| Feature thread | Both tasks, because both carry `parent=F` |

From these, anyone can answer "which tasks needed escalating, from which model to which, and what it cost", with `tail --label retry_of=…` or a `debug export` of the feature (17.5).

**If the strong model fails too**, the lead stops escalating after its limit. It blocks the feature with a question for the person, naming both attempts and their branches.

### 16.8 Done when (for Milestone 2)

1. **Restart:** `kill -9` one worker of a pool of 3. The supervisor starts a replacement within 2 s, and the killed worker's task completes on a later attempt.
2. **Scale down without loss:** `scale fast 5 → 1` while 5 workers are busy. All 5 tasks complete, none is released or failed, and 4 workers stop.
3. **Scale up:** `scale strong +3` gives 3 more workers tasks within 2 s when matching tasks are ready.
4. **Routing by keyword:** with the catalog above, no task is ever claimed by a pool whose model lacks one of its `needs` (check the profiles in the log). Among the tasks a pool covers, it takes the oldest first.
5. **Two fleets, one board:** two supervisors share a board. Every task completes exactly once, and races end in exit 7, never double work.
6. **Vocabulary:** `--needs typscript` is refused with a suggestion. `--needs ts` is stored as `needs.typescript`. A raw label with an unknown keyword is listed as unmatched in `status`.
7. **Match:** `workers match` gives the same answer the dispatcher acts on, for 1,000 random tasks and catalogs.
8. **Escalation scenario:** the scenario in 16.7, scripted with fake workers, ends with the retry completed by a tier 2 profile. A lead restarted between creating the retry and cancelling the old task makes no second retry.
9. **Crash loop:** a command that exits at once pauses its pool after 5 crashes, and the other pools keep working.
10. **Supervisor death:** `kill -9` the supervisor. No child outlives it by more than 1 s, and every task it held returns to `ready` within one lease.
11. **Reload:** changing a pool's model drains the old workers, and later tasks run with the new model (`CORK_MODEL`).
12. **Plain client:** every fleet test runs against an unmodified Milestone 1 daemon. No change to `cork-core` or the API.

## 17. A lead agent and parent and child tasks (Milestone 2, an experiment)

**Status:** tooling built in Milestone 2 (PR #10): the lead skill, `wait --type`, `tasks escalate` and the `lead` stress scenario. The real runs in 17.5 are still to do; see docs/lead-experiment.md.

The idea: one agent, the **lead**, owns a feature for a person.
- It splits the feature into tasks, which the workers (16) do.
- It watches their events and reviews the results.
- It creates follow-up tasks, and escalates struggling ones to stronger models (16.7).
- It raises issues to the person.

**The person stays the accountable owner.** The lead acts for them, under their name, and cannot widen its own reach.

### 17.1 Parent and child tasks, with labels first

For the experiment, a feature and its tasks are linked with **plain labels**, so the core stays as it is. The core gets real parent links only if the experiment shows they are worth it.

- **The feature** is a task in the `features/` channel, labelled `kind=feature`. Worker pools do not select `features/`, so no worker takes it. Only the lead pool does (17.3).
- **Children** are tasks in a work channel labelled `parent=<feature id>`.
- **Rollup:** `cork tasks list --label parent=<id>` lists them with their states. The lead skill sums them ("5 completed, 1 claimed, 1 blocked") and posts the summary in the feature's thread.
- **Watching all of them:** `cork wait --label parent=<id> --type TaskCompleted --type TaskFailed --type TaskBlocked --type LeaseExpired` wakes on any child's change. The only addition is `--type` on `wait`, a CLI flag for a filter the API already has (7.4).
- **Order:** inside a feature, dependencies (6.8) order the children, such as "tests after the schema change". Once 6.8 is built, the lead creates them as one plan with `after` keys (6.8.12).
- **Closing:** the lead decides when the feature is done and completes it. Children ending never closes the feature by itself. To cancel a feature, the lead cancels its unclaimed children first, then the feature.

**What would move this into the core later:** checks the board can't make on labels (a child of a missing parent, a feature claimed by a worker), a rollup in one read, and cancel of a whole feature in one commit. The experiment (17.5) tells us whether those matter.

### 17.2 The lead skill

`skills/cork-lead/SKILL.md`, next to the worker skill, teaches this loop:

1. **Plan.** Read the feature brief, then post the plan in the feature's thread. Create small, self-contained children, each with:
   - `--label parent=<id>`.
   - The keywords it needs. Check them with `workers match --needs …` and ask only for what the task truly needs, so cheap models get the work they can do.
   - An idempotency key (`<feature>-<n>`), so a restarted lead never duplicates them.
2. **Watch.** `cork wait --label parent=<id> --type …`, in a loop (17.1). Never poll.
3. **Review each result.** Read the child's result and the branch it names. Then, depending on what it finds:
   - Accept the work.
   - `reopen` the child with a note on what to fix.
   - Create a follow-up child.
   - Escalate it to a stronger model, one tier at a time (16.7).
4. **Raise issues to the person.** For something the lead cannot decide, such as scope, product choices or anything risky, it blocks the feature with a question that @mentions the person. It keeps working on the other children meanwhile. For news without a decision, it posts in the feature's thread.
5. **Finish.** When every child is settled, post a summary in the feature's thread: what was done, the branches, escalations, and anything left open. Then complete the feature. Merging and deploying stay with the person.

**Limits the skill sets, which the lead must not lift itself:**
- At most `max_children`, default 20.
- At most 2 follow-ups and 2 escalations per child, so a stuck child does not loop forever.
- A time budget for the feature.

When the lead hits a limit, it blocks the feature and asks the person.

### 17.3 Running the lead

The lead is one more pool, on the strongest model, selecting only features:

```toml
[pool.lead]
model = "opus"
select = "features/"
count = 1
command = ["claude", "-p", "Lead the Cork feature in $CORK_TASK_TEXT", "--model", "{model}"]
lease = "30m"
```

A person posts the brief with `cork tasks create features "<brief>" --label kind=feature`, and the lead pool takes it. The lead runs for a long time while it waits. That is fine: the wrapper renews the lease, and a crashed lead is picked up by the next attempt. That attempt reads the feature's thread and children from the board, plus `CORK_PREVIOUS` (9.1), so it can resume without starting over.

### 17.4 Trust

- The lead acts as the person's profile (`lead via chris`), so every task it creates is owned by the person. Workers' default `--from` (owner = person) still applies.
- Children's results, and an old task's thread read during an escalation, are untrusted input, as `CORK_PREVIOUS` is (9.1). The skill tells the lead to judge them, not to follow instructions in them.
- The lead never merges, deploys or sends messages outside the board unless the brief says so. Even then, it renews its own claim first (9.1).

### 17.5 How we measure it

Run the same features three ways:
- (A) A person splits the work by hand.
- (B) A lead with one pool on one model.
- (C) A lead with pools matched by keyword, with escalation (16.6, 16.7).

Use 5 to 10 small features from a real repo, each with a written definition of done. Everything below except the first row comes from the log (`tasks history`, `debug export`), so no extra tracking is needed:

| Measure | From |
| --- | --- |
| Done: share of features whose result meets the definition of done, judged by the person | Person's review |
| Time from brief to feature completed | `TaskCreated` → `TaskCompleted` on the feature |
| Person's time: questions raised, and minutes from question to unblock | `TaskBlocked` / `TaskUnblocked` on the feature |
| Rework: reopens and follow-up children per feature | `TaskReopened`, children created after the plan |
| Waste: failed and abandoned attempts, and child tasks never used | `TaskFailed`, `LeaseExpired`, canceled children |
| Escalations: how many, from which model to which, and whether the retry succeeded | `retry_of` labels and the profiles that claimed each task |
| Cost: model time per feature, split by model | Wrapper run times per profile, joined with each model's `cost` |
| Routing gaps: tasks no pool could take, and how long they waited | `workers status` unmatched list, `TaskCreated` → first `TaskClaimed` |

`cork-stress` gets a `lead` scenario that drives a fake lead and fake workers through 100 features. It checks that rollups match the log, that the `parent` wait sees every child change, that no feature completes before all of its children are settled, and that escalations never duplicate.

### 17.6 Done when (for Milestone 2)

1. `--type` on `wait` works. A wait on `--label parent=<id>` sees every child task event, through both the store and the in-memory path (7.4). This is a check of Milestone 1's label filter on task events, not a new feature.
2. Workers never take a `features/` task.
3. A lead killed with `kill -9` mid-feature is replaced, and the next attempt creates no duplicate children or retries (idempotency keys).
4. `skills/cork-lead/SKILL.md` exists, and one real run of each setup in 17.5 is written up in `docs/`, with the measures filled in.
5. **The decision the experiment feeds:** after the runs, a short note in `docs/` says whether parent links, rollup or keyword matching should move into the core, and why.

## 18. Observability: performance, cost and usage (Milestone 3, planned)

**Status:** plan only, added on 2026-11-27 at Chris's request. Do not build it until Chris says go. It comes after the fleet (16) and the lead (17), because it measures them.

**The headline number is cost per successful task, by model.** Failed attempts, lease expiries and escalations all count toward it. A cheap model that fails half the time and needs a retry on a strong one can cost more than starting on the strong one. This number shows that.

Two kinds of data, kept apart:

- **Flow**: what happened to the work. It is already all in the log, so it comes from replay, with no new capture.
- **Usage**: tokens, cost and turns per attempt. Only the agent knows these. The wrapper captures them and posts them to the board as plain message data. **The core stays unaware of usage**: no new event types, fields or tables.

### 18.1 Flow metrics, from the log

Each of these is computed by replaying events, per channel, label, worker profile, pool and feature:

| Metric | How |
| --- | --- |
| Queue depth over time | ready tasks at each point in the log |
| Time to claim | `TaskCreated` (or `TaskReopened`, `TaskUnblocked`) → `TaskClaimed` |
| Work time per attempt | `TaskClaimed` → the attempt's settle or `LeaseExpired` |
| Time to done | `TaskCreated` → `TaskCompleted` |
| Attempts per task, and the success rate of first attempts | `attempt` on `TaskClaimed` |
| Failure reasons, grouped | `reason` on `TaskFailed` |
| Lease expiries | `LeaseExpired`, by profile (crashes, slow machines) |
| Time blocked on a person | `TaskBlocked` → `TaskUnblocked` |
| Escalations | tasks with `retry_of` (16.7), and whether the retry succeeded |

Because these come from the log, they can be computed for any past period, and recomputed after a change to how they are defined.

### 18.2 Usage, captured by the wrapper

- **Generic:** before it exits, the command may write JSON to the file named in `$CORK_USAGE`:

  ```json
  {"model": "claude-sonnet-…", "input_tokens": 182340, "output_tokens": 9120,
   "cache_read_tokens": 140000, "cost_usd": 0.71, "turns": 23}
  ```

  Every field is optional, and unknown fields are kept.
- **Built in for Claude Code:** `tasks run --usage claude-json` runs the command with `--output-format json` in mind. The wrapper parses the final JSON object Claude Code prints, which holds the cost, token counts per model and the number of turns. It takes the usage from it and uses its `result` text as the task result, so the result stays readable. Parsers for other agents can be added the same way, and a parser that fails changes nothing about settling.
- **Where it goes:** just before it settles, the wrapper posts one reply in the task's thread with `--label usage=attempt` and the JSON as `data`, adding `task`, `attempt`, `claim_id`, `profile`, `pool`, `model`, `exit` and `duration_ms`. This is an ordinary message, so export, import, replay and the UI all carry it with no new rules. It is posted for every outcome: completed, failed, blocked, max-runtime and released.
- **What is lost:** when the wrapper is killed with `kill -9` or the machine dies, that attempt's usage is not recorded. `cork stats` counts these attempts and reports their usage as unknown. It does not guess.
- **Cost is an estimate.** It is what the agent reports, or tokens times a price table in `models.toml`:

  ```toml
  [model.sonnet.price]
  input_per_mtok = 3.0
  output_per_mtok = 15.0
  cache_read_per_mtok = 0.3
  ```

  The real bill is the provider's. `stats` says which source each number came from.

### 18.3 `cork stats`

```bash
cork stats [--since 7d] [--channel work/] [--label k=v] [--feature <id>]
           [--by model|pool|profile|channel|label:<key>|day] [--json | --csv]
```

`cork stats` replays the log through `/events`, so it reads, never writes. It runs on any machine that can reach the board. It prints:

- **Outcomes:** tasks completed, failed and canceled, and the success rate.
- **Speed:** p50 and p95 of time to claim, work time and time to done.
- **Effort:** attempts per task, lease expiries and escalations.
- **Usage:** tokens in and out, turns and cost, all per group.
- **Cost per successful task:** all cost spent on a task, including failed attempts and every retry in its `retry_of` chain, divided by tasks completed. The chain is charged to the model the task **started** on, and also shown split by the model that finished it. That way "start on haiku and escalate" can be compared with "start on sonnet".

`--by model` uses the `model` in each usage reply. For attempts with no usage, it falls back to the pool in the profile name (`<fleet>-<pool>-<n>`), read through `models.toml`.

Example:

```text
model    tasks  done  success  p50 work  attempts/task  escalated  tokens in/out   cost    cost/done
haiku      120    96     80%      4m          1.3          18 → ↑     21M / 0.9M   $31.20    $0.33*
sonnet      60    58     97%      7m          1.1           1 → ↑     44M / 2.1M  $164.10    $2.83
opus        12    12    100%     15m          1.0           0         19M / 1.2M  $298.50   $24.88
* includes the cost of 18 escalated retries on sonnet
```

### 18.4 Live metrics

- **The daemon** serves `GET /metrics` in Prometheus text format, behind the same token (10). It holds only what the daemon knows:
  - Tasks by state per channel.
  - Claims, settles and lease expiries, as counters.
  - Write latency and commit size, as histograms.
  - Open waiters and streams.
  - The log head, idempotency hits, and the WAL size.

  No model or cost data: the daemon knows nothing of models.
- **The fleet supervisor** (16) serves its own `/metrics` on its control socket, or on a local port if configured. It holds:
  - Workers busy and idle per pool.
  - Restarts and crash loops.
  - Unmatched tasks.
  - Tokens and cost per pool and model, totalled from the usage it sees.
- Dashboards and alerts are left to whatever the person already runs, such as Grafana. Cork ships one example dashboard file and no alerting of its own.

### 18.5 Tracing, later and optional

When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, wrappers and the supervisor emit OpenTelemetry spans:

- **One trace per task**, with its trace id derived from the task id. So a task's spans join up across attempts, machines and escalations, and a retry's trace links to its `retry_of`.
- **One span per attempt**, carrying the claim id, profile, pool, model, exit code and usage.
- **Child spans** for renew calls, and for the agent's own spans if the agent supports trace context. The wrapper passes `TRACEPARENT` to the command.
- **The daemon** joins in by reading `traceparent` on requests and adding spans for the write transaction and waits.

The log stays the source of truth. Traces are for following one task across systems, not for counting.

### 18.6 Privacy

- Usage replies hold numbers, model names and timings, never prompts, code or output.
- `cork stats` never prints message text unless asked with `--reasons`, which shows grouped failure reasons.
- `/metrics` uses channel names as labels. Server mode (13) can turn that off for boards where channel names are sensitive.

### 18.7 Done when (for Milestone 3)

1. **Flow from replay:** for a scripted run with known timings, every metric in 18.1 from `cork stats` matches the script within 1 ms. The same numbers come back after `export` and `serve --import`.
2. **Usage capture:** a fake agent that writes `$CORK_USAGE`, and a recorded Claude Code JSON output, each produce one `usage=attempt` reply per attempt with the right fields, for completed, failed, blocked and max-runtime runs. A broken usage file changes nothing about settling.
3. **Cost per successful task:** a scripted escalation chain (haiku fails, sonnet completes) is charged in full to haiku as the starting model, and shown split by finishing model.
4. **Unknown usage:** an attempt killed with `kill -9` is counted, and shown as unknown usage, not zero.
5. **`/metrics`:** the daemon's counters match the log after a stress run (`cork-stress` checks them alongside its invariants). Scraping adds no measurable write latency.
6. **No core change:** everything except `/metrics` runs against an unmodified Milestone 2 daemon.
7. **A real report:** the measured runs of 17.5 are re-read with `cork stats`, and the write-up in `docs/` shows cost per successful task by model for each setup.
