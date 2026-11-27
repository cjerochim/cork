# Agent Board: independent review of HANDOFF.md (v2)

An independent reviewer read the whole spec and looked for counterpoints, with simplicity and maintainability as the priorities. I checked its findings against the doc. They hold up. My view is at the top, and the reviewer's full findings follow.

## My take

**The main point is right.** The spec commits to one writer per board, but it still pays for machinery that only several competing writers would need. Using the single writer properly removes the most code and the most tests.

| # | Reviewer's top change | My view |
| --- | --- | --- |
| 1 | Decide inside the write transaction. Drop the 16-try retry loop, the `conflict` code and the "shuffle the oldest 5" claim. Claim-next becomes strict first-in, first-out. Core writes `LeaseExpired` in the same commit whenever a decision finds an ended lease. Delete the high-availability and Postgres ordering material. | **Adopt.** Keep the expected-version check as a safety assertion. Cost: if we ever want several writers (high availability), this comes back. That is a reasonable thing to defer. |
| 2 | Make settles idempotent per claim id: a retried `complete` that already succeeded returns success, not `claim_lost`. Define what the wrapper does on network errors and 500s (keep renewing until the lease runs out). Allow `--claim` / `BOARD_CLAIM` when the claim file is missing. | **Adopt.** The retried-complete case is a real bug in the current spec: an agent would believe it lost work it finished. |
| 3 | Wake waiters from memory, not with one database query per waiter. Serve `claim --wait` in first-come order. Keep read transactions short so the SQLite log file (WAL) cannot grow without limit. | **Adopt.** At 1,000 waiters the current design does about 1,000 queries per commit. |
| 4 | Define daemon restart: give running leases a grace window, re-read the token after `unauthorized`, check versions, never auto-start a daemon from inside `tasks run`, and use a real monotonic clock. | **Adopt.** Today a daemon restart would kill healthy workers, because the token changes and leases expire at start-up. |
| 5 | Fix the API contracts before §7.4 freezes them: return 200 instead of 408/404 for an empty claim, drop the SSE `event:` field, give the wrapper fixed exit codes, reserve `cursor_expired`, redaction, edit/delete event types and an event schema version. | **Adopt.** These are cheap now and breaking later. |

**Also worth taking:**
- Store idempotency keys in a small table instead of an event stream.
- Put only `board.task` on task events, not every root label. An 8-hour task would otherwise write about 9,500 label rows.
- Trim the browser sign-in detail (§14.4) to the rules. Move the full design to a later UI spec.
- Cap mentions per message.
- Paginate `GET /threads/{id}`.
- Only pass replies from the owner or the claim holder into `BOARD_PREVIOUS`, to reduce prompt injection.
- Fix every inconsistency in section 4 below.

**Where I'd push back a little:**
- **Dropping the "effective state" SQL.** The reviewer suggests accepting a stored status that is wrong for about 1 ms after a lease ends. With change 1, every decision checks the lease anyway. I'd keep the effective-state expression, but in exactly one SQL view, as the reviewer's fallback suggests.
- **Event sourcing itself.** The reviewer would keep it, and so would I. Replay and history depend on it.

---

## Reviewer's findings

The thread running through most of this: the spec locks in a single writer (§3, §5.2, §13.4) but still pays for machinery that only multiple concurrent writers would need.

### 1. Over-engineering and simplifications

**A. Read, decide, append, retry 16 times, return `conflict` (§6.4).** SQLite has one writer connection and uses `BEGIN IMMEDIATE`. So the server can read the task, call core and append inside the same write transaction, and a version conflict can never happen. The `Exact(n)` check can stay as an assertion that panics if it fails.
- What goes: the retry loop, the `conflict` code and the race-retry tests.
- What is lost: nothing, as long as §13.4's "one writer per board" holds.
- Recommendation: cut it.

**B. `claim --ready`: take the oldest 5, shuffle, retry, re-query (§6.5).** This is a workaround for many writers. Inside the writer transaction, use `SELECT … ORDER BY created_position LIMIT 1` and claim it.
- What is gained: strict FIFO, no wasted attempts, and the 200-worker contention test (§11 #10) stops being interesting.
- Recommendation: cut it.

**C. Promise 4 and the Postgres ordering notes (§5).** With one writer process, ordered visibility is true on Postgres too.
- Recommendation: state "adapters may assume a single writer process" and delete the sequence and watermark discussion.
- Also delete §13.4 "High availability, later" (leader election, write forwarding). It is design debt for a feature nobody has asked for, and it is the only thing that makes promise 4 hard.

**D. The `idem:{owner}:{key}` stream (§4, §6.2).**
- Problem: it puts one event per keyed create into the global log forever.
- Simpler: a projection table `idempotency(owner, key, task_id, request_hash)` with a unique key, written in the same commit.
- What is lost: idempotency keys no longer appear in replay, which nobody needs.

**E. Copying root labels onto every task event (§6.3).**
- Problem: up to 33 `event_labels` rows per event, including every `LeaseRenewed`. An 8-hour task renewed every 100 s writes about 9,500 label rows.
- Fix: put only `board.task` (and maybe the channel) on task events, and join to `task_labels` for the user labels in event filters.
- Alternatively, give renew events no labels at all. No one waits on "renewed".

**F. §14.4 session design in full detail.** It is deferred work, written down in detail. Keep the rule "the daemon token never reaches a browser, there is no token in a query string, and `/v1/sessions` is reserved". Move the rest to a UI spec.

**G. Event sourcing itself.** Not reversed. `tasks show --at`, `history` and `debug rebuild` are the stated debugging goals, and they need the log. Keep it, but take A, B and E, so that the log is the only complex part.

### 2. Design risks and disagreements

1. **The lease timer as the only writer of `LeaseExpired` (§6.4, change 3).**
   - Problem: if a claim wins the race against the timer, the log shows `TaskClaimed(attempt 1)` followed directly by `TaskClaimed(attempt 2)` with no expiry event. Every fold, the UI (§14.1 says "a `LeaseExpired` event tells it") and the invariant checks then have to infer the expiry.
   - Fix: have core emit `LeaseExpired` itself whenever any decision runs over an ended lease (claim, cancel, reopen), in the same commit. The timer becomes one trigger among several, and the log is always complete.
2. **Effective state duplicated in SQL (§5.3).** It must be special-cased in at least three places: the `tasks` query, the `message_labels` lookup for `board.task=ready` ("the label on returned roots", §5.3), and `/tasks?status=`. The contract test catches drift only for the cases it covers.
   - Fix: with 2.1 in place plus the timer, stored status is wrong only for about 1 ms after a lease ends. Either accept that for reads and drop the CASE, or keep the CASE in exactly one SQL view.
3. **Clock (§3).** `max(last_now, system_time)` handles only backward steps, and handles them badly. A one-hour backward NTP step freezes `now`, so no lease ends for an hour. A forward step ends every lease at once.
   - Fix: measure durations in-process with a monotonic clock (`Instant`) anchored at start, and use wall time only to stamp `at` and to carry across restarts.
4. **HTTP statuses.**
   - `timeout` as 408 (§7.2): 408 means "the client was too slow sending the request". Proxies and some clients retry it automatically. A retried claim-wait is mostly harmless; a misreading proxy is not.
   - `nothing_ready` as 404: it collides with "no such route" and with `not_found`.
   - Fix: return 200 with `{task: null, next_after}`, matching how `/events` already behaves.
5. **SSE `event: <type>` (§14.2).** A browser `EventSource` only fires `onmessage` for unnamed events. With named events, a UI must register a listener per type, and a new event type is silently invisible. That defeats "clients ignore unknown types".
   - Fix: omit `event:` and keep `type` in `data`.
6. **Exit codes (§8.5, §9).** "The wrapper exits with the command's code" collides with 3, 4 and 5: a child that exits 5 looks like `claim_lost`.
   - `claim_not_held` (a missing local file, a client bug) shares 5 with `claim_lost` (you were fenced out), but they need different fixes.
   - `--loop` has no defined exit code.
   - Fix: give wrapper outcomes fixed codes (for example "task failed" = 6) and put the child's status in `--json`.
7. **Claim files as the only route to a claim id (§8.3).** When the file is gone (another container, a tmp `BOARD_HOME`), the only option is waiting out the lease.
   - Fix: settle commands accept `--claim <id>` or `BOARD_CLAIM`. `tasks run` must keep the claim in memory and never depend on the file.
   - Also say plainly that `X-Board-Profile` is client-asserted, so `--for` / `forbidden` is routing, not security.
8. **The long-poll cap of 5 minutes (§7.3).** nginx and most load balancers time out idle connections at 60 s.
   - Fix: make the server cap configurable, with a default of about 50 s in server mode. The CLI re-issues anyway.
9. **Import and export with positions (§13.4).** Not specified:
   - whether positions are kept (if not, every UI cursor and `Last-Event-ID` breaks across a migration);
   - how portable person names (OS user) map to server token persons;
   - setting the clock floor to `max(at)`.

   Import also writes raw events, which strains §14.3's "every event is the result of a core decision". Allow it only into an empty store, keep positions, and require a person mapping.

### 3. Edge cases

- **Retried settle after an unclear failure.** `complete` commits, then the response is lost. The retry gets `claim_lost`, the CLI deletes the file and exits 5, and the agent believes it lost a task it actually finished.
  - Fix: the same `claim_id` with the same operation on a task that claim already settled returns the original success.
  - Also add idempotency keys to `post`/`reply` (§6.2 covers only create).
- **Renew failing with 500 or connection refused.** §9 defines only `claim_lost`. The wrapper should keep retrying until `expires_at` minus a margin (from the server's `now`), then stop the child.
- **Daemon restart or upgrade while workers run.**
  - The token is regenerated on every start (§10), so every running wrapper gets `unauthorized` on renew. The spec has no step for re-reading `daemon.json`.
  - A wrapper whose renew fails will auto-start a daemon (§8.4), possibly a different binary version.
  - If downtime exceeds the lease, start-up writes `LeaseExpired` for healthy workers (§6.4 "records every ended lease at start"), and their wrappers kill good work.
  - Fixes: a grace window at start-up (extend live claims by the downtime, or wait one lease before expiring), no auto-start from `tasks run` while a task is running, and `/health` version checks.
- **Many waiters (§7.3).** One `watch` bump wakes all 1,000 waiters, and each re-queries the database. That is about 1,000 queries per commit, or 1M/s at 1,000 commits/s, all through "a small pool" of read connections, and the same applies to SSE (§14.2).
  - Fix: broadcast committed envelopes, match filters in memory, and go to the store only for catch-up or lag.
  - `claim --wait` has no fairness. The longest waiter can starve. Use a FIFO waiter queue that hands tasks over in the writer.
- **WAL growth.** Long-lived streams that hold a read transaction block checkpoints. Every read must be a short transaction, never held across a wait. Run periodic `wal_checkpoint(TRUNCATE)` and have the soak test check WAL size.
- **`X-Board-Head`** must be read in the same read transaction as the data, or the gap-free promise in §7.4 breaks.
- **Disk full.** The lease timer hits `SQLITE_FULL` and retries in a tight loop. It needs backoff and a degraded state on `/ready`.
- **`synchronous=NORMAL`.** On power loss, an acknowledged claim can roll back. Document this; the server default is `full` (§13.2), which is fine.
- **Log growth, retention, redaction.** Positions are cursors "forever" (§7.4), so any later compaction breaks the contract.
  - Reserve a `cursor_expired` (410) code now.
  - "A posted secret stays in it" (§10) is not acceptable in production. Design a `MessageRedacted` event plus a physical scrub of `data`.
  - Editing and deleting messages are not in the model at all. Reserve the event types.
- **Event schema evolution.** The envelope has no schema version, and the rule for renaming or changing a field (upcasting in core) is unstated.
- **Huge threads.** `GET /threads/{id}` has no pagination. A task with 10,000 progress replies returns everything.
- **Mention spam.** `board.mention` sits outside the 32-label cap. A 64 KB message can create thousands of index rows. Cap mentions (for example at 32), and define the `@name` grammar (charset, code blocks, case).
- **Unicode.** Handles and person names (OS user, `BOARD_USER`) have no charset or normalization rule. Label values need NFC or byte-exact matching stated.
- **Ambiguous ids.** A 26-character Crockford string is a valid channel name, so §8.1's "ULID, else channel" is ambiguous.
- **Prompt injection.** `BOARD_PREVIOUS` (§9.1) feeds thread replies, which anyone can post, into the next attempt. With `--from '*'` in server mode, any person's text runs as instructions.
  - Mark untrusted content in the skill, and include only replies by the owner or the claim holder.
- **Blocking from inside `tasks run`.** The agent has `BOARD_CLAIM`, but the CLI `block` is Milestone 2 (§6.7). If it blocks through the API, the wrapper then completes or fails a blocked task and gets `invalid_state` or `claim_lost`. This is undefined.
  - Also, after `unblock`, the person's answer is not a "progress reply", so the next attempt may never see it.
- **Idempotent create with a different body** silently returns the first task. It should return `invalid_input` or `conflict` when the request hash differs. The keys never expire either.
- **Terminal by `LeaseExpired`.** A lead waiting for `type=TaskFailed` misses tasks that failed because attempts ran out. Either always emit `TaskFailed`, or document filtering on the label.
- **Rate limit.** `50/s per person` (§13.2) throttles one person running 50 workers.

### 4. Internal inconsistencies

- §13.5 says no pids in any response, but `/health` returns a pid (§7.1).
- §8.2 says Milestone 1 ships `tasks history`, `tasks show --at`, `debug rebuild` and `export`, and §9.1 adds `board progress`. None are in the §8.1 command list or the §11 scope. `--max-runtime` is missing from the §8.1 `tasks run` synopsis.
- Export is filtered in §8.2 and covers the whole log in §13.4. It is one command with two meanings, and a filtered export cannot be imported.
- §13.1 lists "a UI at `/ui/`" for portable clients, but §11 puts the UI out of scope.
- The origin allowlist is `--allow-origin` (§14.4) and `allow_origins` (§13.2), and §10 points to 14.4 for it.
- §14.1 asks the UI to dedupe "by stream and version", but no read response (`/tasks`, `/messages`) is specified to return a stream version.
- Naming switches between `status` (§5.2, `/tasks?status=`), `--state` (§8.1) and "States" (§6.1).
- Change 2 cites §6.4 and §6.5, but the mechanism is §5.3.
- §9 "People" uses `board tasks unblock`, but the CLI exposes it only in Milestone 2 (§6.7, §11).
- Change 3 says "correctness never depends on the timer", but Done-when #5 and #6 require the timer's event and its wake-up within 100 ms. That is liveness, not correctness; say so.
- §7.1: the claim-by-id body field is unnamed.

### 5. Reviewer's top 5 before anyone builds

1. **Use the single writer.** Decide inside the write transaction. Drop the 16-try retry loop, `conflict`, and the top-5 shuffle; claim-next becomes `LIMIT 1`. Core emits `LeaseExpired` lazily in the same commit. Delete the HA and Postgres-sequence material. (§3, §5, §6.4, §6.5, §13.4)
2. **Make settles idempotent per `claim_id`, and specify how the wrapper handles network errors and 500s** (keep renewing until the lease ends). Add `--claim` / `BOARD_CLAIM` as a fallback to the file. (§6.4, §8.3, §9)
3. **Fan waiters out in memory, not one database query per waiter.** Add a FIFO queue for claim waiters, and keep read transactions short to bound WAL growth. (§7.3, §14.2)
4. **Define daemon restart behavior:** a lease grace window at start, token re-read on `unauthorized`, version skew checks, no auto-start while a task is running, and a monotonic clock. (§3, §6.4, §8.4, §10)
5. **Fix the contracts before they freeze under §7.4.** Use 200 instead of 408 and 404 for empty claims, drop SSE `event:`, give wrapper exit codes fixed meanings, and reserve `cursor_expired`, redaction, edit/delete event types and an event schema version. Then fix the §4 inconsistencies.
