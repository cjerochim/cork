// The board's fold and column rules, tested without a browser (HANDOFF
// 20.10). Run with `node --test crates/cork-server/ui/test/`. The logs were
// recorded from dummy workers (see record-fixture.sh); no agent ran.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fold, empty, apply, head, short } from '../state.js';
import { columns, closed, pools, card, sentence, colour } from '../columns.js';

const here = new URL('.', import.meta.url);
const log = (name) =>
  readFileSync(new URL(name, here), 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
const json = (name) => JSON.parse(readFileSync(new URL(name, here), 'utf8'));

for (const name of ['every-type', 'demo']) {
  test(`${name}: the fold matches GET /v1/tasks`, () => {
    const s = fold(log(`${name}.jsonl`));
    const want = json(`${name}.states.json`);
    const got = {};
    for (const t of s.tasks.values()) if (t.created_position) got[t.id] = t.state;
    assert.deepEqual(got, want);
  });

  test(`${name}: replaying an event twice changes nothing`, () => {
    const events = log(`${name}.jsonl`);
    const once = fold(events);
    const twice = fold(events, fold(events));
    assert.deepEqual([...twice.tasks.values()], [...once.tasks.values()]);
  });

  test(`${name}: folding in two halves equals folding at once (stream resume)`, () => {
    const events = log(`${name}.jsonl`);
    const mid = Math.floor(events.length / 2);
    const s = fold(events.slice(mid - 3), fold(events.slice(0, mid)));
    assert.deepEqual([...s.tasks.values()], [...fold(events).tasks.values()]);
  });
}

test('every task event type is in the recorded log', () => {
  const types = new Set(log('every-type.jsonl').map((e) => e.type));
  for (const t of [
    'TaskCreated', 'TaskClaimed', 'LeaseRenewed', 'LeaseExpired', 'TaskReleased',
    'TaskCompleted', 'TaskFailed', 'TaskBlocked', 'TaskUnblocked', 'TaskCanceled',
    'TaskReopened', 'TaskReplaced', 'DependenciesMet', 'DependencyFailed', 'MessagePosted',
  ]) assert.ok(types.has(t), t);
});

const byTitle = (s, title) => [...s.tasks.values()].find((t) => t.title === title);

test('titles come from the root message; plain threads are not tasks', () => {
  const s = fold(log('every-type.jsonl'));
  assert.ok(byTitle(s, 'Claim, renew, complete'));
  assert.equal(byTitle(s, 'A plain thread, not a task'), undefined);
});

test('usage replies give cost to the task and a pool to the worker', () => {
  const s = fold(log('every-type.jsonl'));
  assert.equal(byTitle(s, 'Claim, renew, complete').cost, 0.25);
  assert.equal(s.workers.get('rust-1').pool, 'rust');
  assert.equal(s.workers.get('rust-1').model, 'sonnet');
  assert.equal(s.workers.get('rust-2').pool, null, 'no usage yet: pool unknown');
});

test('a replaced task points at its retry, and dependencies follow it', () => {
  const s = fold(log('every-type.jsonl'));
  const old = byTitle(s, 'Replaced by a retry');
  const retry = byTitle(s, 'The retry');
  assert.equal(old.state, 'canceled');
  assert.equal(old.replaced_by, retry.id);
  assert.equal(head(s, old.id).id, retry.id);
  assert.equal(card(s, retry).retry_of, short(old.id));
});

test('a waiting task on a failed dependency is stuck', () => {
  const s = fold(log('every-type.jsonl'));
  const c = card(s, byTitle(s, 'Waits on f, which fails'));
  assert.equal(c.state, 'waiting');
  assert.ok(c.stuck);
  assert.deepEqual(c.waits_on, [short(byTitle(s, 'Fails').id)]);
});

// A tiny log written by hand: dummy events, nothing real.
let pos = 0;
const ev = (type, id, data = {}, at = 1000 + pos) => ({
  position: ++pos, stream: `task:${id}`, type, at, actor: 'chris',
  data: { type, ...data }, channel: 'work/rust', thread_id: id,
});
const root = (id, text) => ({
  position: ++pos, stream: `thread:${id}`, type: 'MessagePosted', at: 1000 + pos, actor: 'chris',
  data: { id, thread_id: id, text, channel: 'work/rust', labels: {} }, thread_id: id,
});
const created = (id, extra = {}) => [root(id, `Task ${id}`), ev('TaskCreated', id, { channel: 'work/rust', ...extra })];

test('columns: each state in its column, in its declared order', () => {
  pos = 0;
  const s = fold([
    ...created('A'), ...created('B'), ...created('C'), ...created('D', { depends_on: ['A'], open_dependencies: ['A'] }),
    ev('TaskClaimed', 'B', { claim_id: 'k1', holder: { person: 'chris', profile: 'w1' }, expires_at: 10_000, attempt: 1 }, 2000),
    ev('TaskClaimed', 'C', { claim_id: 'k2', holder: { person: 'chris', profile: 'w2' }, expires_at: 10_000, attempt: 1 }, 1500),
  ]);
  const cols = Object.fromEntries(columns(s, { now: 5000, dayStart: 0 }).map((c) => [c.key, c]));
  assert.deepEqual(cols.waiting.cards.map((c) => c.id), ['D']);
  assert.deepEqual(cols.ready.cards.map((c) => c.id), ['A']);
  assert.deepEqual(cols.claimed.cards.map((c) => c.id), ['C', 'B'], 'longest held first');
  assert.deepEqual(cols.waiting.cards[0].waits_on, [short('A')]);
  const lease = cols.claimed.cards[0].lease;
  assert.equal(lease.left_ms, 5000);
  assert.ok(lease.frac > 0.5 && lease.frac < 0.6);
});

test('columns: Done shows today only, newest first, then "+ N more"', () => {
  pos = 0;
  const events = [];
  for (let i = 0; i < 9; i++) {
    const id = `T${i}`;
    events.push(...created(id));
    events.push(ev('TaskClaimed', id, { claim_id: `k${i}`, holder: { person: 'chris', profile: 'w' }, expires_at: 1e9, attempt: 1 }));
    events.push(ev('TaskCompleted', id, { claim_id: `k${i}` }, i < 2 ? 100 : 10_000 + i));
  }
  const s = fold(events);
  const done = columns(s, { now: 20_000, dayStart: 5_000 }).find((c) => c.key === 'completed');
  assert.equal(done.count, 7, 'two finished before today');
  assert.deepEqual(done.cards.map((c) => c.id), ['T8', 'T7', 'T6', 'T5', 'T4', 'T3']);
  assert.equal(done.more, 1);
  assert.equal(columns(s, { hideDone: true }).some((c) => c.key === 'completed'), false);
});

test('blocked cards carry the question and who asked', () => {
  pos = 0;
  const s = fold([
    ...created('Q'),
    ev('TaskClaimed', 'Q', { claim_id: 'k', holder: { person: 'chris', profile: 'lead-1' }, expires_at: 1e9, attempt: 1 }),
    ev('TaskBlocked', 'Q', { claim_id: 'k', question: 'Which port?' }, 4242),
  ]);
  const c = columns(s, { dayStart: 0 }).find((c) => c.key === 'blocked').cards[0];
  assert.equal(c.question, 'Which port?');
  assert.equal(c.asked_by, 'lead-1');
  assert.equal(c.asked_at, 4242);
});

test('filters: pool and channel', () => {
  const s = fold(log('every-type.jsonl'));
  const ids = (v) => columns(s, { ...v, dayStart: 0 }).flatMap((c) => c.cards.map((x) => x.id));
  assert.deepEqual(ids({ pool: 'rust' }), [byTitle(s, 'The retry').id, byTitle(s, 'Claim, renew, complete').id]);
  assert.ok(ids({ channel: 'work/docs' }).every((id) => s.tasks.get(id).channel === 'work/docs'));
});

test('closed and pools', () => {
  const s = fold(log('every-type.jsonl'));
  const c = Object.fromEntries(closed(s).map((x) => [x.key, x.count]));
  assert.deepEqual(c, { failed: 1, canceled: 1 });
  const p = pools(s);
  assert.deepEqual(p.find((x) => x.pool === 'rust'), { pool: 'rust', name: 'rust', busy: 1, total: 1 });
  assert.equal(p.at(-1).pool, null, 'unknown pool sorts last');
});

test('the event strip speaks plainly; colours are stable', () => {
  const events = log('every-type.jsonl');
  const s = fold(events);
  const done = events.find((e) => e.type === 'TaskCompleted');
  assert.deepEqual(sentence(s, done), { who: 'rust-1', text: 'completed Claim, renew, complete' });
  assert.equal(colour('rust'), colour('rust'));
  assert.equal(colour(null), 'cx');
});

test('an unknown event type is ignored', () => {
  const s = empty();
  apply(s, { position: 1, stream: 'task:X', type: 'SomethingNew', at: 1, data: {} });
  assert.equal(s.head, 1);
});
