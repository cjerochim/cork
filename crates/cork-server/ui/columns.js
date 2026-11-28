// What the board shows, declared as data (HANDOFF 20.1, 20.2). Everything
// here is a pure function of the folded state and a few view settings, so
// Node can test which card lands in which column, in which order.

import { head, short } from './state.js';

// One entry per column, in the order work flows. `sort` names a key in
// SORTS; `limit` is how many cards show before "+ N more"; `today` keeps
// only tasks that finished since the start of the local day.
export const COLUMNS = [
  { key: 'waiting', title: 'Waiting on tasks', state: 'waiting', sort: 'oldest', limit: 100 },
  { key: 'ready', title: 'Ready', state: 'ready', sort: 'queue', limit: 100 },
  { key: 'claimed', title: 'In progress', state: 'claimed', sort: 'held_longest', limit: 100 },
  { key: 'blocked', title: 'Needs you', state: 'blocked', sort: 'asked_first', limit: 100 },
  { key: 'completed', title: 'Done', state: 'completed', sort: 'newest_done', limit: 6, today: true },
];

// Failed and canceled live in the sidebar under Closed.
export const CLOSED = [
  { key: 'failed', title: 'Failed', state: 'failed' },
  { key: 'canceled', title: 'Canceled', state: 'canceled' },
];

const by = (f) => (a, b) => f(a) - f(b) || a.created_position - b.created_position;
export const SORTS = {
  oldest: by((t) => t.created_position),
  queue: by((t) => t.created_position), // claim --ready is strict FIFO
  held_longest: by((t) => (t.claim ? t.claim.claimed_at : Infinity)),
  asked_first: by((t) => t.asked_at || t.updated_at),
  newest_done: by((t) => -(t.completed_at || 0)),
};

// The pool a task belongs to: the pool of the worker holding it, or of the
// worker that last held it, else unknown.
export function poolOf(s, t) {
  const p = t.claim?.profile || t.completed_by || t.asked_by;
  return (p && s.workers.get(p)?.pool) || null;
}

function wanted(s, t, view) {
  if (!t.created_position) return false; // a thread, not a task
  if (view.pool && poolOf(s, t) !== view.pool) return false;
  if (view.channel && t.channel !== view.channel) return false;
  return true;
}

// The columns with their cards, ready to draw.
export function columns(s, view = {}) {
  const now = view.now ?? Date.now();
  const dayStart = view.dayStart ?? startOfDay(now);
  const rev = reverse(s);
  return COLUMNS.filter((c) => !(view.hideDone && c.state === 'completed')).map((c) => {
    const all = [...s.tasks.values()]
      .filter((t) => t.state === c.state && wanted(s, t, view))
      .filter((t) => !c.today || (t.completed_at || 0) >= dayStart)
      .sort(SORTS[c.sort]);
    return {
      ...c,
      count: all.length,
      cards: all.slice(0, c.limit).map((t) => card(s, t, now, rev)),
      more: Math.max(0, all.length - c.limit),
    };
  });
}

// Task id -> ids of the tasks whose dependencies resolve to it.
export function reverse(s) {
  const rev = new Map();
  for (const t of s.tasks.values()) {
    for (const d of t.depends_on) {
      const h = head(s, d);
      if (!h) continue;
      if (!rev.has(h.id)) rev.set(h.id, []);
      rev.get(h.id).push(t.id);
    }
  }
  return rev;
}

// What one card says.
export function card(s, t, now = Date.now(), rev = reverse(s)) {
  const deps = t.depends_on.map((d) => head(s, d)).filter(Boolean);
  const open = deps.filter((d) => d.state !== 'completed');
  const met = deps.filter((d) => d.state === 'completed');
  const w = t.claim?.profile || (t.state === 'completed' ? t.completed_by : null) ||
    (t.state === 'blocked' ? t.asked_by : null);
  let lease = null;
  if (t.state === 'claimed' && t.claim) {
    const total = Math.max(1, t.claim.expires_at - t.claim.claimed_at);
    const left = Math.max(0, t.claim.expires_at - now);
    lease = { left_ms: left, frac: Math.min(1, left / total), start: t.claim.claimed_at, end: t.claim.expires_at };
  }
  return {
    id: t.id,
    short: short(t.id),
    title: t.title || '(untitled)',
    state: t.state,
    channel: t.channel,
    pool: poolOf(s, t),
    worker: w || null,
    waits_on: open.map((d) => short(d.id)),
    met: t.state === 'waiting' ? [] : met.map((d) => short(d.id)),
    stuck: open.some((d) => d.state === 'failed' || d.state === 'canceled'),
    cost: t.cost,
    lease,
    question: t.state === 'blocked' ? t.question : null,
    asked_by: t.state === 'blocked' ? t.asked_by : null,
    asked_at: t.state === 'blocked' ? t.asked_at : null,
    retry_of: t.replaces ? short(t.replaces) : null,
    retried_as: t.replaced_by ? short(t.replaced_by) : null,
    waits_for: deps.map((d) => d.id),
    waited_by: rev.get(t.id) || [],
  };
}

export function closed(s, view = {}) {
  return CLOSED.map((c) => {
    const tasks = [...s.tasks.values()]
      .filter((t) => t.state === c.state && wanted(s, t, view))
      .sort(by((t) => -t.updated_at));
    return { ...c, count: tasks.length, tasks };
  });
}

// Each pool with how many of its known workers hold a task now.
export function pools(s) {
  const out = new Map();
  for (const w of s.workers.values()) {
    const key = w.pool || 'pool unknown';
    if (!out.has(key)) out.set(key, { pool: w.pool, name: key, busy: 0, total: 0 });
    out.get(key).total++;
  }
  for (const t of s.tasks.values()) {
    if (t.state !== 'claimed' || !t.claim) continue;
    const w = s.workers.get(t.claim.profile);
    const key = w?.pool || 'pool unknown';
    if (out.has(key)) out.get(key).busy++;
  }
  return [...out.values()].sort((a, b) =>
    (a.pool === null) - (b.pool === null) || a.name.localeCompare(b.name));
}

export function channels(s) {
  const set = new Set();
  for (const t of s.tasks.values()) if (t.created_position) set.add(t.channel);
  return [...set].sort();
}

// A colour slot for a pool name, so the same pool is always the same colour.
export function colour(name) {
  if (!name) return 'cx';
  let h = 0;
  for (const ch of name) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
  return 'c' + (h % 6);
}

// An event in plain words, for the strip and the Events list.
const VERBS = {
  TaskCreated: 'created',
  TaskClaimed: 'claimed',
  LeaseRenewed: 'renewed',
  LeaseExpired: 'lost the lease on',
  TaskReleased: 'released',
  TaskCompleted: 'completed',
  TaskFailed: 'failed',
  TaskBlocked: 'asked about',
  TaskUnblocked: 'unblocked',
  TaskCanceled: 'canceled',
  TaskReopened: 'reopened',
  TaskReplaced: 'replaced',
  DependenciesMet: 'freed',
  DependencyFailed: 'is stuck on a failed dependency:',
};

export function sentence(s, e) {
  const who = (e.actor || '').split(' via ')[0];
  if (e.type === 'MessagePosted') {
    const m = e.data || {};
    const t = s.tasks.get(m.thread_id || e.thread_id);
    const where = t ? t.title : m.channel || e.channel;
    if ((m.labels || {}).usage === 'attempt') return { who, text: `reported usage on ${where}` };
    return { who, text: m.id === m.thread_id ? `posted in ${m.channel}` : `replied on ${where}` };
  }
  const id = e.stream.startsWith('task:') ? e.stream.slice(5) : '';
  const t = s.tasks.get(id);
  const verb = VERBS[e.type] || e.type;
  return { who, text: `${verb} ${t?.title || short(id)}` };
}

export function startOfDay(ms) {
  const d = new Date(ms);
  d.setHours(0, 0, 0, 0);
  return d.getTime();
}
