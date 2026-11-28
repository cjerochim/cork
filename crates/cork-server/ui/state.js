// The board's state, folded from the event log (HANDOFF 20.2, 14.1).
// No DOM here: the same events always give the same state, so Node can
// test it. The rules mirror `Task::apply` in cork-core.

export function empty() {
  return {
    head: 0,
    tasks: new Map(), // id -> task
    workers: new Map(), // profile -> { profile, pool, model, seen_at }
    titles: new Map(), // thread id -> first line of its root message
    recent: [], // the last few events, newest last, for the event strip
  };
}

const RECENT = 40;

function task(s, id) {
  let t = s.tasks.get(id);
  if (!t) {
    // An event for a task we never saw created (a log that starts mid-way).
    t = blank(id);
    s.tasks.set(id, t);
  }
  return t;
}

function blank(id) {
  return {
    id,
    title: '',
    channel: '',
    owner: '',
    state: 'ready',
    created_at: 0,
    created_position: 0,
    updated_at: 0,
    depends_on: [],
    replaces: null,
    replaced_by: null,
    claim: null, // { claim_id, profile, person, claimed_at, expires_at }
    attempts: 0,
    question: null,
    asked_by: null,
    asked_at: null,
    reason: null,
    completed_at: null,
    completed_by: null,
    cost: 0,
    history: [], // { position, type, at, actor }
  };
}

function idOf(e) {
  return e.stream.startsWith('task:') ? e.stream.slice(5) : null;
}

function labels(e) {
  // The envelope carries labels as [[k, v]]; a message's own data as {k: v}.
  const out = {};
  for (const [k, v] of e.labels || []) out[k] = v;
  return out;
}

function who(holder) {
  return holder ? holder.profile || holder.person : null;
}

// Apply one event. Returns the same state, changed in place, so a page can
// fold thousands of events without copying.
export function apply(s, e) {
  if (e.position <= s.head) return s; // seen already: replays are harmless
  s.head = e.position;
  s.recent.push(e);
  if (s.recent.length > RECENT) s.recent.shift();

  if (e.type === 'MessagePosted') {
    message(s, e);
    return s;
  }
  const id = idOf(e);
  if (!id) return s;
  const t = task(s, id);
  const d = e.data || {};
  t.updated_at = e.at;
  t.history.push({ position: e.position, type: e.type, at: e.at, actor: e.actor });

  switch (e.type) {
    case 'TaskCreated':
      t.title = s.titles.get(id) || t.title;
      t.channel = d.channel || e.channel || '';
      t.owner = d.owner || '';
      t.created_at = e.at;
      t.created_position = e.position;
      t.depends_on = d.depends_on || [];
      t.replaces = d.replaces || null;
      t.state = (d.open_dependencies || []).length ? 'waiting' : 'ready';
      if (t.replaces) task(s, t.replaces).replaced_by = id;
      break;
    case 'TaskClaimed':
      t.state = 'claimed';
      t.attempts = d.attempt || t.attempts + 1;
      t.claim = {
        claim_id: d.claim_id,
        profile: who(d.holder),
        person: d.holder ? d.holder.person : null,
        claimed_at: e.at,
        expires_at: d.expires_at,
      };
      seen(s, t.claim.profile, e.at);
      break;
    case 'LeaseRenewed':
      if (t.claim) t.claim.expires_at = d.expires_at;
      break;
    case 'LeaseExpired':
      t.claim = null;
      t.state = 'ready';
      break;
    case 'TaskReleased':
      t.claim = null;
      t.state = 'ready';
      t.attempts = Math.max(0, t.attempts - 1);
      break;
    case 'TaskCompleted':
      t.completed_by = t.claim ? t.claim.profile : null;
      t.completed_at = e.at;
      t.claim = null;
      t.state = 'completed';
      break;
    case 'TaskFailed':
      t.claim = null;
      t.state = 'failed';
      t.reason = d.reason || null;
      break;
    case 'TaskBlocked':
      t.asked_by = t.claim ? t.claim.profile : e.actor;
      t.asked_at = e.at;
      t.claim = null;
      t.state = 'blocked';
      t.question = d.question || '';
      break;
    case 'TaskUnblocked':
      t.state = 'ready';
      break;
    case 'TaskCanceled':
      t.claim = null;
      t.state = 'canceled';
      t.reason = d.reason || null;
      break;
    case 'TaskReplaced':
      t.replaced_by = d.by;
      t.claim = null;
      if (t.state !== 'failed' && t.state !== 'canceled') t.state = 'canceled';
      break;
    case 'DependenciesMet':
      t.state = 'ready';
      break;
    case 'TaskReopened':
      t.state = (d.open_dependencies || []).length ? 'waiting' : 'ready';
      t.attempts = 0;
      t.completed_at = null;
      t.completed_by = null;
      t.reason = null;
      t.question = null;
      t.asked_by = null;
      t.asked_at = null;
      break;
    default:
      // DependencyFailed leaves the task waiting; unknown types are ignored
      // (clients ignore event types they do not know, 7.6).
      break;
  }
  return s;
}

function message(s, e) {
  const m = e.data || {};
  const tid = m.thread_id || e.thread_id;
  // The root message of a task's thread is its title.
  if (m.id && m.id === tid) {
    s.titles.set(tid, firstLine(m.text));
    const t = s.tasks.get(tid);
    if (t) t.title = firstLine(m.text);
    return;
  }
  const ls = m.labels || labels(e);
  if (ls.usage !== 'attempt' || !m.data) return;
  // A `usage=attempt` reply (18): cost for the task, pool and model for
  // the worker. Nothing is guessed from profile names.
  const u = m.data;
  const t = s.tasks.get(u.task || tid);
  if (t && typeof u.cost_usd === 'number') t.cost += u.cost_usd;
  const p = u.profile || (m.actor && m.actor.profile);
  if (p) {
    const w = seen(s, p, e.at);
    if (u.pool) w.pool = u.pool;
    if (u.model) w.model = u.model;
  }
}

function seen(s, profile, at) {
  if (!profile) return null;
  let w = s.workers.get(profile);
  if (!w) {
    w = { profile, pool: null, model: null, seen_at: at };
    s.workers.set(profile, w);
  }
  w.seen_at = Math.max(w.seen_at, at);
  return w;
}

function firstLine(text) {
  const l = String(text || '').split('\n')[0].trim();
  return l.length > 140 ? l.slice(0, 139) + '…' : l;
}

// Fold a whole list of events.
export function fold(events, s = empty()) {
  for (const e of events) apply(s, e);
  return s;
}

// The task a declared id resolves to now: follow `replaced_by` (6.8.3).
export function head(s, id) {
  let t = s.tasks.get(id);
  const seenIds = new Set();
  while (t && t.replaced_by && !seenIds.has(t.id)) {
    seenIds.add(t.id);
    const next = s.tasks.get(t.replaced_by);
    if (!next) break;
    t = next;
  }
  return t || null;
}

// Tasks whose dependencies resolve to `id`.
export function dependents(s, id) {
  const out = [];
  for (const t of s.tasks.values()) {
    if (t.depends_on.some((d) => head(s, d)?.id === id)) out.push(t);
  }
  return out;
}

// A short id for display: the last four characters of the ULID.
export function short(id) {
  return String(id).slice(-4).toUpperCase();
}
