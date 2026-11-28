// Wires the page together (HANDOFF 20): sign in, replay the log, follow
// the stream, and redraw from state. The page only reads.

import { empty, apply, short } from './state.js';
import { columns, closed, pools, channels, card, colour, sentence, startOfDay, reverse } from './columns.js';
import { render, gate, bind, highlight, tick } from './render.js';

const API = '/v1';
const store = (() => {
  try { return window.localStorage; } catch { return null; }
})();
const remembered = (k) => { try { return store?.getItem(k) || null; } catch { return null; } };
const remember = (k, v) => { try { v ? store?.setItem(k, v) : store?.removeItem(k); } catch { /* private mode */ } };

let s = empty();
let ui = {
  theme: remembered('cork.theme'), // null follows the OS (20.3)
  panel: null, // { kind: 'task', id } | { kind: 'closed', state } | { kind: 'events' | 'workers' | 'cost' }
  pool: null,
  channel: null,
  hideDone: remembered('cork.hideDone') === '1',
};
let who = null;
let stats = null;
let live = false;
let liveText = 'connecting';
let skew = 0; // server clock minus ours, from the API's `now`
let hover = null;

const now = () => Date.now() + skew;

class Gone extends Error {}

async function api(path, opts = {}) {
  const r = await fetch(API + path, { credentials: 'same-origin', ...opts });
  if (r.status === 401) throw new Gone('session');
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body?.error?.message || `HTTP ${r.status}`);
  return body;
}

// Step 3 of 20.6: trade the one-time code in the fragment for a cookie.
async function signIn() {
  const m = /(?:^|[#&])code=([0-9a-f]+)/.exec(location.hash);
  if (!m) return;
  history.replaceState(null, '', location.pathname); // the code is spent either way
  const r = await fetch(API + '/sessions/redeem', {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'content-type': 'application/json', 'x-cork-ui': '1' },
    body: JSON.stringify({ code: m[1] }),
  });
  if (!r.ok) throw new Gone('code');
}

// Replay the whole log in pages, then follow the stream from its head.
async function replay() {
  let after = 0;
  for (;;) {
    const page = await api(`/events?after=${after}&limit=1000`);
    skew = page.now - Date.now();
    for (const e of page.events) apply(s, e);
    if (!page.events.length || page.next_after <= after) break;
    after = page.next_after;
  }
}

let es = null;
let retry = 1000;
function follow() {
  es?.close();
  es = new EventSource(`${API}/events/stream?after=${s.head}`);
  es.onopen = () => { live = true; retry = 1000; draw(); };
  es.onmessage = (msg) => {
    const e = JSON.parse(msg.data);
    apply(s, e);
    if (e.type === 'TaskCompleted' || e.type === 'MessagePosted') refreshStats();
    draw();
  };
  es.onerror = () => {
    live = false;
    liveText = 'reconnecting';
    draw();
    if (es.readyState !== EventSource.CLOSED) return; // the browser retries with Last-Event-ID
    // Closed for good: find out why, then reopen from our own head (14.1).
    api('/whoami').then(
      () => setTimeout(follow, (retry = Math.min(retry * 2, 15000))),
      (err) => err instanceof Gone ? signedOut('Session ended') : setTimeout(follow, retry),
    );
  };
}

let statsTimer = null;
function refreshStats(delay = 2000) {
  if (statsTimer) return;
  statsTimer = setTimeout(async () => {
    statsTimer = null;
    try {
      stats = await api(`/stats?since=${startOfDay(now())}&by=pool`);
      draw();
    } catch { /* the board still works without cost numbers */ }
  }, delay);
}

function signedOut(title) {
  live = false;
  es?.close();
  gate(title, 'Run <code>cork inspect</code> to open the board again.');
}

// The view model: everything render() draws, derived from state and ui.
function model() {
  const t = now();
  const view = { now: t, dayStart: startOfDay(t), pool: ui.pool, channel: ui.channel, hideDone: ui.hideDone };
  const rev = reverse(s);
  const busy = new Set();
  for (const x of s.tasks.values()) if (x.state === 'claimed' && x.claim) busy.add(x.claim.profile);
  const dark = ui.theme ? ui.theme === 'dark' : matchMedia('(prefers-color-scheme: dark)').matches;
  return {
    ui, now: t, dark, live, liveText, stats, hover,
    board: who?.board, head: s.head,
    columns: columns(s, view),
    closed: closed(s, view),
    pools: pools(s),
    channels: channels(s),
    workers: s.workers,
    busy,
    // A task's root message arrives just before its TaskCreated: show one line, not two.
    recent: s.recent
      .filter((e) => !(e.type === 'MessagePosted' && e.data?.id === e.data?.thread_id && s.tasks.get(e.data.id)?.created_position))
      .map((e) => ({ ...e, words: sentence(s, e) })),
    task: (id) => s.tasks.get(id),
    card: (x) => card(s, x, t, rev),
    short,
    colour,
  };
}

let queued = false;
function draw() {
  if (queued) return;
  queued = true;
  requestAnimationFrame(() => {
    queued = false;
    render(model());
  });
}

// What each action does to the ui state. Nothing else changes it.
function dispatch(a) {
  switch (a.type) {
    case 'open': ui = { ...ui, panel: { kind: 'task', id: a.id } }; break;
    case 'close': ui = { ...ui, panel: null }; break;
    case 'closed': ui = { ...ui, panel: { kind: 'closed', state: a.state } }; break;
    case 'page': ui = { ...ui, panel: a.page === 'board' ? null : { kind: a.page } };
      if (a.page === 'cost') refreshStats();
      break;
    case 'pool': ui = { ...ui, pool: ui.pool === a.pool ? null : a.pool }; break;
    case 'filter-pool': ui = { ...ui, pool: a.value }; break;
    case 'filter-channel': ui = { ...ui, channel: a.value }; break;
    case 'hide-done': ui = { ...ui, hideDone: !ui.hideDone }; remember('cork.hideDone', ui.hideDone ? '1' : null); break;
    case 'theme': {
      const dark = ui.theme ? ui.theme === 'dark' : matchMedia('(prefers-color-scheme: dark)').matches;
      ui = { ...ui, theme: dark ? 'light' : 'dark' };
      remember('cork.theme', ui.theme);
      break;
    }
    case 'hover': {
      const t = a.id && s.tasks.get(a.id);
      if (!t) { hover = null; highlight(null, []); return; }
      const c = card(s, t, now());
      hover = { self: a.id, links: [...c.waits_for, ...c.waited_by] };
      highlight(hover.self, hover.links);
      return; // hover never redraws the board
    }
    default: return;
  }
  draw();
}

async function main() {
  bind(dispatch);
  matchMedia('(prefers-color-scheme: dark)').addEventListener('change', draw);
  try {
    await signIn();
    who = await api('/whoami');
    await replay();
  } catch (err) {
    if (err instanceof Gone) {
      return err.message === 'code'
        ? signedOut('That sign-in link has been used or has expired')
        : signedOut('Not signed in');
    }
    gate('Could not load the board', String(err.message || err));
    return;
  }
  gate(null);
  draw();
  follow();
  refreshStats(0);
  // Lease bars move once a second, only while the tab shows.
  setInterval(() => {
    if (document.visibilityState === 'visible') tick(now());
  }, 1000);
}

main();
