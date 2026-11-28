// The only module that touches the DOM (HANDOFF 20.2). It turns a view
// model into HTML and reports clicks back as plain actions. The page's CSP
// allows no inline styles, so sizes are set through CSSOM and colours by
// class.

const $ = (id) => document.getElementById(id);

const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
export const h = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ESC[c]);

const ICONS = {
  waiting: '<circle cx="7" cy="7" r="5.5" fill="none" stroke="currentColor" stroke-width="1.5" stroke-dasharray="2.2 2"/>',
  ready: '<circle cx="7" cy="7" r="5.5" fill="none" stroke="currentColor" stroke-width="1.5"/>',
  claimed: '<circle cx="7" cy="7" r="5.5" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="M7 3.2A3.8 3.8 0 0 1 7 10.8Z" fill="currentColor"/>',
  blocked: '<circle cx="7" cy="7" r="6.2" fill="currentColor"/><path d="M7 3.8v3.6" stroke="#fff" stroke-width="1.6" stroke-linecap="round"/><circle cx="7" cy="9.9" r=".95" fill="#fff"/>',
  completed: '<circle cx="7" cy="7" r="6.2" fill="currentColor"/><path d="M4.4 7.2l1.8 1.8 3.5-3.7" fill="none" stroke="#fff" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>',
  failed: '<circle cx="7" cy="7" r="6.2" fill="currentColor"/><path d="M4.8 4.8l4.4 4.4M9.2 4.8l-4.4 4.4" stroke="#fff" stroke-width="1.5" stroke-linecap="round"/>',
  canceled: '<circle cx="7" cy="7" r="6.2" fill="currentColor"/><path d="M4.6 7h4.8" stroke="#fff" stroke-width="1.5" stroke-linecap="round"/>',
};
export const icon = (state) =>
  `<svg class="ic ic-${state}" width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">${ICONS[state] || ICONS.ready}</svg>`;

const NAV_ICONS = {
  board: '<rect x="1.5" y="2" width="3.2" height="10" rx="1"/><rect x="5.9" y="2" width="3.2" height="7" rx="1"/><rect x="10.3" y="2" width="2.2" height="5" rx="1"/>',
  events: '<path d="M1.5 7h2.5l1.5-4 3 8 1.5-4h2.5"/>',
  workers: '<circle cx="7" cy="5" r="2.5"/><path d="M2.5 12.5a4.5 4.5 0 0 1 9 0"/>',
  cost: '<circle cx="7" cy="7" r="5.5"/><path d="M8.8 5.2c-.4-.6-1-.9-1.8-.9-1 0-1.8.5-1.8 1.3 0 1.8 3.8.9 3.8 2.7 0 .8-.8 1.4-2 1.4-.8 0-1.5-.3-1.9-.9M7 3.3v.9M7 9.8v.9"/>',
  moon: '<path d="M11.5 8.6A5 5 0 1 1 5.4 2.5a4 4 0 0 0 6.1 6.1z"/>',
  sun: '<circle cx="7" cy="7" r="2.6"/><path d="M7 1v1.6M7 11.4V13M1 7h1.6M11.4 7H13M2.8 2.8l1.1 1.1M10.1 10.1l1.1 1.1M2.8 11.2l1.1-1.1M10.1 3.9l1.1-1.1"/>',
  dep: '<path d="M3 2v4.5a2 2 0 0 0 2 2h4M7 6.5l2 2-2 2"/>',
  x: '<path d="M3.5 3.5l7 7M10.5 3.5l-7 7"/>',
};
const line = (k, size = 14) =>
  `<svg width="${size}" height="${size}" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.3" aria-hidden="true">${NAV_ICONS[k]}</svg>`;

export const money = (v) => (v == null ? '–' : '$' + (v >= 100 ? v.toFixed(0) : v.toFixed(2)));
export function ago(ms, now) {
  const s = Math.max(0, Math.round((now - ms) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${Math.round(s / 3600)}h ago`;
  return `${Math.round(s / 86400)}d ago`;
}
const left = (ms) => (ms < 60_000 ? `${Math.ceil(ms / 1000)}s` : `${Math.round(ms / 60_000)}m`);

function initials(p) {
  const parts = String(p).split(/[-_.]/).filter(Boolean);
  const a = (parts[0] || p)[0] || '?';
  const b = parts.length > 1 ? parts.at(-1).slice(-1) : '';
  return (a + b).toUpperCase();
}

function avatar(worker, cls) {
  if (!worker) return '<span class="av-none" title="Nobody holds it"></span>';
  return `<span class="av ${cls}" title="${h(worker)}">${h(initials(worker))}</span>`;
}

function card(c, m) {
  const sel = m.ui.panel?.kind === 'task' && m.ui.panel.id === c.id ? ' sel' : '';
  const pc = m.colour(c.pool);
  let notes = '';
  if (c.waits_on.length) {
    notes += `<span class="note${c.stuck ? ' bad' : ''}">${line('dep', 11)}${c.stuck ? 'stuck on' : 'waits on'} ${h(c.waits_on.join(', '))}</span>`;
  } else if (c.met.length) {
    notes += `<span class="note">${line('dep', 11)}${h(c.met.join(', '))} done</span>`;
  }
  if (c.retry_of) notes += `<span class="note">retry of ${h(c.retry_of)}</span>`;
  if (c.cost > 0) notes += `<span class="cost">${money(c.cost)}</span>`;
  let ask = '';
  if (c.state === 'blocked') {
    ask = `<div class="ask">“${h(c.question)}”<div class="by">asked by ${h(c.asked_by || 'someone')} · ${ago(c.asked_at, m.now)}</div></div>
      <div class="acts" aria-hidden="true"><span class="primary" title="Not in this version">Answer</span><span class="ghost" title="Not in this version">Reassign</span></div>`;
  }
  const lease = c.lease
    ? `<div class="lease" data-start="${c.lease.start}" data-end="${c.lease.end}"><i></i></div>`
    : '';
  return `<button class="card ${h(c.state)}${sel}" data-id="${h(c.id)}" data-act="open" aria-label="${h(c.title)}, ${h(c.state)}">
  <div class="card-top"><span class="mono">${h(c.short)}</span>${avatar(c.worker, pc)}</div>
  <div class="card-title">${icon(c.state)}<span class="t">${h(c.title)}</span></div>
  ${ask}<div class="meta"><span class="chan"><span class="pdot ${pc}"></span>${h(c.channel)}</span>${notes}</div>${lease}
</button>`;
}

function column(c, m) {
  const cards = c.cards.map((x) => card(x, m)).join('');
  const more = c.more ? `<div class="more">+ ${c.more} more</div>` : '';
  const empty = c.count ? '' : `<div class="empty">Nothing ${c.key === 'completed' ? 'done today' : 'here'}</div>`;
  return `<div class="col" data-col="${c.key}">
  <div class="col-head">${icon(c.state)}${h(c.title)}<span class="n">${c.count}</span></div>
  ${cards}${more}${empty}</div>`;
}

function nav(m) {
  const items = [['board', 'Board'], ['events', 'Events'], ['workers', 'Workers'], ['cost', 'Cost']];
  const open = m.ui.panel?.kind;
  return items
    .map(([k, label]) => {
      const on = k === 'board' ? !open || open === 'task' || open === 'closed' : open === k;
      return `<button class="${on ? 'on' : ''}" data-act="page" data-page="${k}">${line(k)}${label}</button>`;
    })
    .join('');
}

function pools(m) {
  if (!m.pools.length) return '<div class="row muted">No workers seen yet</div>';
  return m.pools
    .map((p) => {
      const on = p.pool && m.ui.pool === p.pool ? ' on' : '';
      const act = p.pool ? `data-act="pool" data-pool="${h(p.pool)}"` : 'disabled';
      return `<button class="row${on}" ${act}><span class="sw ${m.colour(p.pool)}"></span>${h(p.name)}<span class="end">${p.busy}/${p.total} busy</span></button>`;
    })
    .join('');
}

function closed(m) {
  return m.closed
    .map((c) => `<button class="row" data-act="closed" data-state="${c.key}">${icon(c.state)}${c.title}<span class="end">${c.count}</span></button>`)
    .join('');
}

function spend(m) {
  const t = m.stats?.total;
  if (!t) return '<div class="k">Spent today</div><div class="v num">–</div>';
  const per = t.cost_per_success == null ? 'no finished tasks yet' : `${money(t.cost_per_success)} per finished task`;
  return `<div class="k">Spent today</div><div class="v num">${money(t.usage.cost_usd)}</div><div class="s">${per}</div>`;
}

function filters(m) {
  const opt = (v, label, cur) => `<option value="${h(v)}"${v === cur ? ' selected' : ''}>${h(label)}</option>`;
  const poolOpts = [opt('', 'Pool: all', m.ui.pool || ''), ...m.pools.filter((p) => p.pool).map((p) => opt(p.pool, `Pool: ${p.pool}`, m.ui.pool))].join('');
  const chOpts = [opt('', 'Channel: all', m.ui.channel || ''), ...m.channels.map((c) => opt(c, `Channel: ${c}`, m.ui.channel))].join('');
  return `<select class="chip-btn${m.ui.pool ? ' on' : ''}" data-act="filter-pool" aria-label="Pool">${poolOpts}</select>
<select class="chip-btn${m.ui.channel ? ' on' : ''}" data-act="filter-channel" aria-label="Channel">${chOpts}</select>
<button class="chip-btn${m.ui.hideDone ? ' on' : ''}" data-act="hide-done" aria-pressed="${!!m.ui.hideDone}">Hide done</button>`;
}

function strip(m) {
  const last = m.recent.slice(-2).reverse();
  const items = last
    .map((e) => `<span class="pos">#${e.position}</span><span><b>${h(e.words.who)}</b> ${h(e.words.text)}</span>`)
    .join('');
  return `${items}<span class="where">${h(location.host)} · head #${m.head.toLocaleString()}</span>`;
}

function kv(rows) {
  return `<dl class="kv">${rows.filter(Boolean).map(([k, v]) => `<dt>${h(k)}</dt><dd>${v}</dd>`).join('')}</dl>`;
}

function taskLink(m, id) {
  const t = m.task(id);
  if (!t) return `<span class="mono">${h(m.short(id))}</span>`;
  return `<button class="row" data-act="open" data-id="${h(id)}">${icon(t.state)}<span class="mono muted">${h(m.short(id))}</span>${h(t.title)}</button>`;
}

function panel(m) {
  const p = m.ui.panel;
  const head = (label) => `<div class="p-head">${label}<button class="x" data-act="close" aria-label="Close">${line('x')}</button></div>`;
  if (p.kind === 'task') {
    const t = m.task(p.id);
    if (!t) return head('Task') + '<p class="muted">Not on this board.</p>';
    const c = m.card(t);
    const w = c.worker ? m.workers.get(c.worker) : null;
    const hist = [...t.history].reverse().slice(0, 50)
      .map((e) => `<div><span class="pos">#${e.position}</span><span>${h(e.type)}</span><span class="when">${h((e.actor || '').split(' via ')[0])} · ${ago(e.at, m.now)}</span></div>`)
      .join('');
    return `${head(`<span class="mono">${h(c.short)}</span>`)}
<h2>${h(t.title || '(untitled)')}</h2>
${kv([
      ['State', `${icon(t.state)} ${h(t.state)}`],
      ['Channel', h(t.channel)],
      ['Worker', c.worker ? `${h(c.worker)}${w?.pool ? ` · ${h(w.pool)}` : ' · pool unknown'}${w?.model ? ` · ${h(w.model)}` : ''}` : '<span class="muted">nobody</span>'],
      c.lease && ['Lease', `${left(c.lease.left_ms)} left`],
      ['Attempts', String(t.attempts)],
      ['Cost', money(t.cost)],
      ['Created', `${h(t.owner)} · ${ago(t.created_at, m.now)}`],
      t.replaces && ['Retry of', taskLink(m, t.replaces)],
      t.replaced_by && ['Retried as', taskLink(m, t.replaced_by)],
    ])}
${t.question && t.state === 'blocked' ? `<h3>Question</h3><div class="reason">${h(t.question)}</div>` : ''}
${t.reason ? `<h3>Reason</h3><div class="reason">${h(t.reason)}</div>` : ''}
${c.waits_for.length ? `<h3>Waits for</h3><div class="list">${c.waits_for.map((id) => taskLink(m, id)).join('')}</div>` : ''}
${c.waited_by.length ? `<h3>Waited on by</h3><div class="list">${c.waited_by.map((id) => taskLink(m, id)).join('')}</div>` : ''}
<h3>History</h3><div class="hist">${hist}</div>`;
  }
  if (p.kind === 'closed') {
    const c = m.closed.find((x) => x.key === p.state);
    const rows = c.tasks.slice(0, 200).map((t) => taskLink(m, t.id)).join('') || '<p class="muted">None.</p>';
    return `${head('Closed')}<h2>${c.title} · ${c.count}</h2><div class="list">${rows}</div>`;
  }
  if (p.kind === 'events') {
    const rows = m.recent.slice().reverse()
      .map((e) => `<div><span class="pos">#${e.position}</span><span><b>${h(e.words.who)}</b> ${h(e.words.text)}</span><span class="when">${ago(e.at, m.now)}</span></div>`)
      .join('');
    return `${head('Events')}<h2>Latest events</h2><div class="hist">${rows || '<p class="muted">None yet.</p>'}</div>`;
  }
  if (p.kind === 'workers') {
    const busy = m.busy;
    const rows = [...m.workers.values()].sort((a, b) => a.profile.localeCompare(b.profile))
      .map((w) => `<div class="row"><span class="av ${m.colour(w.pool)}">${h(initials(w.profile))}</span>${h(w.profile)}<span class="end">${h(w.pool || 'pool unknown')}${w.model ? ' · ' + h(w.model) : ''}${busy.has(w.profile) ? ' · busy' : ''}</span></div>`)
      .join('');
    return `${head('Workers')}<h2>Workers seen</h2><div class="list">${rows || '<p class="muted">None yet.</p>'}</div>`;
  }
  if (p.kind === 'cost') {
    const s = m.stats;
    if (!s) return `${head('Cost')}<h2>Today</h2><p class="muted">Loading…</p>`;
    const rows = s.groups
      .map((g) => `<div class="row"><span class="sw ${m.colour(g.key)}"></span>${h(g.key)}<span class="end">${money(g.usage.cost_usd)} · ${g.completed} done</span></div>`)
      .join('');
    return `${head('Cost')}<h2>Today · ${money(s.total.usage.cost_usd)}</h2>
${kv([
      ['Finished', String(s.total.completed)],
      ['Per finished', money(s.total.cost_per_success)],
      ['Failed', String(s.total.failed)],
      ['Unknown', `${s.total.usage.unknown} attempts with no usage`],
    ])}
<h3>By pool</h3><div class="list">${rows || '<p class="muted">No usage yet.</p>'}</div>
<p class="muted">The same numbers as <code>cork stats --since today</code>.</p>`;
  }
  return '';
}

// Cards that moved slide to their new place. Only in-between frames: what
// the page shows is already final.
function rects(root) {
  const out = new Map();
  for (const el of root.querySelectorAll('.card[data-id]')) out.set(el.dataset.id, el.getBoundingClientRect());
  return out;
}
const still = () => matchMedia('(prefers-reduced-motion: reduce)').matches;

export function render(m) {
  document.documentElement.dataset.theme = m.ui.theme || '';
  set('theme', line(m.dark ? 'sun' : 'moon'));
  $('board-name').textContent = m.board || 'cork';
  $('crumb-board').textContent = m.board || 'main';
  document.title = `${m.board || 'cork'} · Cork board`;
  const live = $('live');
  live.className = 'live ' + (m.live ? 'on' : 'off');
  $('live-text').textContent = m.live ? 'live' : m.liveText;
  set('nav', nav(m));
  set('pools', pools(m));
  set('closed', closed(m));
  set('spend', spend(m));
  set('filters', filters(m));

  const board = $('board');
  const focused = document.activeElement?.closest?.('.card')?.dataset.id;
  const before = still() ? null : rects(board);
  const changed = set('board', m.columns.map((c) => column(c, m)).join(''));
  tick(m.now);
  if (changed && before) {
    for (const el of board.querySelectorAll('.card[data-id]')) {
      const a = before.get(el.dataset.id);
      if (!a) continue;
      const b = el.getBoundingClientRect();
      const dx = a.left - b.left;
      const dy = a.top - b.top;
      if (Math.abs(dx) > 1 || Math.abs(dy) > 1) {
        el.animate([{ transform: `translate(${dx}px, ${dy}px)` }, { transform: 'none' }], {
          duration: 260, easing: 'cubic-bezier(.2,.7,.2,1)',
        });
      }
    }
  }
  if (changed && focused) board.querySelector(`.card[data-id="${CSS.escape(focused)}"]`)?.focus({ preventScroll: true });
  if (m.hover) highlight(m.hover.self, m.hover.links);

  set('strip', strip(m));
  const pan = $('panel');
  pan.hidden = !m.ui.panel;
  if (m.ui.panel) set('panel', panel(m));
}

// Lease bars move once a second without a full render (20.5). They only
// show time passing: a card leaves In progress only on a LeaseExpired event.
export function tick(now) {
  for (const el of document.querySelectorAll('.lease[data-end]')) {
    const start = Number(el.dataset.start);
    const end = Number(el.dataset.end);
    const frac = Math.max(0, Math.min(1, (end - now) / Math.max(1, end - start)));
    el.firstElementChild.style.setProperty('--p', frac.toFixed(4));
    el.classList.toggle('low', frac < 0.2);
    el.title = `lease: ${left(Math.max(0, end - now))} left`;
  }
}

// Only touch a region when its HTML changed, so an open menu or a hover
// survives a redraw.
const last = new Map();
function set(id, html) {
  if (last.get(id) === html) return false;
  last.set(id, html);
  $(id).innerHTML = html;
  return true;
}

export function highlight(self, links) {
  const board = $('board');
  board.classList.toggle('hovering', !!self);
  for (const el of board.querySelectorAll('.card')) {
    el.classList.toggle('hl', !!self && links.includes(el.dataset.id));
    el.classList.toggle('hl-self', el.dataset.id === self);
  }
}

export function gate(title, text) {
  $('gate').hidden = !title;
  if (title) {
    $('gate-title').textContent = title;
    $('gate-text').innerHTML = text;
  }
}

// Clicks, keys and hovers become plain actions for main.js.
export function bind(dispatch) {
  document.addEventListener('click', (ev) => {
    const el = ev.target.closest('[data-act]');
    if (!el || el.tagName === 'SELECT') return;
    const { act, id, page, pool, state } = el.dataset;
    dispatch({ type: act, id, page, pool, state });
  });
  document.addEventListener('change', (ev) => {
    const el = ev.target.closest('select[data-act]');
    if (el) dispatch({ type: el.dataset.act, value: el.value || null });
  });
  $('theme').addEventListener('click', () => dispatch({ type: 'theme' }));
  document.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape') dispatch({ type: 'close' });
  });
  const board = $('board');
  board.addEventListener('mouseover', (ev) => {
    const el = ev.target.closest('.card');
    dispatch({ type: 'hover', id: el ? el.dataset.id : null });
  });
  board.addEventListener('mouseleave', () => dispatch({ type: 'hover', id: null }));
  board.addEventListener('focusin', (ev) => {
    const el = ev.target.closest('.card');
    if (el) dispatch({ type: 'hover', id: el.dataset.id });
  });
}

