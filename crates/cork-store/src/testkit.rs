//! The storage contract suite (HANDOFF 5.4). Every adapter must pass it.
//!
//! In an adapter crate's tests:
//!
//! ```ignore
//! cork_store::contract_tests!(fresh_store());
//! ```
//!
//! The expression is a future that yields a fresh, empty store each time.

use crate::*;
use cork_core::message::{post_reply, post_root};
use cork_core::{
    Actor, BoardError, CreateTask, Decision, ErrorCode, Labels, NewMessage, TaskEvent, task_stream,
    thread_stream,
};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Generate one `#[tokio::test]` per contract check.
#[macro_export]
macro_rules! contract_tests {
    ($make:expr) => {
        $crate::contract_tests!(@each $make;
            failing_closure_writes_nothing,
            guard_no_stream,
            guard_exact,
            guard_any,
            commit_is_atomic_across_streams,
            failed_commit_writes_no_projections,
            positions_go_up,
            versions_are_contiguous,
            read_after_position,
            read_filters,
            writes_run_one_at_a_time,
            concurrent_writes_keep_global_order,
            readers_never_skip_positions,
            message_projection,
            message_label_filters,
            channels_projection,
            task_projection,
            effective_state_matches_core,
            ready_queue_order_and_filters,
            lease_end_queries,
            idempotency_projection,
            max_at_and_head,
            task_events_match_task_labels,
            import_keeps_positions,
        );
    };
    (@each $make:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn $name() {
                let store = ::std::sync::Arc::new($make.await);
                $crate::testkit::$name(store).await;
            }
        )*
    };
}

// ---------- helpers ----------

fn ev(ty: &str, n: u64) -> NewEvent {
    NewEvent::new(
        ty,
        serde_json::json!({ "n": n }),
        "tester",
        1,
        EventMeta::default(),
    )
}

fn one(stream: &str, expected: ExpectedVersion, events: Vec<NewEvent>) -> Commit {
    Commit {
        appends: vec![StreamAppend {
            stream_id: stream.into(),
            expected,
            events,
        }],
        projections: vec![],
    }
}

/// Write a fixed commit.
pub async fn put<S: Store>(s: &S, c: Commit) -> Result<Vec<StoredEvent>> {
    s.write(move |_| Ok((c, ()))).await.map(|(e, _)| e)
}

fn id() -> String {
    ulid::Ulid::new().to_string()
}

fn is_internal(r: &Result<Vec<StoredEvent>>) -> bool {
    matches!(r, Err(e) if e.code == ErrorCode::Internal)
}

fn root(channel: &str, text: &str, labels: Labels, at: Millis) -> Message {
    post_root(
        id(),
        channel,
        NewMessage {
            text: text.into(),
            labels,
            data: None,
        },
        Actor::person("chris"),
        at,
    )
    .unwrap()
}

fn labels(pairs: &[(&str, &str)]) -> Labels {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn message_event(m: &Message) -> NewEvent {
    let mut meta_labels: Vec<(String, String)> = m
        .labels
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    for h in &m.mentions {
        meta_labels.push((cork_core::labels::MENTION_LABEL.into(), h.clone()));
    }
    NewEvent::new(
        cork_core::message::MESSAGE_POSTED,
        serde_json::to_value(m).unwrap(),
        m.actor.to_string(),
        m.at,
        EventMeta {
            channel: Some(m.channel.clone()),
            thread_id: Some(m.thread_id.clone()),
            labels: meta_labels,
        },
    )
}

/// The commit that posts a message, the way the server builds it.
pub fn post_commit(m: &Message) -> Commit {
    Commit {
        appends: vec![StreamAppend {
            stream_id: thread_stream(&m.thread_id),
            expected: if m.is_root() {
                ExpectedVersion::NoStream
            } else {
                ExpectedVersion::Any
            },
            events: vec![message_event(m)],
        }],
        projections: vec![Projection::Message(m.clone())],
    }
}

/// Task events carry `board.task=<state after the event>` (HANDOFF 6.3).
/// `task` is the state before the first event, or `None` for a create.
fn task_events(
    task: Option<&Task>,
    events: &[TaskEvent],
    actor: &str,
    now: Millis,
) -> Vec<NewEvent> {
    let mut state = task.cloned();
    events
        .iter()
        .map(|e| {
            let next = match &state {
                None => Task::genesis(e, now).unwrap(),
                Some(t) => {
                    let mut t = t.clone();
                    t.apply(e, now);
                    t
                }
            };
            let ev = NewEvent::new(
                e.event_type(),
                serde_json::to_value(e).unwrap(),
                actor,
                now,
                EventMeta {
                    channel: Some(next.channel.clone()),
                    thread_id: Some(next.id.clone()),
                    labels: vec![(
                        cork_core::labels::TASK_LABEL.into(),
                        next.state.as_str().into(),
                    )],
                },
            );
            state = Some(next);
            ev
        })
        .collect()
}

/// Create a task the way the server does: root message plus task stream, in one commit.
pub async fn create_task<S: Store>(
    s: &S,
    channel: &str,
    task_labels: Labels,
    input: CreateTask,
    now: Millis,
) -> Task {
    let m = root(channel, "do it", task_labels.clone(), now);
    let created = Task::create(
        m.id.clone(),
        channel,
        &Actor::person("chris"),
        task_labels,
        &input,
    )
    .unwrap();
    let task = Task::genesis(&created, now).unwrap();
    let mut commit = post_commit(&m);
    commit.appends.push(StreamAppend {
        stream_id: task_stream(&task.id),
        expected: ExpectedVersion::NoStream,
        events: task_events(None, std::slice::from_ref(&created), "chris", now),
    });
    commit.projections.push(Projection::Task(task.clone()));
    commit.projections.push(Projection::SetMessageLabel {
        message_id: task.id.clone(),
        key: cork_core::labels::TASK_LABEL.into(),
        value: task.state.as_str().into(),
    });
    put(s, commit).await.unwrap();
    task
}

/// Read a task, decide with core and commit, all inside one write, the way
/// the server does. Returns the task after the change.
pub async fn decide<S, F>(s: &S, task_id: &str, now: Millis, f: F) -> Result<Task>
where
    S: Store,
    F: FnOnce(&Task) -> Result<Decision> + Send + 'static,
{
    let task_id = task_id.to_string();
    s.write(move |tx| {
        let stored = tx
            .get_task(&task_id)?
            .ok_or_else(|| BoardError::not_found(format!("task {task_id}")))?;
        let task = stored.task;
        let d = f(&task)?;
        let mut next = task.clone();
        for e in &d.events {
            next.apply(e, now);
        }
        let commit = if d.events.is_empty() {
            Commit::default()
        } else {
            Commit {
                appends: vec![StreamAppend {
                    stream_id: task_stream(&task.id),
                    expected: ExpectedVersion::Exact(task.version),
                    events: task_events(Some(&task), &d.events, "chris", now),
                }],
                projections: vec![
                    Projection::Task(next.clone()),
                    Projection::SetMessageLabel {
                        message_id: task.id.clone(),
                        key: cork_core::labels::TASK_LABEL.into(),
                        value: next.state.as_str().into(),
                    },
                ],
            }
        };
        Ok((commit, next))
    })
    .await
    .map(|(_, t)| t)
}

fn worker(name: &str) -> Actor {
    Actor::new("chris", Some(name.into())).unwrap()
}

fn lease(ms: i64, attempts: u32) -> CreateTask {
    CreateTask {
        lease_ms: Some(ms),
        max_attempts: Some(attempts),
        assignee: None,
    }
}

// ---------- atomic write and the version guard ----------

pub async fn failing_closure_writes_nothing<S: Store>(s: Arc<S>) {
    let r: Result<(Vec<StoredEvent>, ())> = s.write(|_| Err(BoardError::invalid("no"))).await;
    assert_eq!(r.unwrap_err().code, ErrorCode::InvalidInput);
    assert_eq!(s.head().await.unwrap(), 0);
    // An empty commit writes nothing and is fine.
    assert!(put(&*s, Commit::default()).await.unwrap().is_empty());
    assert_eq!(s.head().await.unwrap(), 0);
}

pub async fn guard_no_stream<S: Store>(s: Arc<S>) {
    let out = put(&*s, one("s:a", ExpectedVersion::NoStream, vec![ev("A", 1)]))
        .await
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].version, 1);
    let r = put(&*s, one("s:a", ExpectedVersion::NoStream, vec![ev("A", 2)])).await;
    assert!(is_internal(&r), "{r:?}");
    assert_eq!(s.stream_version("s:a").await.unwrap(), 1);
    assert_eq!(s.stream_version("s:none").await.unwrap(), 0);
}

pub async fn guard_exact<S: Store>(s: Arc<S>) {
    put(
        &*s,
        one(
            "s:a",
            ExpectedVersion::Exact(0),
            vec![ev("A", 1), ev("A", 2)],
        ),
    )
    .await
    .unwrap();
    assert!(is_internal(
        &put(&*s, one("s:a", ExpectedVersion::Exact(1), vec![ev("A", 3)])).await
    ));
    assert!(is_internal(
        &put(&*s, one("s:a", ExpectedVersion::Exact(3), vec![ev("A", 3)])).await
    ));
    let out = put(&*s, one("s:a", ExpectedVersion::Exact(2), vec![ev("A", 3)]))
        .await
        .unwrap();
    assert_eq!(out[0].version, 3);
}

pub async fn guard_any<S: Store>(s: Arc<S>) {
    for i in 1..=3 {
        let out = put(&*s, one("s:a", ExpectedVersion::Any, vec![ev("A", i)]))
            .await
            .unwrap();
        assert_eq!(out[0].version, i);
    }
}

pub async fn commit_is_atomic_across_streams<S: Store>(s: Arc<S>) {
    put(&*s, one("s:b", ExpectedVersion::NoStream, vec![ev("B", 1)]))
        .await
        .unwrap();
    let head = s.head().await.unwrap();
    let bad = Commit {
        appends: vec![
            StreamAppend {
                stream_id: "s:a".into(),
                expected: ExpectedVersion::NoStream,
                events: vec![ev("A", 1)],
            },
            StreamAppend {
                stream_id: "s:b".into(),
                expected: ExpectedVersion::NoStream,
                events: vec![ev("B", 2)],
            },
        ],
        projections: vec![],
    };
    assert!(is_internal(&put(&*s, bad).await));
    assert_eq!(
        s.head().await.unwrap(),
        head,
        "a failed commit writes nothing"
    );
    assert_eq!(s.stream_version("s:a").await.unwrap(), 0);
    let good = Commit {
        appends: vec![
            StreamAppend {
                stream_id: "s:a".into(),
                expected: ExpectedVersion::NoStream,
                events: vec![ev("A", 1)],
            },
            StreamAppend {
                stream_id: "s:b".into(),
                expected: ExpectedVersion::Exact(1),
                events: vec![ev("B", 2)],
            },
        ],
        projections: vec![],
    };
    let out = put(&*s, good).await.unwrap();
    assert_eq!(out.len(), 2);
    assert!(out[0].position < out[1].position);
}

pub async fn failed_commit_writes_no_projections<S: Store>(s: Arc<S>) {
    let m = root("dev", "hello", Labels::new(), 1);
    put(
        &*s,
        one(
            &thread_stream(&m.thread_id),
            ExpectedVersion::NoStream,
            vec![ev("X", 1)],
        ),
    )
    .await
    .unwrap();
    // The thread stream exists, so posting the root trips the guard and
    // must leave no message or channel row.
    assert!(is_internal(&put(&*s, post_commit(&m)).await));
    assert!(s.get_message(&m.id, 1).await.unwrap().is_none());
    assert!(s.list_channels().await.unwrap().is_empty());
}

// ---------- order ----------

pub async fn positions_go_up<S: Store>(s: Arc<S>) {
    let mut last = 0;
    for i in 0..50 {
        assert_eq!(
            s.write(|tx| Ok((Commit::default(), tx.head()?)))
                .await
                .unwrap()
                .1,
            last
        );
        let stream = format!("s:{}", i % 7);
        let out = put(
            &*s,
            one(&stream, ExpectedVersion::Any, vec![ev("A", i), ev("A", i)]),
        )
        .await
        .unwrap();
        for e in out {
            assert_eq!(e.position, last + 1, "positions have no gaps");
            last = e.position;
        }
    }
    assert_eq!(s.head().await.unwrap(), last);
}

pub async fn versions_are_contiguous<S: Store>(s: Arc<S>) {
    for i in 0..20 {
        put(&*s, one("s:v", ExpectedVersion::Exact(i), vec![ev("A", i)]))
            .await
            .unwrap();
    }
    let page = s
        .read_events(&EventFilter {
            stream_id: Some("s:v".into()),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    let versions: Vec<u64> = page.events.iter().map(|e| e.version).collect();
    assert_eq!(versions, (1..=20).collect::<Vec<_>>());
}

pub async fn read_after_position<S: Store>(s: Arc<S>) {
    let mut positions = vec![];
    for i in 0..10 {
        positions.push(
            put(&*s, one("s:r", ExpectedVersion::Any, vec![ev("A", i)]))
                .await
                .unwrap()[0]
                .position,
        );
    }
    let page = s
        .read_events(&EventFilter::after(positions[4], 100))
        .await
        .unwrap();
    assert_eq!(
        page.events.iter().map(|e| e.position).collect::<Vec<_>>(),
        positions[5..].to_vec()
    );
    assert_eq!(page.head, positions[9]);
    let page = s
        .read_events(&EventFilter::after(positions[4], 2))
        .await
        .unwrap();
    assert_eq!(page.events.len(), 2);
    assert_eq!(page.events[0].position, positions[5]);
    assert_eq!(page.events[0].data, serde_json::json!({ "n": 5 }));
    assert_eq!(page.events[0].schema, 1);
    let page = s
        .read_events(&EventFilter::after(positions[9], 100))
        .await
        .unwrap();
    assert!(page.events.is_empty());
    assert_eq!(
        page.head, positions[9],
        "an empty page still reports the head"
    );
}

pub async fn read_filters<S: Store>(s: Arc<S>) {
    let mk = |ty: &str, ch: &str, th: &str, l: &[(&str, &str)]| {
        NewEvent::new(
            ty,
            serde_json::json!({}),
            "a",
            1,
            EventMeta {
                channel: Some(ch.into()),
                thread_id: Some(th.into()),
                labels: l
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
        )
    };
    let rows = [
        mk("A", "work", "t1", &[("step", "fix")]),
        mk(
            "B",
            "work/fix",
            "t2",
            &[
                ("step", "fix"),
                ("board.mention", "x"),
                ("board.mention", "y"),
            ],
        ),
        mk("A", "workshop", "t3", &[("step", "review")]),
        mk("B", "dev", "t4", &[]),
        mk("A", "work_x", "t5", &[]),
    ];
    for (i, r) in rows.into_iter().enumerate() {
        put(
            &*s,
            one(&format!("s:{i}"), ExpectedVersion::NoStream, vec![r]),
        )
        .await
        .unwrap();
    }
    let q = |f: EventFilter| {
        let s = s.clone();
        async move {
            let f = EventFilter { limit: 100, ..f };
            let page = s.read_events(&f).await.unwrap();
            for e in &page.events {
                assert!(f.matches(e), "EventFilter::matches agrees with the adapter");
            }
            page.events
                .iter()
                .map(|e| e.stream_id.clone())
                .collect::<Vec<_>>()
        }
    };
    let sub = |p: &str| Some(ChannelSelector::Subtree(p.into()));
    assert_eq!(
        q(EventFilter {
            channel: sub("work"),
            ..Default::default()
        })
        .await,
        ["s:0", "s:1"]
    );
    // `_` is a legal channel character, not a wildcard.
    assert_eq!(
        q(EventFilter {
            channel: sub("work_"),
            ..Default::default()
        })
        .await,
        Vec::<String>::new()
    );
    assert_eq!(
        q(EventFilter {
            channel: Some(ChannelSelector::Exact("work".into())),
            ..Default::default()
        })
        .await,
        ["s:0"]
    );
    assert_eq!(
        q(EventFilter {
            thread_id: Some("t3".into()),
            ..Default::default()
        })
        .await,
        ["s:2"]
    );
    assert_eq!(
        q(EventFilter {
            labels: vec![("step".into(), "fix".into())],
            ..Default::default()
        })
        .await,
        ["s:0", "s:1"]
    );
    assert_eq!(
        q(EventFilter {
            labels: vec![
                ("board.mention".into(), "x".into()),
                ("board.mention".into(), "y".into())
            ],
            ..Default::default()
        })
        .await,
        ["s:1"]
    );
    assert_eq!(
        q(EventFilter {
            types: vec!["B".into()],
            ..Default::default()
        })
        .await,
        ["s:1", "s:3"]
    );
    assert_eq!(
        q(EventFilter {
            stream_id: Some("s:3".into()),
            ..Default::default()
        })
        .await,
        ["s:3"]
    );
    assert!(
        q(EventFilter {
            labels: vec![("step".into(), "fix".into())],
            types: vec!["Z".into()],
            ..Default::default()
        })
        .await
        .is_empty()
    );
}

/// The v3 single writer: many concurrent claims of one task, each deciding
/// inside its own write. Exactly one wins and the rest see it claimed.
pub async fn writes_run_one_at_a_time<S: Store>(s: Arc<S>) {
    let t = create_task(&*s, "work", Labels::new(), lease(60_000, 3), 10).await;
    let mut handles = vec![];
    for i in 0..32 {
        let s = s.clone();
        let id = t.id.clone();
        handles.push(tokio::spawn(async move {
            decide(&*s, &id, 20, move |t| {
                t.claim(20, &worker(&format!("w{i}")), format!("c{i}"), None)
            })
            .await
        }));
    }
    let mut wins = 0;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => wins += 1,
            Err(e) => assert_eq!(e.code, ErrorCode::TaskClaimed, "{e:?}"),
        }
    }
    assert_eq!(wins, 1);
    assert_eq!(s.stream_version(&task_stream(&t.id)).await.unwrap(), 2);
}

pub async fn concurrent_writes_keep_global_order<S: Store>(s: Arc<S>) {
    let mut handles = vec![];
    for w in 0..8u64 {
        let s = s.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..50 {
                put(
                    &*s,
                    one(
                        &format!("s:{w}"),
                        ExpectedVersion::Exact(i),
                        vec![ev("A", i)],
                    ),
                )
                .await
                .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let all = s
        .read_events(&EventFilter::after(0, 10_000))
        .await
        .unwrap()
        .events;
    assert_eq!(all.len(), 400);
    let mut seen = HashSet::new();
    let mut per_stream: BTreeMap<String, u64> = BTreeMap::new();
    for w in all.windows(2) {
        assert!(w[0].position < w[1].position);
    }
    for e in &all {
        assert!(seen.insert(e.position));
        let v = per_stream.entry(e.stream_id.clone()).or_default();
        *v += 1;
        assert_eq!(e.version, *v, "versions in a stream follow position order");
    }
}

/// Promise 3: a reader that follows "after N" sees every event exactly once.
pub async fn readers_never_skip_positions<S: Store>(s: Arc<S>) {
    let writers = 8u64;
    let per = 60u64;
    let done = Arc::new(AtomicBool::new(false));
    let reader = {
        let s = s.clone();
        let done = done.clone();
        tokio::spawn(async move {
            let mut after = 0;
            let mut seen = Vec::new();
            loop {
                let finished = done.load(Ordering::SeqCst);
                let page = s.read_events(&EventFilter::after(after, 50)).await.unwrap();
                for e in &page.events {
                    assert!(e.position > after);
                    after = e.position;
                    seen.push(e.position);
                }
                if finished && page.events.is_empty() {
                    return seen;
                }
                tokio::task::yield_now().await;
            }
        })
    };
    let mut handles = vec![];
    for w in 0..writers {
        let s = s.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..per {
                put(
                    &*s,
                    one(&format!("s:{w}"), ExpectedVersion::Any, vec![ev("A", i)]),
                )
                .await
                .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    done.store(true, Ordering::SeqCst);
    let seen = reader.await.unwrap();
    let all: Vec<u64> = s
        .read_events(&EventFilter::after(0, 100_000))
        .await
        .unwrap()
        .events
        .iter()
        .map(|e| e.position)
        .collect();
    assert_eq!(all.len() as u64, writers * per);
    assert_eq!(
        seen, all,
        "the tailing reader sees every position, in order, once"
    );
}

// ---------- message projections ----------

pub async fn message_projection<S: Store>(s: Arc<S>) {
    let m = root("work/fix", "hi @Fixer", labels(&[("step", "fix")]), 1);
    put(&*s, post_commit(&m)).await.unwrap();
    let reply = post_reply(
        id(),
        &m,
        NewMessage {
            text: "re".into(),
            ..Default::default()
        },
        Actor::person("chris"),
        2,
    )
    .unwrap();
    put(&*s, post_commit(&reply)).await.unwrap();
    assert_eq!(s.get_message(&m.id, 2).await.unwrap().unwrap(), m);
    assert_eq!(s.get_message(&reply.id, 2).await.unwrap().unwrap(), reply);

    let list = |q: MessageQuery| {
        let s = s.clone();
        async move {
            s.list_messages(&MessageQuery {
                limit: if q.limit == 0 { 10 } else { q.limit },
                ..q
            })
            .await
            .unwrap()
        }
    };
    let thread = list(MessageQuery {
        thread_id: Some(m.thread_id.clone()),
        ..Default::default()
    })
    .await;
    assert_eq!(
        thread
            .iter()
            .map(|x| x.message.id.clone())
            .collect::<Vec<_>>(),
        vec![m.id.clone(), reply.id.clone()]
    );
    assert!(thread[0].position < thread[1].position);
    let roots = list(MessageQuery {
        channel: Some(ChannelSelector::Subtree("work".into())),
        roots_only: true,
        ..Default::default()
    })
    .await;
    assert_eq!(roots.len(), 1);
    let newest = list(MessageQuery {
        thread_id: Some(m.thread_id.clone()),
        limit: 1,
        newest_first: true,
        ..Default::default()
    })
    .await;
    assert_eq!(newest.len(), 1);
    assert_eq!(newest[0].message.id, reply.id);
    let after = list(MessageQuery {
        thread_id: Some(m.thread_id.clone()),
        after: thread[0].position,
        ..Default::default()
    })
    .await;
    assert_eq!(after.len(), 1);
    // Mentions are indexed as `board.mention`, lowercased.
    let mentioned = list(MessageQuery {
        labels: vec![("board.mention".into(), "fixer".into())],
        ..Default::default()
    })
    .await;
    assert_eq!(mentioned.len(), 1);
    assert_eq!(mentioned[0].message.id, m.id);
}

pub async fn message_label_filters<S: Store>(s: Arc<S>) {
    let a = root("dev", "a", labels(&[("step", "fix")]), 1);
    let b = root("dev", "b", Labels::new(), 1);
    put(&*s, post_commit(&a)).await.unwrap();
    put(&*s, post_commit(&b)).await.unwrap();
    let by = |k: &str, v: &str| {
        let s = s.clone();
        let l = vec![(k.to_string(), v.to_string())];
        async move {
            s.list_messages(&MessageQuery {
                labels: l,
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap()
        }
    };
    let got = by("step", "fix").await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].message.id, a.id);
    put(
        &*s,
        Commit {
            appends: vec![],
            projections: vec![Projection::SetMessageLabel {
                message_id: b.id.clone(),
                key: "board.x".into(),
                value: "1".into(),
            }],
        },
    )
    .await
    .unwrap();
    let got = by("board.x", "1").await;
    assert_eq!(got.len(), 1);
    assert_eq!(
        got[0].message.labels.get("board.x").map(String::as_str),
        Some("1")
    );
    // Setting a key again replaces the value.
    put(
        &*s,
        Commit {
            appends: vec![],
            projections: vec![Projection::SetMessageLabel {
                message_id: b.id.clone(),
                key: "board.x".into(),
                value: "2".into(),
            }],
        },
    )
    .await
    .unwrap();
    assert!(by("board.x", "1").await.is_empty());
    assert_eq!(by("board.x", "2").await.len(), 1);
}

pub async fn channels_projection<S: Store>(s: Arc<S>) {
    let a = root("dev", "a", Labels::new(), 1);
    let b = root("dev", "b", Labels::new(), 1);
    let c = root("work/fix", "c", Labels::new(), 1);
    for m in [&a, &b, &c] {
        put(&*s, post_commit(m)).await.unwrap();
    }
    let reply = post_reply(
        id(),
        &a,
        NewMessage {
            text: "r".into(),
            ..Default::default()
        },
        Actor::person("x"),
        1,
    )
    .unwrap();
    let out = put(&*s, post_commit(&reply)).await.unwrap();
    let chans = s.list_channels().await.unwrap();
    let dev = chans.iter().find(|c| c.path == "dev").unwrap();
    assert_eq!(dev.threads, 2);
    assert_eq!(dev.messages, 3);
    assert_eq!(dev.last_position, out[0].position);
    assert!(chans.iter().any(|c| c.path == "work/fix"));
}

// ---------- task projections ----------

pub async fn task_projection<S: Store>(s: Arc<S>) {
    let t = create_task(&*s, "work", labels(&[("step", "fix")]), lease(1_000, 1), 10).await;
    let stored = s.get_task(&t.id).await.unwrap().unwrap();
    assert_eq!(stored.task, t);
    let t2 = decide(&*s, &t.id, 10, |t| {
        t.claim(10, &worker("w"), "c1".into(), None)
    })
    .await
    .unwrap();
    let again = s.get_task(&t.id).await.unwrap().unwrap();
    assert_eq!(again.task, t2);
    assert_eq!(
        again.created_position, stored.created_position,
        "queue place never moves"
    );
    // A rejected decision writes nothing.
    let head = s.head().await.unwrap();
    let e = decide(&*s, &t.id, 20, |t| {
        t.claim(20, &worker("x"), "c2".into(), None)
    })
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::TaskClaimed);
    assert_eq!(s.head().await.unwrap(), head);
    // The root message carries the state label.
    let m = s.get_message(&t.id, 20).await.unwrap().unwrap();
    assert_eq!(m.labels["board.task"], "claimed");
    // After the lease, with one attempt, reads say failed without any write.
    let m = s.get_message(&t.id, 5_000).await.unwrap().unwrap();
    assert_eq!(m.labels["board.task"], "failed");
}

/// Effective state in SQL must agree with `Task::effective_state` (HANDOFF 5.3).
pub async fn effective_state_matches_core<S: Store>(s: Arc<S>) {
    // A spread of tasks: ready, live claim, ended claim with attempts left,
    // ended claim with none left, completed, blocked, canceled.
    let mut ids = vec![];
    let mk = |attempts| lease(1_000, attempts);
    let ready = create_task(&*s, "q", Labels::new(), mk(3), 0).await;
    ids.push(ready.id.clone());
    for (attempts, then) in [
        (3, None),
        (1, None),
        (3, Some("complete")),
        (3, Some("block")),
        (3, Some("cancel")),
    ] {
        let t = create_task(&*s, "q", Labels::new(), mk(attempts), 0).await;
        decide(&*s, &t.id, 100, |t| {
            t.claim(100, &worker("w"), "c".into(), None)
        })
        .await
        .unwrap();
        match then {
            Some("complete") => {
                decide(&*s, &t.id, 200, |t| t.complete(200, "c", None))
                    .await
                    .unwrap();
            }
            Some("block") => {
                decide(&*s, &t.id, 200, |t| t.block(200, "c", "why?".into()))
                    .await
                    .unwrap();
            }
            Some("cancel") => {
                decide(&*s, &t.id, 200, |t| {
                    t.cancel(200, &Actor::person("chris"), None)
                })
                .await
                .unwrap();
            }
            _ => {}
        }
        ids.push(t.id);
    }
    // Live through 1_100 inclusive, ended from 1_101.
    for now in [150, 1_100, 1_101, 50_000] {
        let mut want: std::collections::HashMap<TaskState, Vec<String>> = Default::default();
        for id in &ids {
            let t = s.get_task(id).await.unwrap().unwrap().task;
            want.entry(t.effective_state(now))
                .or_default()
                .push(id.clone());
        }
        for state in TaskState::ALL {
            let got: Vec<String> = s
                .list_tasks(&TaskQuery {
                    state: Some(state),
                    limit: 100,
                    now,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_iter()
                .map(|t| t.task.id)
                .collect();
            let expect = want.get(&state).cloned().unwrap_or_default();
            assert_eq!(got, expect, "state {state:?} at {now}");
            let via_label: Vec<String> = s
                .list_messages(&MessageQuery {
                    labels: vec![("board.task".into(), state.as_str().into())],
                    limit: 100,
                    now,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_iter()
                .map(|m| {
                    assert_eq!(m.message.labels["board.task"], state.as_str());
                    m.message.id
                })
                .collect();
            assert_eq!(via_label, expect, "board.task={} at {now}", state.as_str());
        }
    }
}

pub async fn ready_queue_order_and_filters<S: Store>(s: Arc<S>) {
    let fix = labels(&[("step", "fix")]);
    let a = create_task(&*s, "work/fix", fix.clone(), lease(1_000, 3), 1).await;
    let b = create_task(&*s, "work", Labels::new(), lease(1_000, 3), 2).await;
    let c = create_task(&*s, "other", fix.clone(), lease(1_000, 3), 3).await;
    let d = create_task(&*s, "work/fix", fix.clone(), lease(1_000, 3), 4).await;
    let e = create_task(
        &*s,
        "work",
        Labels::new(),
        CreateTask {
            assignee: Some("reviewer".into()),
            ..lease(1_000, 3)
        },
        5,
    )
    .await;
    // Claim a: it leaves the queue until its lease ends.
    decide(&*s, &a.id, 10, |t| {
        t.claim(10, &worker("w"), "c".into(), None)
    })
    .await
    .unwrap();
    let ready = |q: TaskQuery| {
        let s = s.clone();
        async move {
            let q = TaskQuery {
                state: Some(TaskState::Ready),
                limit: if q.limit == 0 { 10 } else { q.limit },
                ..q
            };
            s.list_tasks(&q)
                .await
                .unwrap()
                .into_iter()
                .map(|t| t.task.id)
                .collect::<Vec<_>>()
        }
    };
    let sub = |p: &str| Some(ChannelSelector::Subtree(p.into()));
    assert_eq!(
        ready(TaskQuery {
            now: 20,
            ..Default::default()
        })
        .await,
        vec![b.id.clone(), c.id.clone(), d.id.clone(), e.id.clone()]
    );
    assert_eq!(
        ready(TaskQuery {
            now: 20,
            channel: sub("work"),
            ..Default::default()
        })
        .await,
        vec![b.id.clone(), d.id.clone(), e.id.clone()]
    );
    assert_eq!(
        ready(TaskQuery {
            now: 20,
            channel: sub("work"),
            labels: vec![("step".into(), "fix".into())],
            ..Default::default()
        })
        .await,
        vec![d.id.clone()]
    );
    // Assigned tasks are only offered to their assignee.
    assert_eq!(
        ready(TaskQuery {
            now: 20,
            claimable_by: Some("w".into()),
            ..Default::default()
        })
        .await,
        vec![b.id.clone(), c.id.clone(), d.id.clone()]
    );
    assert_eq!(
        ready(TaskQuery {
            now: 20,
            claimable_by: Some("reviewer".into()),
            ..Default::default()
        })
        .await
        .len(),
        4
    );
    // After a's lease ends it is ready again, in its original queue place.
    assert_eq!(
        ready(TaskQuery {
            now: 2_000,
            channel: Some(ChannelSelector::Exact("work/fix".into())),
            ..Default::default()
        })
        .await,
        vec![a.id.clone(), d.id.clone()]
    );
    assert!(
        ready(TaskQuery {
            now: 20,
            owner: Some("nobody".into()),
            ..Default::default()
        })
        .await
        .is_empty()
    );
    // FIFO head, the way `claim --ready` asks for it, also inside a write.
    let head = s.list_tasks(&TaskQuery::ready(20)).await.unwrap();
    assert_eq!(head.len(), 1);
    assert_eq!(head[0].task.id, b.id);
    let in_tx = s
        .write(|tx| Ok((Commit::default(), tx.list_tasks(&TaskQuery::ready(2_000))?)))
        .await
        .unwrap()
        .1;
    assert_eq!(in_tx[0].task.id, a.id);
    assert_eq!(
        ready(TaskQuery {
            now: 20,
            after: head[0].created_position,
            ..Default::default()
        })
        .await,
        vec![c.id.clone(), d.id.clone(), e.id.clone()]
    );
}

pub async fn lease_end_queries<S: Store>(s: Arc<S>) {
    assert_eq!(s.next_lease_end().await.unwrap(), None);
    let a = create_task(&*s, "work", Labels::new(), lease(1_000, 3), 0).await;
    let b = create_task(&*s, "work", Labels::new(), lease(5_000, 3), 0).await;
    decide(&*s, &a.id, 100, |t| {
        t.claim(100, &worker("w"), "ca".into(), None)
    })
    .await
    .unwrap();
    decide(&*s, &b.id, 100, |t| {
        t.claim(100, &worker("w"), "cb".into(), None)
    })
    .await
    .unwrap();
    assert_eq!(s.next_lease_end().await.unwrap(), Some(1_100));
    let mut claimed = s.claimed_tasks().await.unwrap();
    claimed.sort();
    let mut both = vec![a.id.clone(), b.id.clone()];
    both.sort();
    assert_eq!(claimed, both);
    let ended = |now| {
        let s = s.clone();
        async move {
            s.write(move |tx| Ok((Commit::default(), tx.ended_leases(now, 10)?)))
                .await
                .unwrap()
                .1
        }
    };
    assert!(
        ended(1_100).await.is_empty(),
        "a lease is live through its last millisecond"
    );
    assert_eq!(ended(1_101).await, vec![a.id.clone()]);
    assert_eq!(ended(10_000).await, vec![a.id.clone(), b.id.clone()]);
    decide(&*s, &b.id, 200, |t| t.complete(200, "cb", None))
        .await
        .unwrap();
    assert_eq!(s.next_lease_end().await.unwrap(), Some(1_100));
    // Recording a's lease end takes it off the timer.
    decide(&*s, &a.id, 2_000, |t| t.expire(2_000))
        .await
        .unwrap();
    assert_eq!(s.next_lease_end().await.unwrap(), None);
    assert!(s.claimed_tasks().await.unwrap().is_empty());
}

pub async fn idempotency_projection<S: Store>(s: Arc<S>) {
    let rec = |key: &str, at| IdempotencyRecord {
        person: "chris".into(),
        key: key.into(),
        request_hash: "h1".into(),
        result: serde_json::json!({ "ok": key }),
        created_at: at,
    };
    let get = |person: &'static str, key: &'static str| {
        let s = s.clone();
        async move {
            s.write(move |tx| Ok((Commit::default(), tx.get_idempotency(person, key)?)))
                .await
                .unwrap()
                .1
        }
    };
    assert_eq!(get("chris", "k1").await, None);
    put(
        &*s,
        Commit {
            appends: vec![],
            projections: vec![
                Projection::Idempotency(rec("k1", 100)),
                Projection::Idempotency(rec("k2", 5_000)),
            ],
        },
    )
    .await
    .unwrap();
    assert_eq!(get("chris", "k1").await, Some(rec("k1", 100)));
    assert_eq!(get("other", "k1").await, None, "keys are per person");
    assert_eq!(s.prune_idempotency(1_000).await.unwrap(), 1);
    assert_eq!(get("chris", "k1").await, None);
    assert_eq!(get("chris", "k2").await, Some(rec("k2", 5_000)));
    s.checkpoint().await.unwrap();
}

pub async fn max_at_and_head<S: Store>(s: Arc<S>) {
    assert_eq!(s.max_at().await.unwrap(), None);
    assert_eq!(s.head().await.unwrap(), 0);
    let mut e1 = ev("A", 1);
    e1.at = 500;
    let mut e2 = ev("A", 2);
    e2.at = 300; // a clock that stepped back
    put(&*s, one("s:a", ExpectedVersion::Any, vec![e1, e2]))
        .await
        .unwrap();
    assert_eq!(s.max_at().await.unwrap(), Some(500));
    assert_eq!(s.head().await.unwrap(), 2);
}

/// A filter on a task's own label matches its task events, joined from the
/// task, together with the `board.task` label each event stores (HANDOFF 6.3).
pub async fn task_events_match_task_labels<S: Store>(s: Arc<S>) {
    let t = create_task(&*s, "work", labels(&[("step", "fix")]), lease(60_000, 3), 1).await;
    let other = create_task(&*s, "work", Labels::new(), lease(60_000, 3), 1).await;
    decide(&*s, &t.id, 2, |t| {
        t.claim(2, &worker("w"), "c".into(), None)
    })
    .await
    .unwrap();
    let out = s
        .write({
            let id = t.id.clone();
            move |tx| {
                let task = tx.get_task(&id)?.unwrap().task;
                let d = task.complete(3, "c", Some("done".into()))?;
                let events = task_events(Some(&task), &d.events, "chris", 3);
                let mut next = task.clone();
                for e in &d.events {
                    next.apply(e, 3);
                }
                Ok((
                    Commit {
                        appends: vec![StreamAppend {
                            stream_id: task_stream(&id),
                            expected: ExpectedVersion::Exact(task.version),
                            events,
                        }],
                        projections: vec![Projection::Task(next)],
                    },
                    (),
                ))
            }
        })
        .await
        .unwrap()
        .0;
    // What `write` returns carries the joined labels too, so in-memory
    // matching (the server's fan-out) agrees with the store.
    assert!(out[0].meta.labels.contains(&("step".into(), "fix".into())));
    let types = |l: Vec<(&str, &str)>| {
        let s = s.clone();
        let f = EventFilter {
            stream_id: None,
            labels: l
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            limit: 100,
            ..Default::default()
        };
        async move {
            let page = s.read_events(&f).await.unwrap();
            for e in &page.events {
                assert!(f.matches(e));
            }
            page.events
                .iter()
                .map(|e| e.event_type.clone())
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(
        types(vec![("step", "fix")]).await,
        [
            "MessagePosted",
            "TaskCreated",
            "TaskClaimed",
            "TaskCompleted"
        ]
    );
    assert_eq!(
        types(vec![("step", "fix"), ("board.task", "completed")]).await,
        ["TaskCompleted"]
    );
    assert_eq!(
        types(vec![("board.task", "ready")]).await,
        ["TaskCreated", "TaskCreated"]
    );
    let _ = other;
}

/// Import loads a log into an empty store, positions kept, and builds the
/// projections from it (HANDOFF 13.4).
pub async fn import_keeps_positions<S: Store>(s: Arc<S>) {
    let m = root("work", "imported", labels(&[("step", "fix")]), 10);
    let created = Task::create(
        m.id.clone(),
        "work",
        &Actor::person("chris"),
        m.labels.clone(),
        &lease(60_000, 3),
    )
    .unwrap();
    let task = Task::genesis(&created, 10).unwrap();
    let claim = task
        .claim(20, &worker("w"), "c1".into(), None)
        .unwrap()
        .events;
    let mut new_events = vec![message_event(&m)];
    new_events.extend(task_events(
        None,
        std::slice::from_ref(&created),
        "chris",
        10,
    ));
    new_events.extend(task_events(Some(&task), &claim, "w via chris", 20));
    let streams = [
        thread_stream(&m.thread_id),
        task_stream(&m.id),
        task_stream(&m.id),
    ];
    let log: Vec<StoredEvent> = new_events
        .into_iter()
        .zip(streams)
        .enumerate()
        .map(|(i, (e, stream))| StoredEvent {
            position: i as u64 + 1,
            version: if i == 2 { 2 } else { 1 },
            stream_id: stream,
            event_type: e.event_type,
            schema: e.schema,
            data: e.data,
            actor: e.actor,
            at: e.at,
            meta: e.meta,
        })
        .collect();
    s.import(log.clone()).await.unwrap();
    let back = s.read_events(&EventFilter::after(0, 100)).await.unwrap();
    assert_eq!(back.head, 3);
    for (a, b) in log.iter().zip(&back.events) {
        assert_eq!(
            (a.position, &a.stream_id, a.version, &a.data),
            (b.position, &b.stream_id, b.version, &b.data)
        );
    }
    let stored = s.get_task(&m.id).await.unwrap().unwrap();
    assert_eq!(stored.created_position, 2);
    assert_eq!(stored.task.state, TaskState::Claimed);
    assert_eq!(
        s.get_message(&m.id, 30).await.unwrap().unwrap().labels["board.task"],
        "claimed"
    );
    assert_eq!(s.next_lease_end().await.unwrap(), Some(20 + 60_000));
    // Writes continue after the imported head.
    let out = put(&*s, one("s:x", ExpectedVersion::NoStream, vec![ev("A", 1)]))
        .await
        .unwrap();
    assert_eq!(out[0].position, 4);
    // Import only goes into an empty store.
    assert!(s.import(log).await.is_err());
}
