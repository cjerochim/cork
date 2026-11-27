//! Task dependencies and replacement (HANDOFF 6.8).
//!
//! A task declares what it waits for (`depends_on`) and a retry declares what
//! it replaces (`replaces`). Everything else is derived from those two facts:
//!
//! - The **head** of an id is the newest task in its replacement chain,
//!   reached by following `replaced_by`.
//! - A dependency is **met** when its head is `completed`.
//! - A task is `waiting` while any dependency is not met. The board writes
//!   `DependenciesMet` in the commit that meets the last one.
//! - A waiting task is **stuck** while the head of a dependency is `failed`
//!   or `canceled` (a view, not a state).
//!
//! The functions here are pure. They read other tasks through
//! [`TaskLookup`], which the server backs with its write transaction (and
//! the tasks its commit has already changed).

use crate::Millis;
use crate::actor::Actor;
use crate::error::{BoardError, ErrorCode, Result};
use crate::task::{Task, TaskEvent, TaskState};

/// The most dependencies one task may declare (6.8.5).
pub const MAX_DEPENDENCIES: usize = 64;
/// The longest replacement chain (6.8.3).
pub const MAX_REPLACEMENTS: usize = 16;

/// Reads other tasks, as of the decision being made.
pub trait TaskLookup {
    fn task(&mut self, id: &str) -> Result<Option<Task>>;
}

impl<F: FnMut(&str) -> Result<Option<Task>>> TaskLookup for F {
    fn task(&mut self, id: &str) -> Result<Option<Task>> {
        self(id)
    }
}

fn must(look: &mut dyn TaskLookup, id: &str) -> Result<Task> {
    look.task(id)?
        .ok_or_else(|| BoardError::not_found(format!("task {id}")))
}

/// The newest task in `id`'s replacement chain.
pub fn head(look: &mut dyn TaskLookup, id: &str) -> Result<Task> {
    let mut t = must(look, id)?;
    for _ in 0..=MAX_REPLACEMENTS {
        match t.replaced_by.clone() {
            Some(next) => t = must(look, &next)?,
            None => return Ok(t),
        }
    }
    Err(BoardError::internal(format!(
        "replacement chain from {id} is longer than {MAX_REPLACEMENTS}"
    )))
}

/// `id` and every task it replaced, newest first. Each of them resolves to
/// the same head as `id` when `id` is a head.
pub fn chain_back(look: &mut dyn TaskLookup, id: &str) -> Result<Vec<String>> {
    let mut out = vec![id.to_string()];
    let mut t = must(look, id)?;
    while let Some(prev) = t.replaces.clone() {
        if out.len() > MAX_REPLACEMENTS {
            return Err(BoardError::internal(format!(
                "replacement chain to {id} is longer than {MAX_REPLACEMENTS}"
            )));
        }
        t = must(look, &prev)?;
        out.push(prev);
    }
    Ok(out)
}

/// The declared ids whose head has not completed.
pub fn open_dependencies(look: &mut dyn TaskLookup, depends_on: &[String]) -> Result<Vec<String>> {
    let mut open = vec![];
    for d in depends_on {
        if head(look, d)?.state != TaskState::Completed {
            open.push(d.clone());
        }
    }
    Ok(open)
}

/// The dependency fields of a new task, decided once at create time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolved {
    pub depends_on: Vec<String>,
    pub replaces: Option<String>,
    /// The declared ids not met now. Not empty: the task starts `waiting`.
    pub open: Vec<String>,
}

/// Check and resolve what a new task declares (6.8.3, 6.8.5).
///
/// - `after`: ids of existing tasks. Duplicates are dropped; at most
///   [`MAX_DEPENDENCIES`]. Each must exist.
/// - `replaces`: the task this one takes over. It inherits that task's
///   `depends_on` exactly, so it cannot also declare `after`.
pub fn resolve_create(
    look: &mut dyn TaskLookup,
    after: &[String],
    replaces: Option<&str>,
) -> Result<Resolved> {
    let depends_on = match replaces {
        Some(old) => {
            if !after.is_empty() {
                return Err(BoardError::invalid(
                    "a retry takes over its task's dependencies: give --replaces or --after, not both",
                ));
            }
            must(look, old)?.depends_on
        }
        None => dedupe(after)?,
    };
    for d in &depends_on {
        must(look, d)?;
    }
    let open = open_dependencies(look, &depends_on)?;
    Ok(Resolved {
        depends_on,
        replaces: replaces.map(str::to_string),
        open,
    })
}

/// Drop duplicates, keep the first order, and check the count.
pub fn dedupe(ids: &[String]) -> Result<Vec<String>> {
    let mut out: Vec<String> = vec![];
    for id in ids {
        let id = id.trim();
        if id.is_empty() {
            return Err(BoardError::invalid("a dependency id is empty"));
        }
        if !out.iter().any(|o| o == id) {
            out.push(id.to_string());
        }
    }
    if out.len() > MAX_DEPENDENCIES {
        return Err(BoardError::invalid(format!(
            "a task may depend on at most {MAX_DEPENDENCIES} tasks, not {}",
            out.len()
        )));
    }
    Ok(out)
}

/// How many replacements lead to `old`, for [`Task::replace`].
pub fn chain_len(look: &mut dyn TaskLookup, old: &str) -> Result<usize> {
    Ok(chain_back(look, old)?.len() - 1)
}

/// What a commit did to a task that other tasks may wait on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settled {
    pub id: String,
    /// `completed`, `failed` or `canceled`.
    pub outcome: TaskState,
    pub reason: Option<String>,
}

impl Settled {
    /// The settles in a decision's events. `TaskReplaced` is not one: a
    /// retry takes the task's place, so nothing is readied or failed.
    pub fn from_events(id: &str, events: &[TaskEvent]) -> Vec<Settled> {
        events
            .iter()
            .filter_map(|e| {
                let (outcome, reason) = match e {
                    TaskEvent::TaskCompleted { .. } => (TaskState::Completed, None),
                    TaskEvent::TaskFailed { reason, .. } => {
                        (TaskState::Failed, Some(reason.clone()))
                    }
                    TaskEvent::TaskCanceled { reason } => (TaskState::Canceled, reason.clone()),
                    _ => return None,
                };
                Some(Settled {
                    id: id.to_string(),
                    outcome,
                    reason,
                })
            })
            .collect()
    }
}

/// The events a settle writes on its waiting dependents, in the same commit
/// (6.8.5, Decision 1).
///
/// `dependents` are the tasks declaring a dependency on `settled.id` or a
/// task it replaced, oldest first; `look` sees the commit so far. Only
/// waiting tasks whose dependency resolves to `settled.id` are written to,
/// in the order given, so tasks ready in one commit keep the queue's
/// first-come, first-served order (6.5):
///
/// - completed: `DependenciesMet` on each whose dependencies are now all met;
/// - failed or canceled: one `DependencyFailed` on each.
pub fn on_settled(
    look: &mut dyn TaskLookup,
    settled: &Settled,
    dependents: Vec<Task>,
) -> Result<Vec<(Task, TaskEvent)>> {
    let mut out: Vec<(Task, TaskEvent)> = vec![];
    for d in dependents {
        if out.iter().any(|(t, _)| t.id == d.id) {
            continue;
        }
        if d.state != TaskState::Waiting {
            continue;
        }
        let mut via = None;
        let mut all_met = true;
        for dep in &d.depends_on {
            let h = head(look, dep)?;
            if h.id == settled.id && via.is_none() {
                via = Some(dep.clone());
            }
            if h.state != TaskState::Completed {
                all_met = false;
            }
        }
        let Some(via) = via else { continue };
        let event = match settled.outcome {
            TaskState::Completed if all_met => TaskEvent::DependenciesMet {
                met_by: settled.id.clone(),
            },
            TaskState::Completed => continue,
            s @ (TaskState::Failed | TaskState::Canceled) => TaskEvent::DependencyFailed {
                dependency: via,
                head: settled.id.clone(),
                state: s,
                reason: settled.reason.clone(),
            },
            _ => continue,
        };
        out.push((d, event));
    }
    Ok(out)
}

/// One dependency as a reader sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct DependencyView {
    pub declared: String,
    pub head: Task,
    /// The head's effective state at `now`.
    pub state: TaskState,
}

/// Each dependency of `task` with its head and the head's effective state.
pub fn view(look: &mut dyn TaskLookup, task: &Task, now: Millis) -> Result<Vec<DependencyView>> {
    task.depends_on
        .iter()
        .map(|d| {
            let h = head(look, d)?;
            Ok(DependencyView {
                declared: d.clone(),
                state: h.effective_state(now),
                head: h,
            })
        })
        .collect()
}

/// A waiting task is stuck while the head of one of its dependencies is
/// `failed` or `canceled` by effective state (Decision 1).
pub fn is_stuck(task_state: TaskState, deps: &[DependencyView]) -> bool {
    task_state == TaskState::Waiting
        && deps
            .iter()
            .any(|d| matches!(d.state, TaskState::Failed | TaskState::Canceled))
}

/// The error for claiming a waiting task by id, naming what it waits on.
pub fn waiting_error(task: &Task, deps: &[DependencyView]) -> BoardError {
    let unmet: Vec<String> = deps
        .iter()
        .filter(|d| d.head.state != TaskState::Completed)
        .map(|d| {
            if d.head.id == d.declared {
                format!("{} ({})", d.declared, d.state)
            } else {
                format!("{} → {} ({})", d.declared, d.head.id, d.state)
            }
        })
        .collect();
    BoardError::new(
        ErrorCode::InvalidState,
        format!("task {} is waiting on {}", task.id, unmet.join(", ")),
    )
}

/// Who a `DependencyFailed` on `dependent` mentions: the handle that created
/// it, and its owner if that is a different handle (Decision 1).
pub fn mentions(dependent: &Task, creator: Option<&Actor>) -> Vec<String> {
    let mut out = vec![];
    if let Some(c) = creator {
        out.push(c.handle().to_string());
    }
    if !out.contains(&dependent.owner) {
        out.push(dependent.owner.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Labels;
    use crate::task::{CreateTask, Decision};
    use std::collections::HashMap;

    const T0: Millis = 1_000_000;

    fn owner() -> Actor {
        Actor::new("chris", Some("lead".into())).unwrap()
    }
    fn worker() -> Actor {
        Actor::new("chris", Some("w".into())).unwrap()
    }

    /// A tiny board: tasks by id, with decisions applied in place.
    #[derive(Default)]
    struct World {
        tasks: HashMap<String, Task>,
        n: u32,
    }

    impl World {
        fn look(&self) -> impl FnMut(&str) -> Result<Option<Task>> + '_ {
            |id: &str| Ok(self.tasks.get(id).cloned())
        }
        fn create(&mut self, after: &[&str], replaces: Option<&str>) -> Result<String> {
            self.n += 1;
            let id = format!("t{:02}", self.n);
            let after: Vec<String> = after.iter().map(|s| s.to_string()).collect();
            let r = resolve_create(&mut self.look(), &after, replaces)?;
            if let Some(old) = replaces {
                let o = self.tasks[old].clone();
                let chain = chain_len(&mut self.look(), old)?;
                let d = o.replace(T0, &owner(), &id, chain)?;
                self.apply(old, d);
            }
            let e = Task::create_with(
                id.clone(),
                "work",
                &owner(),
                Labels::new(),
                &CreateTask::default(),
                r,
            )?;
            self.tasks.insert(id.clone(), Task::genesis(&e, T0)?);
            Ok(id)
        }
        fn apply(&mut self, id: &str, d: Decision) {
            let t = self.tasks.get_mut(id).unwrap();
            for e in &d.events {
                t.apply(e, T0);
            }
        }
        fn state(&self, id: &str) -> TaskState {
            self.tasks[id].state
        }
        /// Claim and settle `id`, then run the dependency rule. Returns the
        /// events written on dependents.
        fn settle(&mut self, id: &str, how: &str) -> Vec<(String, TaskEvent)> {
            let t = self.tasks[id].clone();
            let d = t.claim(T0, &worker(), "c".into(), None).unwrap();
            self.apply(id, d);
            let t = self.tasks[id].clone();
            let d = match how {
                "complete" => t.complete(T0, "c", Some(format!("{id} done"))),
                "fail" => t.fail(T0, "c", "broke".into()),
                _ => unreachable!(),
            }
            .unwrap();
            self.apply(id, d);
            self.run_rule(id)
        }
        fn cancel(&mut self, id: &str) -> Vec<(String, TaskEvent)> {
            let d = self.tasks[id]
                .cancel(T0, &owner(), Some("no".into()))
                .unwrap();
            self.apply(id, d);
            self.run_rule(id)
        }
        fn run_rule(&mut self, id: &str) -> Vec<(String, TaskEvent)> {
            let t = self.tasks[id].clone();
            let settled = Settled {
                id: id.to_string(),
                outcome: t.state,
                reason: t.reason.clone(),
            };
            let chain = chain_back(&mut self.look(), id).unwrap();
            let mut dependents: Vec<Task> = self
                .tasks
                .values()
                .filter(|t| t.depends_on.iter().any(|d| chain.contains(d)))
                .cloned()
                .collect();
            // Ids are made in creation order here.
            dependents.sort_by(|a, b| a.id.cmp(&b.id));
            let out = on_settled(&mut self.look(), &settled, dependents).unwrap();
            let mut written = vec![];
            for (t, e) in out {
                self.tasks.get_mut(&t.id).unwrap().apply(&e, T0);
                written.push((t.id, e));
            }
            written
        }
    }

    fn types(w: &[(String, TaskEvent)]) -> Vec<(&str, &'static str)> {
        w.iter()
            .map(|(id, e)| (id.as_str(), e.event_type()))
            .collect()
    }

    #[test]
    fn no_dependencies_starts_ready() {
        let mut w = World::default();
        let a = w.create(&[], None).unwrap();
        assert_eq!(w.state(&a), TaskState::Ready);
    }

    #[test]
    fn waits_until_every_dependency_completes() {
        let mut w = World::default();
        let a = w.create(&[], None).unwrap();
        let b = w.create(&[], None).unwrap();
        let c = w.create(&[&a, &b, &a], None).unwrap();
        assert_eq!(
            w.tasks[&c].depends_on,
            [a.clone(), b.clone()],
            "duplicates dropped"
        );
        assert_eq!(w.state(&c), TaskState::Waiting);
        assert!(w.settle(&a, "complete").is_empty(), "b is still open");
        assert_eq!(w.state(&c), TaskState::Waiting);
        let written = w.settle(&b, "complete");
        assert_eq!(types(&written), [(c.as_str(), "DependenciesMet")]);
        assert!(matches!(&written[0].1, TaskEvent::DependenciesMet { met_by } if *met_by == b));
        assert_eq!(w.state(&c), TaskState::Ready);
    }

    #[test]
    fn completed_dependencies_start_ready() {
        let mut w = World::default();
        let a = w.create(&[], None).unwrap();
        w.settle(&a, "complete");
        let b = w.create(&[&a], None).unwrap();
        assert_eq!(w.state(&b), TaskState::Ready);
    }

    #[test]
    fn create_checks_ids_count_and_flags() {
        let mut w = World::default();
        assert_eq!(
            w.create(&["nope"], None).unwrap_err().code,
            ErrorCode::NotFound
        );
        let a = w.create(&[], None).unwrap();
        assert_eq!(
            w.create(&[&a], Some(&a)).unwrap_err().code,
            ErrorCode::InvalidInput,
            "--after with --replaces"
        );
        assert_eq!(
            w.create(&[" "], None).unwrap_err().code,
            ErrorCode::InvalidInput
        );
        let many: Vec<String> = (0..=MAX_DEPENDENCIES).map(|i| format!("x{i}")).collect();
        assert_eq!(dedupe(&many).unwrap_err().code, ErrorCode::InvalidInput);
        assert_eq!(
            dedupe(&many[..MAX_DEPENDENCIES]).unwrap().len(),
            MAX_DEPENDENCIES
        );
    }

    #[test]
    fn failed_dependency_is_surfaced_not_spread() {
        let mut w = World::default();
        let a = w.create(&[], None).unwrap();
        let b = w.create(&[&a], None).unwrap();
        let c = w.create(&[&a], None).unwrap();
        let written = w.settle(&a, "fail");
        assert_eq!(
            types(&written),
            [
                (b.as_str(), "DependencyFailed"),
                (c.as_str(), "DependencyFailed")
            ]
        );
        assert!(matches!(
            &written[0].1,
            TaskEvent::DependencyFailed { dependency, head, state: TaskState::Failed, reason: Some(r) }
                if *dependency == a && *head == a && r == "broke"
        ));
        assert_eq!(
            w.state(&b),
            TaskState::Waiting,
            "the dependent is not failed"
        );
        let deps = view(&mut w.look(), &w.tasks[&b], T0).unwrap();
        assert!(is_stuck(TaskState::Waiting, &deps));
        assert!(!is_stuck(TaskState::Ready, &deps));
    }

    #[test]
    fn cancel_is_surfaced_too() {
        let mut w = World::default();
        let a = w.create(&[], None).unwrap();
        let b = w.create(&[&a], None).unwrap();
        let written = w.cancel(&a);
        assert!(matches!(
            &written[..],
            [(id, TaskEvent::DependencyFailed { state: TaskState::Canceled, .. })] if *id == b
        ));
    }

    #[test]
    fn a_retry_takes_over_and_unsticks_with_nothing_written() {
        let mut w = World::default();
        let root = w.create(&[], None).unwrap();
        let a = w.create(&[&root], None).unwrap();
        let b = w.create(&[&a], None).unwrap();
        w.settle(&root, "complete");
        w.settle(&a, "fail");
        assert!(is_stuck(
            w.state(&b),
            &view(&mut w.look(), &w.tasks[&b], T0).unwrap()
        ));

        let a2 = w.create(&[], Some(&a)).unwrap();
        assert_eq!(
            w.tasks[&a2].depends_on,
            vec![root.clone()],
            "inherits depends_on"
        );
        assert_eq!(w.tasks[&a2].replaces.as_deref(), Some(a.as_str()));
        assert_eq!(w.tasks[&a].replaced_by.as_deref(), Some(a2.as_str()));
        assert_eq!(w.state(&a), TaskState::Failed, "a failed task stays failed");
        assert_eq!(w.state(&a2), TaskState::Ready, "its dependency is met");
        // b declared a, which now resolves to a2: no longer stuck.
        let deps = view(&mut w.look(), &w.tasks[&b], T0).unwrap();
        assert_eq!(deps[0].head.id, a2);
        assert!(!is_stuck(w.state(&b), &deps));
        assert_eq!(w.tasks[&b].depends_on, vec![a.clone()], "b was not edited");

        let written = w.settle(&a2, "complete");
        assert_eq!(types(&written), [(b.as_str(), "DependenciesMet")]);
    }

    #[test]
    fn replacement_rules() {
        let mut w = World::default();
        let a = w.create(&[], None).unwrap();
        // A stranger may not replace.
        let t = w.tasks[&a].clone();
        assert_eq!(
            t.replace(T0, &Actor::person("dana"), "x", 0)
                .unwrap_err()
                .code,
            ErrorCode::TaskNotOwned
        );
        // Not while claimed.
        let d = t.claim(T0, &worker(), "c".into(), None).unwrap();
        w.apply(&a, d);
        assert_eq!(
            w.create(&[], Some(&a)).unwrap_err().code,
            ErrorCode::InvalidState
        );
        // Not once completed.
        let t = w.tasks[&a].clone();
        w.apply(&a, t.complete(T0, "c", None).unwrap());
        assert_eq!(
            w.create(&[], Some(&a)).unwrap_err().code,
            ErrorCode::InvalidState
        );
        // A ready task becomes canceled; a second replacement names the first.
        let b = w.create(&[], None).unwrap();
        let b2 = w.create(&[], Some(&b)).unwrap();
        assert_eq!(w.state(&b), TaskState::Canceled);
        let e = w.create(&[], Some(&b)).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidState);
        assert!(e.message.contains(&b2), "{}", e.message);
        // A replaced task cannot be reopened.
        let e = w.tasks[&b].reopen(T0, &owner(), vec![]).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidState);
        assert!(e.message.contains(&b2));
    }

    #[test]
    fn chains_are_bounded() {
        let mut w = World::default();
        let mut cur = w.create(&[], None).unwrap();
        for _ in 0..MAX_REPLACEMENTS {
            cur = w.create(&[], Some(&cur)).unwrap();
        }
        assert_eq!(chain_len(&mut w.look(), &cur).unwrap(), MAX_REPLACEMENTS);
        assert_eq!(
            w.create(&[], Some(&cur)).unwrap_err().code,
            ErrorCode::InvalidState
        );
    }

    #[test]
    fn a_waiting_task_can_be_replaced_and_its_retry_waits_too() {
        let mut w = World::default();
        let a = w.create(&[], None).unwrap();
        let b = w.create(&[&a], None).unwrap();
        let b2 = w.create(&[], Some(&b)).unwrap();
        assert_eq!(w.state(&b), TaskState::Canceled);
        assert_eq!(w.state(&b2), TaskState::Waiting);
        // b is no longer waiting, so only b2 readies.
        let written = w.settle(&a, "complete");
        assert_eq!(types(&written), [(b2.as_str(), "DependenciesMet")]);
    }

    #[test]
    fn reopen_resolves_again() {
        let mut w = World::default();
        let a = w.create(&[], None).unwrap();
        let b = w.create(&[&a], None).unwrap();
        w.settle(&a, "complete");
        assert_eq!(w.state(&b), TaskState::Ready);
        // Reopen the dependency: b, already ready, is not touched.
        let d = w.tasks[&a].reopen(T0, &owner(), vec![]).unwrap();
        w.apply(&a, d);
        assert_eq!(w.state(&b), TaskState::Ready);
        // Finish b, then reopen it while a is open: it waits again.
        w.settle(&b, "complete");
        let open = open_dependencies(&mut w.look(), &w.tasks[&b].depends_on).unwrap();
        assert_eq!(open, vec![a.clone()]);
        let d = w.tasks[&b].reopen(T0, &owner(), open).unwrap();
        w.apply(&b, d);
        assert_eq!(w.state(&b), TaskState::Waiting);
        let written = w.settle(&a, "complete");
        assert_eq!(types(&written), [(b.as_str(), "DependenciesMet")]);
    }

    #[test]
    fn waiting_task_cannot_be_claimed_and_says_why() {
        let mut w = World::default();
        let a = w.create(&[], None).unwrap();
        let b = w.create(&[&a], None).unwrap();
        let t = &w.tasks[&b];
        assert_eq!(
            t.claim(T0, &worker(), "c".into(), None).unwrap_err().code,
            ErrorCode::InvalidState
        );
        let deps = view(&mut w.look(), t, T0).unwrap();
        let e = waiting_error(t, &deps);
        assert!(e.message.contains(&format!("{a} (ready)")), "{}", e.message);
        // Cancel still works on a waiting task.
        let d = w.tasks[&b].cancel(T0, &owner(), None).unwrap();
        w.apply(&b, d);
        assert_eq!(w.state(&b), TaskState::Canceled);
    }

    #[test]
    fn mentions_creator_and_owner_once() {
        let mut w = World::default();
        let a = w.create(&[], None).unwrap();
        let t = &w.tasks[&a];
        assert_eq!(mentions(t, Some(&owner())), ["lead", "chris"]);
        assert_eq!(mentions(t, Some(&Actor::person("chris"))), ["chris"]);
        assert_eq!(mentions(t, None), ["chris"]);
    }

    #[test]
    fn events_round_trip_and_old_shapes_read() {
        for e in [
            TaskEvent::TaskReplaced { by: "n".into() },
            TaskEvent::DependenciesMet { met_by: "y".into() },
            TaskEvent::DependencyFailed {
                dependency: "x".into(),
                head: "y".into(),
                state: TaskState::Failed,
                reason: None,
            },
            TaskEvent::TaskReopened {
                open_dependencies: vec!["a".into()],
            },
        ] {
            let j = serde_json::to_value(&e).unwrap();
            assert_eq!(serde_json::from_value::<TaskEvent>(j).unwrap(), e);
        }
        // Milestone 1 shapes, with no dependency fields.
        let old: TaskEvent = serde_json::from_value(serde_json::json!({
            "type": "TaskCreated", "task_id": "t", "channel": "dev", "owner": "chris",
            "lease_ms": 1000, "max_attempts": 3, "labels": {}
        }))
        .unwrap();
        let t = Task::genesis(&old, T0).unwrap();
        assert_eq!(t.state, TaskState::Ready);
        assert!(t.depends_on.is_empty());
        let old: TaskEvent =
            serde_json::from_value(serde_json::json!({"type": "TaskReopened"})).unwrap();
        assert_eq!(
            old,
            TaskEvent::TaskReopened {
                open_dependencies: vec![]
            }
        );
    }
}
