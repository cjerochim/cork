# The lead experiment

HANDOFF 17.5 asks for the same features to be run three ways, with the measures filled in from the log. This page is the plan for those runs and the place their results go.

**Status (2026-11-27):** the tooling is built and tested with fake agents. The fake lead and workers in `crates/cork-cli/tests/fleet.rs` and `cork-stress lead` stand in for them. **No real runs yet.** Real runs use paid models, so they wait for Chris to pick the features and say go.

## Setups

| Setup | How |
| --- | --- |
| A, by hand | A person splits the feature into tasks and runs one pool on one model. |
| B, lead and one model | `features/` goes to a lead pool. One worker pool, on one model, takes everything in `work/`. |
| C, lead with keywords and escalation | A lead pool, plus pools matched by keyword (for example `fast` on haiku, `mid` on sonnet, `strong` on opus), with `tasks escalate`. |

Use the same 5 to 10 small features from a real repo for every setup. Write each feature's definition of done before the runs.

## Measures

All but the first come from the log (`cork tasks history`, `cork debug export --channel features/`, `cork tasks list --label parent=<id>`):

| Measure | A | B | C |
| --- | --- | --- | --- |
| Done: features meeting their definition of done (person's review) | | | |
| Time from brief to feature completed | | | |
| Person's time: questions, and minutes from question to unblock | | | |
| Rework: reopens and follow-up children per feature | | | |
| Waste: failed and abandoned attempts, unused children | | | |
| Escalations: how many, from which model to which, and whether the retry succeeded | | | |
| Cost: model time per feature, by model, times each model's `cost` | | | |
| Routing gaps: unmatched tasks, and their wait before the first claim | | | |

## The decision this feeds

After the runs, add a short note here: should parent links, rollup or keyword matching move into the core, and why? The checks that labels can't make are a child of a missing parent, a feature claimed by a worker, a one-read rollup, and cancelling a whole feature in one commit.
