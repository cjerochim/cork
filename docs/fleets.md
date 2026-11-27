# Worker fleets and keywords

This is the user guide for HANDOFF sections 16 and 17. The spec says why; this page says how.

Everything here is client side. The board stores and filters plain labels, as in Milestone 1, and knows nothing about models. Every fleet command works against an unmodified Milestone 1 daemon.

## The model catalog

`models.toml` lists the keyword vocabulary and what each model can do. A file ending in `.json` with the same shape is read too.

```toml
[keywords]
rust = "Writes and reviews Rust"
docs = "Prose: READMEs, guides, changelogs"

[aliases]
documentation = "docs"

[model.sonnet]
command = ["claude", "-p", "Do the Cork task in $CORK_TASK_TEXT", "--model", "sonnet"]
tier = 2        # 1 is the weakest; a model also covers every tier below its own
cost = 3        # relative, for choosing the cheapest model and for reports
keywords = ["rust", "docs"]
```

Cork looks for the catalog in this order:

1. `--models FILE`, or `CORK_MODELS`.
2. The catalog named by the fleet config: `--config FILE`, `CORK_WORKERS`, or `~/.config/cork/workers.toml`.
3. `~/.config/cork/models.toml` (or `.json`).

Rules the catalog enforces:

- Keywords are lowercase `[a-z0-9_-]`. Aliases must point at a keyword.
- `tier-N` keywords are made from each model's `tier`. They cannot be declared, and a model cannot list them.
- A model that names an unknown keyword is refused, with the closest match.
- Unknown fields are refused, so a typo such as `keyword =` is caught.

## Asking for keywords

```bash
cork tasks create work "Port the parser to Rust" --needs rust,large-context
cork tasks create work "Redesign storage" --needs rust,architecture,tier-3
```

`--needs a,b` adds the labels `needs.a=yes` and `needs.b=yes`. Each word is checked against the catalog first:

- An unknown word is refused (exit 2), naming the closest match: `unknown keyword 'typscript'; did you mean 'typescript'?`
- An alias is stored as its keyword: `--needs ts` becomes `needs.typescript=yes`.

A task with no `needs.*` labels can go to any pool. Labels written another way (raw `--label`, the API) are not checked; `workers match` and `workers status` report them as unmatched.

## Who can take a task

```bash
cork workers keywords                 # the vocabulary, aliases, and which models cover each keyword
cork workers match --needs rust       # before the task exists
cork workers match <task-id>          # an existing task, against this machine's fleet
```

```text
$ cork workers match --needs rust
haiku: missing rust
sonnet: covers
opus: covers (costs 5x sonnet)
reviewer: missing rust
```

With a fleet config, `match` also judges each pool and names the one the fleet would give the task to. That answer comes from the same function the dispatcher uses. A pool covers a task when:

- its `select` matches the task's channel,
- the task's owner is the fleet's person, or the pool's `from` (`*` for anyone),
- the task has no `--for` assignee (those are for a named agent, not a fleet),
- every `needs.*` keyword is in the pool's list or tiers, and none is unknown,
- and, for an `exclusive = true` pool, the task asks for the pool's top tier.

Among the pools that cover a task, the cheapest wins: by `cost` (a model without a cost comes after any with one), then tier, then pool name. `match` exits 3 when nothing covers the task.

## Watching a feature's children

`cork wait` takes `--type` to wake on task events instead of messages:

```bash
cork wait work/ --label parent=$F --type TaskCompleted --type TaskFailed --type TaskBlocked --type LeaseExpired
```
