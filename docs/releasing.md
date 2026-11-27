# Releasing Cork

**Status:** agreed plan. Nothing here is built or published yet. Chris approved semver and a single `0.1.0` first release on 2026-11-27, and accepted the recommended defaults for the rest (section 9). The release tooling is built only when Chris says go.

This plan covers four things: how Cork numbers its versions, how a changelog gets written, how tags and GitHub releases happen, and how Cork later reaches crates.io and npm. The aim is a release flow that is declared in config and run by CI. A person decides *when* to release and *what the version is* by merging one pull request. Nothing else is done by hand.

## 1. Version numbers

Cork uses [Semantic Versioning](https://semver.org): `MAJOR.MINOR.PATCH`.

"0.01, 0.02" reads as `0.1.0`, `0.2.0`. Cargo and npm both require three numbers, and semver does not allow leading zeros, so `0.01` is not a valid version for either.

While Cork is below 1.0:

| Bump | When | Example |
|---|---|---|
| Minor (`0.1.0` → `0.2.0`) | A milestone or a feature lands. May include breaking changes. | Task dependencies |
| Patch (`0.2.0` → `0.2.1`) | Bug fixes and docs only. Never breaks anything. | A lease timer fix |
| Major (`1.0.0`) | Once, when the contracts in section 7 are declared frozen. | |

After 1.0, normal semver applies: a breaking change needs `2.0.0`.

### 1.1 Three different version numbers

Cork already has three version numbers. This plan changes only the first.

| Number | Where | What it means | Changes when |
|---|---|---|---|
| **Release version** | `[workspace.package] version` in `Cargo.toml`, reported as `server_version` by `/health` | Which build of Cork you have | Every release |
| **API version** | `API_VERSION` in `cork-api`, the `/v1` prefix | Which HTTP contract the daemon speaks | Only on a breaking API change, which means `/v2` served beside `/v1` (HANDOFF 7) |
| **Event schema** | `schema` on each event type (HANDOFF 5) | The shape of one event type's `data` | When that event's shape changes, with an upcaster |

So `0.2.0` and `0.3.0` can both speak API version 1. The CLI already errors on a different API version and only warns on a different server version (HANDOFF 8.4), which is the right behaviour for this scheme.

### 1.2 One version for every crate

All crates already share `version.workspace = true`. Keep it that way: every crate in the workspace is released together, at the same version, from the same tag. That keeps `cork-cli 0.3.0` always paired with `cork-core 0.3.0`, and needs only one tag per release (`v0.3.0`).

## 2. Milestones mapped to versions

No tags exist yet, and `Cargo.toml` has said `0.1.0` since Milestone 1. Milestones 1, 2 and 3 are all merged into `main`.

**Decided (2026-11-27):** the first release is `0.1.0`, cut from today's `main`. Its changelog has one section per milestone, so the history is still readable. Each later milestone is one minor version.

| Version | Contents | HANDOFF |
|---|---|---|
| `0.1.0` | Milestones 1–3: the board, tasks and leases, `tasks run`, worker fleets and keyword routing, escalation and the lead skill, usage capture, `cork stats`, `/metrics` | 11, 16, 17, 18 |
| `0.2.0` | Task dependencies and the "waiting on tasks" state | 6.8 |
| `0.3.0` | Server mode with SQLite | 13 |
| `0.4.0`+ | Grants, the web UI, MCP adapter, OpenTelemetry, in whatever order Chris picks | 13.3, 14, 18.5 |
| `1.0.0` | The contracts in section 7 are frozen | |

**Alternative:** tag the three old milestone merges as `v0.1.0`, `v0.2.0` and `v0.3.0`. This gives a tag per milestone, but those commits all say `0.1.0` in `Cargo.toml`, so a binary built from the `v0.3.0` tag would report itself as `0.1.0`. Retro tags would have to be source-only, with no binaries. That is why it is not the recommendation.

Patch releases (`0.2.1`) can happen at any time between milestones.

## 3. The changelog

One file, `CHANGELOG.md`, at the repo root, in the [Keep a Changelog](https://keepachangelog.com) layout: a section per version, newest first, with `Added`, `Changed`, `Fixed` and `Removed` groups, and an `Unreleased` section at the top.

It is generated, not typed. The release tool (section 4) reads the commit messages since the last tag and writes the new section into the release pull request. Before merging, anyone can edit that section in the PR to reword or drop lines.

For that to work, commit messages need a small, fixed prefix that says which group a change belongs to. This is the [Conventional Commits](https://www.conventionalcommits.org) style:

| Prefix | Changelog group | Bump it implies before 1.0 |
|---|---|---|
| `feat:` | Added | minor |
| `fix:` | Fixed | patch |
| `feat!:` or `fix!:` | Changed (breaking) | minor |
| `docs:`, `spec:` | Not listed (or a Docs group, if wanted) | none |
| `test:`, `ci:`, `chore:`, `refactor:` | Not listed | none |

Today's messages are close already (`spec: ...`, `cork stats: ...`, `fleet tests: rustfmt`). The change is only to start with one of the words above. A CI check on PR titles can hold the rule, so nobody has to remember it.

The `0.1.0` section is the one exception. It is written once by hand from the merged PRs #1–#14, because the old commits do not carry prefixes.

## 4. How a release happens

Two tools, each with one config file in the repo. Both are widely used for Rust CLIs.

- **[release-plz](https://release-plz.dev)** owns versions, the changelog and tags. Config: `release-plz.toml`.
- **[dist](https://opensource.axo.dev/cargo-dist/)** (formerly cargo-dist) owns binaries, installers and the GitHub release. Config: `[workspace.metadata.dist]` in `Cargo.toml` (or `dist-workspace.toml`). It generates its own workflow, `.github/workflows/release.yml`.

The flow:

```
merge PRs to main
      │
      ▼
release-plz (on every push to main)
  opens or updates one PR: "Release v0.2.0"
  - bumps the workspace version and every internal `version = "..."` pin
  - writes the new CHANGELOG.md section
      │
      │  Chris edits the version or the notes if wanted, then merges
      ▼
release-plz (on the merge)
  creates tag v0.2.0
  (later: runs `cargo publish` for each crate, in dependency order)
      │
      ▼
dist (on the tag)
  builds binaries for each platform
  creates the GitHub release "v0.2.0" with the changelog section as notes
  attaches archives, checksums, a shell installer
  (later: publishes the npm package and a Homebrew formula)
```

What this gives:

- **Releasing is one merge.** The release PR is the only manual step, and it is a review, not a procedure.
- **The version is declared, not computed in someone's head.** release-plz proposes the next version from the commit prefixes. Chris can overwrite it in the PR (for example, to call a milestone `0.2.0` even if only fixes landed).
- **Nothing is published by a laptop.** Every artifact is built by CI from a tag.
- **Normal CI is unchanged.** `ci.yml` keeps running fmt, clippy and tests. The release PR has to pass it like any other PR.

### 4.1 One setup detail

A tag pushed by a workflow using the default `GITHUB_TOKEN` does not start other workflows. That is a GitHub rule. So release-plz needs a token that can trigger dist's workflow: either a fine-grained personal access token or a small GitHub App, stored as a repository secret (for example `RELEASE_TOKEN`). Chris creates this once. It is the only secret needed until crates.io and npm are added.

### 4.2 Pre-releases

To try a release before it is real, set the version in the release PR to `0.2.0-rc.1`. dist marks the GitHub release as a pre-release, and package registries will not install it by default. Then release `0.2.0` when happy.

## 5. Platforms

Cork has Unix-only code (process groups and signals in `tasks run`, `cork workers` and the client), with fallbacks behind `cfg(not(unix))` that have never been run.

Proposed first set of binaries:

| Target | Runner |
|---|---|
| macOS Apple silicon (`aarch64-apple-darwin`) | `macos-14` |
| macOS Intel (`x86_64-apple-darwin`) | `macos-13` |
| Linux x86-64 (`x86_64-unknown-linux-gnu`) | `ubuntu-latest` |
| Linux ARM64 (`aarch64-unknown-linux-gnu`) | `ubuntu-24.04-arm` |

Windows is left out until someone runs the test suite on it. `rusqlite` is built with `bundled`, so every target needs a C compiler. That is why each target builds on its own native runner rather than by cross-compiling.

## 6. Packages, in order

Each step is a few lines of config and one secret. Each waits for Chris to say go.

### 6.1 GitHub Releases (first)

Comes with section 4. People install with the shell installer dist generates:

```sh
curl -LsSf https://github.com/cjerochim/cork/releases/latest/download/cork-cli-installer.sh | sh
```

or by downloading the archive for their platform.

### 6.2 crates.io (second)

- The CLI crate is `cork-cli` (the name `cork` is taken, HANDOFF 12). It installs a binary called `cork`: `cargo install cork-cli`.
- `cargo publish` needs every dependency on crates.io too, so these crates must be published, in this order: `cork-core`, `cork-api`, `cork-store`, `cork-store-sqlite`, `cork-server`, `cork-client`, `cork-stats`, `cork-fleet`, `cork-cli`. release-plz works out the order itself.
- `cork-stress` stays unpublished (`publish = false` is already set).
- All nine names are free on crates.io as of 2026-11-27. crates.io has no way to reserve a name without publishing, so they could be taken before the first publish.
- Publishing is permanent. A version can be yanked but never deleted or reused, which is the main reason this step waits.
- Secret: a crates.io API token as `CARGO_REGISTRY_TOKEN`. Later, crates.io trusted publishing can replace the token.

### 6.3 npm (third)

The npm package does not contain Rust. It is a small installer that downloads the right prebuilt `cork` binary from the GitHub release, so `npm install -g cork-cli` and `npx cork-cli` work for people who live in Node. dist generates and publishes it from the same tag (`installers = ["npm"]`), so the npm version always equals the release version.

- The name `cork` is taken on npm. `cork-cli` is free as of 2026-11-27. A scope such as `@cjerochim/cork` is also possible.
- Secret: an npm automation token as `NPM_TOKEN`.

### 6.4 Homebrew (optional)

dist can also publish a formula to a tap repo (for example `cjerochim/homebrew-tap`), giving `brew install cjerochim/tap/cork`. This needs a second repo, so it is optional.

## 7. What 1.0 means

`1.0.0` is a promise that these will not break without a major version:

- The HTTP API under `/v1` (routes, bodies, outcomes, error codes; HANDOFF 7).
- The event log format and every event type's schema, read by any later version (HANDOFF 5, 6).
- CLI exit codes (HANDOFF 8.6) and the `--json` output shapes.
- The config files (`~/.config/cork`, `models.toml`).

Most of these are already written as stable in the spec. 1.0 is when they have also held up through server mode and real use.

## 8. What changes in the repo

When Chris says go, one PR adds:

- `CHANGELOG.md` with the hand-written `0.1.0` section.
- `release-plz.toml`, and `.github/workflows/release-plz.yml`.
- `[workspace.metadata.dist]` in `Cargo.toml`, and the `release.yml` that `dist init` generates.
- A PR title check for the prefixes in section 3.
- A short "Install" section in the README.

No Rust code changes. crates.io and npm are turned on later, each by a one-line config change and a secret.

## 9. Decisions

1. ~~**First release.**~~ **Decided 2026-11-27:** semver, and one `0.1.0` from `main` covering Milestones 1–3, no retro tags.
2. **Changelog.** Generated from commit prefixes, editable in the release PR.
3. **Order of packages.** GitHub releases first, then crates.io, then npm. No Homebrew for now.
4. **npm name.** `cork-cli`, the same as the crate.
5. **Platforms.** macOS and Linux only, until Windows has had a test run.

Choices 2 to 5 are the recommended defaults, accepted by Chris on 2026-11-27 ("continue"). Any of them can still change before the release tooling is built.
