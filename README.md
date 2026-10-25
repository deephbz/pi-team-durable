# @hypercarrier/pi-team-durable

Pi Team Bright semantics on [pi-durable](https://www.npmjs.com/package/@earendil-works/pi-durable):
one Task authority per Team, Workers as durable subthreads, accepted-then-delivered
notices.

## Status

- **Prerelease.** The npm `next` tag carries it. This package has its own version
  line, independent of Pi Team Bright. Pi Team Bright 0.25.1 is its contract pin.
- **No standalone runtime.** The package needs a host that supplies the Team ports:
  model roles or a `catalog`, `observationPort`, `abortRun` and `stop`. The runtime
  builds Worker creation and stop settlement from them. Today the host is the AmPi
  node.
- **Exact peers.** The peer dependencies pin `@earendil-works/pi-durable`,
  `@earendil-works/pi-ai` and `@earendil-works/chord` to 1.4.0. Other versions give
  a peer conflict.
- **First version without provenance.** The first version was published by hand.
  Later versions publish from CI with npm provenance.
- **Protocol value.** The persisted notice value `sender: 'durable-team'` is a
  protocol value from the package's former name. It stays unchanged.
- **Tests here.** CI runs the unit suites and a 100-seed differential oracle run.
  Run 500 or more seeds by hand before a release.
  The SIGKILL crash suite and `bench/e2e.ts` stay in HyperCarrier (private).
- **Before deploy.** The first open migrates the store to the indexed format. The
  migration is one-way: rollback means restoring a backup. Back up the store, and
  stop every older node process first. 0.1.0-rc.1 stores version-1 documents.
  This version migrates them to version 2, and a downgrade cannot read a migrated
  store.

Known issues:

1. An Alert is lost when the recipient's model errors. It shows as unanswered. Pi
   Team Bright retries it; this package does not.
2. `team_sync` can replay pages that an earlier call released. Act only on
   `task_read`, and never call `team_sync` from a nested tool call.
3. Interrupt: a Worker can interrupt the lead, the run to stop is chosen when the
   Alert is accepted, and the abort has no settle bound.
4. An Alert can be admitted after its recipient is closed or fenced. After an owner
   Close, a busy Worker can still run one tool call.

## Install

```sh
npm install @hypercarrier/pi-team-durable@next
```

The package ships compiled JavaScript and type declarations under `dist`.
Node 22.19 or newer. Install the three pinned peers beside it.

`./testing` exports named internals for a host's own tests. It is unstable test support:
its names can change in any release. Production code imports `./authority` only.

## API

`./authority` exports the runtime factory, authority documents, observation
observer, and structural stop ports. A host adapter registers this runtime.

- `ptb.team` owns membership and lifecycle. `ptb.head` holds current graph
  coordinates, readiness, occupancy, attention, and bounded recent references.
  Task family members hold current facts, retained Attempts, cards, and operator
  projections. Immutable graph indexes hold topology. Worker indexes hold
  nonterminal Task IDs. Each changed document is stored as a base.
- A controller-only conversation holds graph revision diffs and controller
  events. Commands restore the incremental engine from current Task records.
  They commit changed facts, indexes, receipts, obligations, and history together.
- Operation receipts retain exact result strings per epoch. Native tool-call
  receipts use task scope and retire with the tool. Direct callers use permanent
  family members. Obligations retain their identity after settlement; bounded
  hash partitions index pending and unscheduled keys.
- Nine lead tools and three Worker tools use the rc.7 contract. Exact conversation
  and generation bindings guard authority. A fork does not acquire an existing
  Team's lead authority.
- `ptb.worker-anchor` owns a Worker conversation and renews a durable sleep.
  Lead Esc does not end it. Stops use stored target IDs through the node's single
  mark-before-resume fence. Failed confirmation retains the intent.
- `ptb.deliver` rechecks an obligation, submits with a stable request ID, records
  admission, and waits for settlement. Notices use `team.notice` entries whose
  data (`TeamNoticeData`) names the delivery kind and its sender: an Alert's
  author, or `team-lead` for Tasks and stops. An interrupt Alert's data also
  has `interrupt: true`. A reader names a notice from this data, never from its
  text.
  Interrupt Alerts persist the recipient run ID at acceptance. A separate phase
  asks the host `abortRun` port to stop that exact run, then checkpoints before
  normal admission. Queue identities stay native; no input is re-queued.
  A failed interruption appears as unproved in the notice. Retry never selects
  a replacement run. The node supplies a cancellable wait outside its admission lock.
- `ptb.notify` reserves an automatic page before admission. A failed lead input
  pauses notification. Explicit sync or a new owner input resumes it.
- `team_sync` reserves successive bounded pages. It never waits for consumption.
  The final request hook records exact page visibility. A later observer requires
  a matching persisted native assistant entry and successful input settlement
  before it advances the cursor. Compaction and failed runs release unseen pages.
- The head keeps references to 100 recent journal events. Their immutable
  `ptb.event` entries stay in the lead conversation, so sync page boundaries stay
  unchanged.
- Observation indexes, page metadata, immutable page bodies, and open proofs
  use separate base-checkpointed documents. Requests check active pages from
  the newest message backward. They do not hash transcript history.
- The observer retires a proof after its run settles and it processes the
  evidence. It retires a page body only when the page is acknowledged or
  replaced and the contiguous cursor covers it. Unsettled proofs and notifier
  submit/settle checkpoints keep referenced bodies. Unknown notifier ownership
  keeps the body. Page metadata and the exact replay receipts remain.
- `observations(tx)` and `observationStats(tx)` are diagnostic readers. The
  latter reports current-record counts and UTF-8 JSON bytes. Production hooks
  and the observer read current indexes.
- `/ptb` reads one acknowledged scan anchor for the current epoch. This anchor
  differs from the runtime reservation anchor in `scanByEpoch`. Cursor advance
  folds eligible waiting-page anchors before removing their waiting entries.
  Body retirement retains the anchor; epoch change removes it.
- `readTeamPanel` reads current Task projections and recent records.
  `readTeamSummary` reads head indexes. `state.open()` is a full-history diagnostic
  adapter; runtime paths do not use it.

The first authority open migrates rc.7 documents in one transaction. It checks
current facts against the reference controller and writes the new records and
log. The same transaction raises all six shared PTB document kinds to version 2:
`ptb.team`, `ptb.graph`, `ptb.journal`, `ptb.outbox`, `ptb.receipts`, and
`ptb.observation`. `ptb.team` keeps its data. The other five retain only
`{fence:"ptb-authority-v2"}`. Current commands use the new authority records.
These occupied addresses prevent old code from creating fresh version-1
documents. pi-durable rejects old typed reads before mutation. Fresh stores
receive the same fence. Native pi-durable documents keep their own versions.

A migrated operation receipt receives its original projection check on its
first replay. Downgrade is not supported.

The fence covers stopped and reopened stores, not an old process that is still
running. Before the first open with this build, stop every node process that
runs older code, and back up the whole store directory. To roll back, restore
that backup and run the older build on it. A rollback discards all Team work
accepted after the migration.

Known hazard until the next contract change: `team_sync` can replay a stale
page. This occurred with a direct snapshot call as well as nested calls. Leads
use `task_read` for any Task state they act on. A direct `team_sync` call does
not establish current Task state.

The host injects model roles: a static `modelRoles` list, or a `catalog(tx,
lead)` resolver that gives each lead its own roles and default. The AmPi
node's `default` role captures the authoritative lead model at each Worker
generation. Unknown roles return a typed refusal.
The package never reads Pi configuration. The observation commit port resolves
its store for every hook; a registry can serve multiple stores.

The executable contract snapshot comes from Pi Team Bright 0.24.0-rc.7 at
`382950ce62bd38b6cd9f7fb80b91ef5b9140e15a` (unreleased Alert interrupt support).
`scripts/rc7-contract.mjs` records source hashes. Pi Team Bright 0.25.1 is the
contract pin: it ships the same source files, and a test compares their SHA-256
values with the recorded ones. The tests regenerate the contract from the
installed devDependency `@hypercarrier/pi-team-bright@0.25.1`.

```sh
npm ci
npm test
```

The hardening oracle freezes `src/authority` at
certified P, the 0.1.0-rc.1 port, in `test/oracle/authority`.
Do not update that copy when optimizing the runtime. The seeded differential
harness compares exact tool results or error messages, all public Task cards,
and created `ptb.deliver` inputs after every command. Each runtime has its own
MemoryStorage Harness. Both schedulers stay paused. The stop port returns fake
settlement. A synthetic persisted lead run permits snapshot and update reads.
Delivery execution and observation acknowledgement have separate tests.

```sh
# From the repository root: N seeds over K processes (defaults: 500 and 4).
npm run test:oracle -- --seeds 500 --shards 4
# Replay one failing seed. Failures print its full command trace.
npm run test:oracle -- --seed 42 --seeds 1 --shards 1
# Run only the deterministic boundary cases.
npm run test:oracle -- --targeted-only
```

Each seed runs about 70 commands over at most seven Tasks. It combines a fixed
lifecycle and transition prefix with 30 random commands, then starts a new Team
epoch. Use `--steps` to change the random command count. A separate deterministic
sequence covers prerequisite repair, superseded dependents, removed and
reintroduced keys, stale lineage, blocked Worker capacity, reorder-only
restatement, and stop notices after Worker reconnection. The normal Vitest suite
runs these cases and three smoke seeds. This oracle detects behavior drift from the frozen
runtime; it does not prove that the frozen behavior is correct.

Fast tests use MemoryStorage, a fixed clock, inert timers, and faux responses.
The SIGKILL crash-recovery suite and `bench/e2e.ts` stay in HyperCarrier (private).
They use the AmPi node's subprocess carrier. This repository's tests do not
cover crash recovery.

The C1 delivery hold remains unported. pi-durable places steers and follow-ups at
turn boundaries. A stale admitted notice cannot grant authority: Task version and
Worker generation checks still apply.

## Development

`npm run build` emits `dist`; `npm install` runs it through `prepare`.
`npm run typecheck`, `npm test` and `npm run verify:package` are the CI checks.
See [releasing](docs/maintainers/releasing.md) for the release procedure.

## Provenance

Extracted from HyperCarrier (private) at the certified port, renamed from
`durable-team`, without its history.
