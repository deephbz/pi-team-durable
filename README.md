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
- **Tests here.** CI runs the unit suites.
  The SIGKILL crash suite and `bench/e2e.ts` stay in HyperCarrier (private).
- **Store format.** This version stores version-1 documents. The next version
  migrates them to version 2, and a downgrade cannot read a migrated store.

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

## API

`./authority` exports the runtime factory, authority documents, observation
observer, and structural stop ports. A host adapter registers this runtime.

- `ptb.team`, `ptb.graph`, `ptb.journal`, `ptb.outbox`, and `ptb.receipts` store
  authority, graph history, recent events, delivery obligations, and replay results.
  Each accepted command and its mechanics commit together.
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
- The journal keeps 100 recent events in its document and immutable `ptb.event`
  entries in native storage. This bound does not cover graph history, receipts,
  outbox, observation proofs, or storage history.

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
