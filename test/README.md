# Differential authority oracle

This harness checks authority rewrites against P, the certified port.
`oracle/authority` contains P's authority files without edits. `sourceRef` in
`oracle-manifest.json` records where they came from.
`oracle-manifest.json` records their SHA-256 values. The harness checks the
manifest before it creates fixtures. Replace the reference only through an
explicit baseline decision. Never patch a frozen file.

Run from the repository root:

```sh
npm run test:oracle -- --candidate-ref v0.1.0-rc.1 --seeds 500 --shards 4 --keep-going
npm run test:oracle -- --candidate-ref v0.1.0-rc.1 --targeted-only
npm run test:oracle -- --against oracle --seeds 10 --shards 2
```

`--candidate-ref` extracts `src/authority` of that commit of this repository into a temporary directory.
Without it, the harness loads the worktree source. Set
`ORACLE_CANDIDATE_MODULE` to a scratch `runtime.ts` URL for mutation checks.
The failure output gives the seed, command history, native-ID bindings, and
replay command. Preserve the module override when replaying a scratch mutation.

## Comparison boundary

The harness compares:

- Tool results and thrown error messages.
- Public Task cards after each command.
- Created `ptb.deliver` task inputs, in order.
- `deliveryDraft` outputs for those inputs, including the full `team.notice`
  entry data and each supplied interruption outcome: `interrupted`, `ended`,
  and `failed`. `failed` produces the not-proved message.
- `pageDraft` outputs for input and tool carriers, including page identity,
  `delivery: update`, and `sender: durable-team`.

One bidirectional native-ID map spans each seed. Actor bindings anchor routing.
Caller IDs, synthetic run coordinates, graph IDs, version tokens, keys, field
order, and all non-ID bytes stay exact. JSON content uses token-position
substitution for native IDs only. The same map applies to draft content.

The fixtures keep the scheduler paused. They do not submit drafts, inspect
persisted admission entries, select interruption outcomes, or execute the host
interrupt port. Draft comparisons verify the public builders that the submit
phases call. Native interruption and admission need their integration tests.

## Interrupt coverage

Targeted cases cross authority Task occupancy (`ready` or `in_progress`) with
presence or absence of a persisted carrier run. These states are independent:
an idle Worker can have an owner-initiated run, and a claimed Task can outlive
its run. Carrier setup changes only pi-durable's `LiveDoc` through its public
transaction API. Synthetic run IDs remain exact.

The generator also varies interrupting Alerts, sender, recipient, fanout,
Task references, and carrier capture in random traces. The targeted cases
check complete notice metadata, optional `interrupt: true`, bounded senders,
and all interruption-content variants. Worker Alerts remain non-interrupting;
P permits `interrupt` only on the lead's schema. Random invalid inputs still
check refusal and error parity.
