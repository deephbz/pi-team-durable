// Hardening oracle for authority performance changes. Compare only tool results, Task cards,
// and created delivery inputs. Frozen authority code lives in ../oracle/authority (d6f87cda).
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { existsSync, realpathSync, readFileSync, readdirSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxProvider } from '@earendil-works/pi-ai/providers/faux';
import { createRegistry, Harness, LiveDoc, MemoryStorage, type ConversationId, type TaskId, type SubmissionId } from '@earendil-works/pi-durable';
import type { createTeamRuntime as RuntimeFactory } from '../../src/authority/runtime.ts';
import { createTeamRuntime as createOracle } from '../oracle/authority/runtime.ts';
import * as oracleDelivery from '../oracle/authority/delivery.ts';
import * as oraclePages from '../oracle/authority/notifier.ts';
import type { Page } from '../oracle/authority/observation.ts';
import { declaredParameters, type CoreApi } from '../../src/authority/tools.ts';
import { contracts, type ToolName, type Role } from '../../src/authority/contracts.ts';
import { NativeIdBijection } from './native-id-bijection.ts';

type CarrierRun = { actor: string; taskId?: number };
type Command = { name: ToolName; args: any; actor?: string; id: number; carrier?: CarrierRun };

// Check the frozen bytes before loading any fixtures. The manifest stays outside the oracle.
const frozen = JSON.parse(readFileSync(new URL('../oracle-manifest.json', import.meta.url), 'utf8'));
const frozenDirectory = new URL('../oracle/authority/', import.meta.url);
assert.deepStrictEqual(readdirSync(frozenDirectory).sort(), Object.keys(frozen.files).sort());
for (const [name, hash] of Object.entries(frozen.files)) {
  assert.equal(createHash('sha256').update(readFileSync(new URL(name, frozenDirectory))).digest('hex'), hash, `Frozen oracle changed: ${name}`);
}
type Definition = { key: string; title: string; goal: string; assignee: string; needs: string[]; on_goal_failed?: { target: string; max_traversals: number } };
const keys = Array.from({ length: 7 }, (_, i) => `t${i}`);
const workers = ['w0', 'w1', 'w2'];
const copy = <T>(x: T): T => structuredClone(x);
const declaredContract = contracts();
// P acted on undeclared role arguments. The oracle gets only the declared input that the current tools act on.
function declared(command: Command): Command {
  const schema = declaredContract[command.actor ? 'worker' : 'lead'][command.name];
  return schema ? { ...command, args: declaredParameters(schema, copy(command.args)) } : command;
}

async function fixture(factory: typeof RuntimeFactory) {
  const models = createModels();
  const faux = fauxProvider({ provider: 'oracle-faux', api: 'oracle-faux', models: [{ id: 'test' }], tokensPerSecond: 0 });
  models.setProvider(faux.provider);
  let harness!: Harness;
  const runtime = factory({
    modelRoles: [{ name: 'builder', use: 'Build', model: { provider: 'oracle-faux', modelId: 'test' } }], defaultModelRole: 'builder',
    observationPort: async () => ({ commit: (fn, c) => harness.commit(fn, c) }),
    abortRun: async () => { throw new Error('The differential fixture must not execute the interrupt host port'); },
    // Carrier settlement is outside this oracle's scope. No scheduler resume or real process stop.
    stop: async (_api, plans) => plans.map(() => ({ status: 'settled' as const, tasks: {} })),
  });
  const registry = createRegistry();
  for (const extension of runtime.extensions) registry.install(extension);
  harness = await Harness.open(new MemoryStorage(), { models, registry, now: () => 1000, setTimer: () => () => {}, onWake: () => {} }, context);
  const root = await harness.root(context, { agent: { model: { provider: 'oracle-faux', modelId: 'test' }, extensions: [runtime.mechanics, runtime.lead, runtime.observation] } });
  // Persist a synthetic lead run so sync exercises real snapshot/update reservations while
  // the scheduler stays paused. This is pi-durable carrier setup, not Team state access.
  await root.commit(async tx => { (await tx.doc(LiveDoc, root.id)).run = { taskId: 900000 as TaskId, inputs: [900001 as SubmissionId] }; }, context);
  const bindings = new Map<string, ConversationId>();
  let newest = root.id;
  const deliveries: unknown[] = [], seen = new Set<number>();
  const off = harness.subscribeCommits(publication => {
    for (const change of publication.changes) {
      if (change.type === 'conversation') newest = change.value.id;
      if (change.type === 'task' && change.value.kind === 'ptb.deliver' && !seen.has(change.value.id)) {
        seen.add(change.value.id); deliveries.push(copy(change.value.input));
      }
    }
  });
  async function execute(command: Command): Promise<any> {
    const role: Role = command.actor ? 'worker' : 'lead';
    const conversationId = command.actor ? bindings.get(command.actor)! : root.id;
    const api: CoreApi = { taskId: command.id as TaskId, conversationId, commit: root.commit.bind(root), now: () => 1000, callId: `call-${command.id}` };
    try {
      if (command.carrier) {
        const target = command.carrier.actor === 'team-lead' ? root.id : bindings.get(command.carrier.actor);
        assert(target, `Missing carrier actor ${command.carrier.actor}`);
        await root.commit(async tx => {
          const live = await tx.doc(LiveDoc, target);
          if (command.carrier!.taskId === undefined) delete live.run;
          else live.run = { taskId: command.carrier!.taskId as TaskId, inputs: [900001 as SubmissionId] };
        }, context);
      }
      const before = newest;
      const result = await runtime.core.execute(command.name, role, command.args, api, context);
      if (command.name === 'ensure_worker' && result.kind === 'worker_ensured' && newest !== before) bindings.set(command.args.name, newest);
      return { result };
    } catch (error) { return { error: error instanceof Error ? error.message : String(error) }; }
  }
  return { execute, deliveries, actorIds: () => new Map<string, number>([['team-lead', root.id], ...bindings]), async close() {
    assert.equal((await harness.inspect(context)).scheduling, 'paused'); assert.equal(faux.state.callCount, 0);
    off(); await harness.close(context);
  } };
}

// Mulberry32. Seed and step fully determine the command trace; no wall-clock or Math.random input.
function random(seed: number) {
  let state = seed >>> 0;
  return (n: number) => { state += 0x6d2b79f5; let x = state; x = Math.imul(x ^ x >>> 15, x | 1); x ^= x + Math.imul(x ^ x >>> 7, x | 61); return ((x ^ x >>> 14) >>> 0) % n; };
}

async function runSeed(seed: number, steps: number, targeted = false) {
  const against = process.env.ORACLE_AGAINST ?? 'current';
  assert(['current', 'oracle'].includes(against), 'ORACLE_AGAINST must be current or oracle');
  const referenceFlag = against === 'current' && process.env.ORACLE_CANDIDATE_REF ? ` --candidate-ref ${process.env.ORACLE_CANDIDATE_REF}` : '';
  const replayCommand = `npm run test:oracle -- --against ${against}${referenceFlag} --seed ${seed} --seeds 1 --shards 1 --steps ${steps}`;
  const candidateModule = process.env.ORACLE_CANDIDATE_MODULE ?? new URL('../../src/authority/runtime.ts', import.meta.url).href;
  const candidate = against === 'oracle' ? createOracle : (await import(candidateModule)).createTeamRuntime as typeof RuntimeFactory;
  const deliveryModule: typeof oracleDelivery = against === 'oracle' ? oracleDelivery : await import(new URL('./delivery.ts', candidateModule).href);
  const pageModule: typeof oraclePages = against === 'oracle' ? oraclePages : await import(new URL('./notifier.ts', candidateModule).href);
  const oracle = await fixture(createOracle as never), current = await fixture(candidate as never);
  const identities = new NativeIdBijection();
  identities.bindActors(oracle.actorIds(), current.actorIds());
  const rng = random(seed), pick = <T>(xs: readonly T[]) => xs[rng(xs.length)];
  let serial = 1000000, graphVersion: string | undefined, cards: any[] = [], defs: Definition[] = [], previousDeliveryCount = 0;
  const history: Command[] = [], versions: string[] = [], coverage = new Set<string>();
  let draftsCompared = 0;
  const statistics = () => ({ commands: history.length, draftsCompared, coverage: [...coverage], nativeIdPairs: identities.pairs.length, nativeStringLocations: [...identities.stringLocations.keys()] });
  function compareDraft(actual: any, expected: any) {
    identities.compareDraft(actual, expected, `$drafts[${draftsCompared++}]`);
    assert.equal(expected.entry.kind, 'team.notice');
    assert.equal(expected.entry.data.source, 'team');
    assert(expected.entry.data.sender.length <= 64);
  }
  async function step(name: ToolName, args: any, actor?: string, reuseId?: number, carrier?: CarrierRun) {
    const command: Command = { name, args: copy(args), actor, id: reuseId ?? ++serial, ...(carrier ? { carrier } : {}) };
    identities.pin(command.id);
    if (carrier?.taskId !== undefined) identities.pin(carrier.taskId);
    const reusedTool = history.some(c => c.id === command.id);
    history.push(command);
    try {
      const expected = await oracle.execute(declared(command)), actual = await current.execute(against === 'oracle' ? declared(command) : command);
      identities.bindActors(oracle.actorIds(), current.actorIds());
      identities.compare(actual, expected, '$tool');
      if (expected.result?.kind === 'task_graph_applied' && !expected.result.replayed && !reusedTool) graphVersion = expected.result.graph_version;
      const read: Command = { name: 'task_read', args: { task_ids: keys }, id: ++serial };
      identities.pin(read.id);
      const oldCards = await oracle.execute(read), newCards = await current.execute(read);
      identities.compare(newCards, oldCards, '$cards');
      cards = (oldCards.result?.outcomes ?? []).filter((x: any) => x.kind === 'found').map((x: any) => x.task);
      versions.push(...cards.map(t => t.version));
      identities.compareDeliveries(current.deliveries, oracle.deliveries);
      for (let index = previousDeliveryCount; index < oracle.deliveries.length; index++) {
        const input = oracle.deliveries[index] as oracleDelivery.DeliveryInput;
        const actualInput = current.deliveries[index] as oracleDelivery.DeliveryInput;
        for (const outcome of input.interrupt ? [undefined, 'interrupted', 'ended', 'failed'] as const : [undefined]) {
          const draft = oracleDelivery.deliveryDraft(input, outcome);
          compareDraft(deliveryModule.deliveryDraft(actualInput, outcome), draft);
          assert.deepStrictEqual(draft.entry.data, { source: 'team', deliveryId: input.key, epoch: input.epoch, delivery: input.kind, sender: input.sender,
            ...(input.interrupt && input.kind === 'alert' ? { interrupt: true } : {}) });
          if (outcome) { assert.equal(JSON.parse(draft.content).interruption.outcome, outcome); coverage.add(`interruption:${outcome}`); }
          coverage.add(`notice:${input.kind}:${input.sender === 'team-lead' ? 'authority' : 'worker'}`);
        }
      }
      previousDeliveryCount = oracle.deliveries.length;
      coverage.add(`${name}:${expected.result?.kind ?? 'throw'}`);
      return expected.result;
    } catch (error) {
      if (process.env.ORACLE_TRACE_FAILURES === '1') console.error(JSON.stringify({ seed, step: history.length - 1, command, previousDeliveryCount, oracleCreated: oracle.deliveries.slice(previousDeliveryCount), currentCreated: current.deliveries.slice(previousDeliveryCount) }));
      throw Object.assign(new Error(`Differential failure seed=${seed} step=${history.length - 1}\nReplay: ${replayCommand}\nNative ID bindings:\n${JSON.stringify(identities.pairs)}\nTrace:\n${JSON.stringify(history, null, 2)}`, { cause: error }), { run: statistics() });
    }
  }
  async function apply(tasks = defs, stale = false) {
    return step('task_graph_apply', { operation_id: `graph-${serial}`, ...(graphVersion ? { expected_graph_version: stale ? 'g_0000000000000000' : graphVersion } : {}), tasks });
  }
  async function update(id: string, transition?: string, actor?: string, stale = false) {
    const card = cards.find(t => t.id === id);
    return step('task_update', { task_id: id, operation_id: `update-${serial}`, expected_version: stale ? pick(versions) ?? 'v_0000000000000000' : card?.version ?? 'v_0000000000000000', ...(transition ? { transition, evidence: `Evidence ${serial}` } : { current_context: `Context ${serial}` }) }, actor ?? card?.assignee ?? 'w0');
  }
  try {
    // A bounded history keeps 500 seeds affordable for the frozen O(history) oracle.
    const epoch = 0;
    {
      graphVersion = undefined;
      await step('team_create', { name: `demo-${epoch}`, purpose: 'Differential contract' });
      for (const name of workers) await step('ensure_worker', { name, scope: `Scope ${name}` });
      await step('ensure_worker', { name: 'spare', scope: 'Lifecycle' });
      await step('worker_stop', { worker: 'spare' });
      await step('ensure_worker', { name: 'spare', scope: 'Lifecycle' }); // reconnect
      await step('ensure_worker', { name: 'spare', scope: 'Lifecycle' }); // reuse
      defs = keys.slice(0, 3 + rng(5)).map((key, i) => ({ key, title: `Title ${key}`, goal: `Goal ${rng(100)}`, assignee: workers[i % workers.length], needs: keys.slice(0, i).filter(() => rng(3) === 0), ...(i === 0 ? { on_goal_failed: { target: key, max_traversals: 1 } } : {}) }));
      await apply();
      // Guaranteed success, invalid actor/state, external blocking, bounded failure, context, and stale version paths.
      await update('t0', 'goal_achieved');
      await update('t0', 'claim', 'w1');
      await update('t0', 'claim');
      await update('t0');
      await update('t0', 'block');
      await update('t0', 'resume');
      await update('t0', 'goal_failed');
      await update('t0', 'claim');
      await update('t0', 'goal_failed');
      await update('t0', 'claim', 'w0', true);
      defs[0].goal += ' revised';
      await apply();
      await update('t0', 'claim');
      await update('t0', 'goal_achieved');
      await step('alert_send', { to: 'w0', kind: 'attention', text: `Lead alert ${epoch}` });
      await step('alert_send', { kind: 'clarification', text: `Worker alert ${epoch}` }, 'w0');
      await step('alert_send', { to: '*', kind: 'announcement', text: `Announcement ${epoch}` });
      const replay = history.find(c => c.name === 'task_graph_apply')!;
      await step(replay.name, replay.args); // same operation_id, new tool Task
      await step(replay.name, replay.args, replay.actor, replay.id); // same tool Task
      await step('team_create', { name: 'collision', purpose: 'Changed input' }, undefined, replay.id);
      await step('team_sync', { view: 'snapshot' });
      await step('team_sync', { view: 'updates' });
      if (targeted) {
        // Exercise the same public draft builders used by Deliver.submit and Notify.submit.
        // No submit/wait or host interruption runs here; the scheduler stays paused.
        for (const carrier of [{ kind: 'input', text: '{"kind":"team_update"}' }, { kind: 'tool', callId: 'probe' }]) {
          const page = { id: 'page-probe', epoch: 'epoch-probe', body: '{"kind":"snapshot"}', carrier } as Page;
          const expected = oraclePages.pageDraft(page);
          compareDraft(pageModule.pageDraft(page), expected);
          assert.deepStrictEqual(expected.entry.data, { source: 'team', pageId: page.id, epoch: page.epoch, delivery: 'update', sender: 'durable-team' });
          coverage.add(`notice:update:${carrier.kind}`);
        }
        // Public Worker names stop at 64 characters. Exercise that reachable boundary
        // through alert_send; storage adapters build their own delivery inputs.
        for (const author of ['reviewer', 'x'.repeat(64)]) {
          assert.equal((await step('ensure_worker', { name: author, scope: 'Sender boundary' })).kind, 'worker_ensured');
          const before = oracle.deliveries.length;
          assert.equal((await step('alert_send', { kind: 'attention', text: 'Sender boundary probe' }, author)).kind, 'alert_sent');
          const created = oracle.deliveries.slice(before) as oracleDelivery.DeliveryInput[];
          assert.equal(created.length, 1);
          assert.equal(created[0].kind, 'alert');
          assert.equal(created[0].sender, author);
          assert.equal(JSON.parse(created[0].content).alert.from, author);
          // step compares created inputs, deliveryDraft and complete entry.data on both sides.
          coverage.add('notice:bounded-sender');
        }
        const def = (key: string, assignee: string, needs: string[] = []): Definition => ({ key, title: key, goal: `Targeted ${key}`, assignee, needs });
        const accepted = async (id: string, transition: string) => {
          const r = await update(id, transition);
          assert.equal(r.kind, 'updated', `Targeted ${id} ${transition}: ${JSON.stringify(r)}`);
        };
        const achieve = async (id: string) => { await accepted(id, 'claim'); await accepted(id, 'goal_achieved'); };
        // Task occupancy and a captured carrier run are independent. An owner can run an idle
        // Worker; a claimed Task can remain in progress after its carrier run ends.
        for (const busy of [false, true]) for (const captured of [false, true]) {
          defs = [{ ...def('t0', 'w0'), goal: `Interrupt occupancy ${busy} ${captured}` }];
          assert.equal((await apply()).kind, 'task_graph_applied');
          if (busy) await accepted('t0', 'claim');
          assert.equal(cards.find(t => t.id === 't0').status, busy ? 'in_progress' : 'ready');
          const before = oracle.deliveries.length;
          assert.equal((await step('alert_send', { to: 'w0', kind: 'attention', text: 'Interrupt probe', interrupt: true }, undefined, undefined,
            { actor: 'w0', ...(captured ? { taskId: 900100 } : {}) })).kind, 'alert_sent');
          const created = oracle.deliveries.slice(before) as any[];
          assert.equal(created.length, 1);
          assert.equal(created[0].kind, 'alert'); assert.equal(created[0].sender, 'team-lead');
          assert.equal(created[0].interrupt, true);
          assert.equal(created[0].expectedRunTaskId, captured ? 900100 : undefined);
          coverage.add(`interrupt:worker:${busy ? 'busy' : 'idle'}:${captured ? 'captured' : 'uncaptured'}`);
        }
        for (const captured of [false, true]) {
          const before = oracle.deliveries.length;
          assert.equal((await step('alert_send', { kind: 'attention', text: 'Worker sender probe' }, 'w0', undefined,
            { actor: 'team-lead', ...(captured ? { taskId: 900000 } : {}) })).kind, 'alert_sent');
          const delivery = oracle.deliveries[before] as any;
          assert.equal(delivery.kind, 'alert'); assert.equal(delivery.sender, 'w0');
          assert.equal(delivery.expectedRunTaskId, undefined);
          assert.equal(delivery.interrupt, undefined);
          coverage.add(`normal:lead:${captured ? 'captured' : 'uncaptured'}`);
        }
        await step('team_sync', { view: 'updates' }, undefined, undefined, { actor: 'team-lead', taskId: 900000 });
        defs = [def('t0', 'w0'), { ...def('t1', 'w1', ['t0']), on_goal_failed: { target: 't0', max_traversals: 2 } }, def('t2', 'w2', ['t1'])];
        const initial = await apply(); assert.equal(initial.kind, 'task_graph_applied', JSON.stringify(initial));
        await achieve('t0');
        await accepted('t1', 'claim');
        const staleLineage = copy(cards.find(t => t.id === 't1'));
        await accepted('t1', 'goal_failed'); // Repair invalidates the achieved prerequisite.
        assert.equal(cards.find(t => t.id === 't0').status, 'ready');
        await achieve('t0');
        await achieve('t1');
        await accepted('t2', 'claim');
        const superseded = copy(cards.find(t => t.id === 't2'));
        defs[0].goal += ' revised';
        await apply(); // A prerequisite revision supersedes its running dependent.
        for (const stale of [staleLineage, superseded]) {
          const r = await step('task_update', { task_id: stale.id, operation_id: `stale-lineage-${serial}`, expected_version: stale.version, transition: 'goal_achieved', evidence: 'Late result' }, stale.assignee);
          assert.equal(r.reason, 'version_conflict');
        }
        // Removal retains Attempt history. Reintroduced t2 reaches its third Attempt.
        defs = [def('t2', 'w2')];
        await apply(); await accepted('t2', 'claim');
        defs = [def('t0', 'w0')]; await apply();
        defs = [def('t2', 'w2')]; await apply(); await accepted('t2', 'claim');
        assert.equal(cards.find(t => t.id === 't2').current_attempt.ordinal, 3);
        // Blocking releases capacity without finishing the blocked Task.
        defs = [{ ...def('t0', 'w0'), goal: 'Blocked work' }, def('t1', 'w0')];
        await apply(); await accepted('t0', 'claim'); await accepted('t0', 'block'); await accepted('t1', 'claim');
        const beforeReorder = copy(cards), previousGraph = graphVersion;
        defs.reverse(); await apply();
        assert.notEqual(graphVersion, previousGraph);
        assert.deepStrictEqual(cards, beforeReorder, 'Reordering preserves public Task views');
        // A superseded Attempt requires a stop notice for each reconnected generation.
        defs = [{ ...def('t2', 'w2'), goal: 'Reconnect work' }];
        await apply(); await accepted('t2', 'claim');
        defs[0].goal += ' revised'; await apply();
        await step('task_update', { task_id: 't2', operation_id: `cancel-reconnect-${serial}`, expected_version: cards.find(t => t.id === 't2').version, transition: 'cancel', evidence: 'Permit stop' });
        assert.equal((await step('worker_stop', { worker: 'w2' })).kind, 'worker_stopped');
        assert.equal((await step('ensure_worker', { name: 'w2', scope: 'Scope w2' })).effect, 'reconnected');
        const deliveryCount = oracle.deliveries.length;
        await apply();
        assert(oracle.deliveries.slice(deliveryCount).some((input: any) => JSON.parse(input.content).kind === 'task_changed'), 'Reconnection re-creates stop notices');
      }
      for (let i = 0; i < steps; i++) {
        const mode = rng(14), card = pick(cards), id = card?.id ?? pick(keys);
        if (mode < 4) {
          if (mode === 0) { // New random DAG; lower-index prerequisites prevent cycles.
            defs = keys.filter(() => rng(5) !== 0).map((key) => ({ key, title: `Title ${rng(20)}`, goal: `Goal ${rng(1000)}`, assignee: pick(workers), needs: [] }));
            defs = defs.map((d, index) => ({ ...d, needs: defs.slice(0, index).filter(() => rng(4) === 0).map(t => t.key), ...(rng(3) === 0 ? { on_goal_failed: { target: pick(defs).key, max_traversals: 1 + rng(3) } } : {}) }));
          } else if (mode === 1 && defs.length) { // Restate one semantic dimension at a time.
            const d = pick(defs), dimension = rng(6);
            if (dimension === 0) d.goal += '!';
            if (dimension === 1) d.title += '!';
            if (dimension === 2) d.assignee = pick(workers);
            if (dimension === 3) d.needs = defs.slice(0, defs.indexOf(d)).filter(() => rng(2) === 0).map(t => t.key);
            if (dimension === 4) d.on_goal_failed = { target: pick(defs).key, max_traversals: 1 + rng(3) };
            if (dimension === 5) { defs = defs.filter(t => t !== d).map(t => ({ ...t, needs: t.needs.filter(k => k !== d.key), ...(t.on_goal_failed?.target === d.key ? { on_goal_failed: undefined } : {}) })); }
          }
          await apply(defs, mode === 3);
        } else if (mode < 8) {
          const valid = card?.status === 'ready' ? 'claim' : card?.status === 'blocked' ? 'resume' : pick(['goal_achieved', 'goal_failed', 'block']);
          await update(id, mode === 4 ? valid : mode === 5 ? undefined : pick(['claim', 'resume', 'block', 'cancel', 'goal_achieved', 'goal_failed']), mode === 6 ? pick(workers.filter(w => w !== card?.assignee)) : card?.assignee, mode === 7);
        } else if (mode === 8) {
          const old = pick(history.filter(c => c.name === 'task_graph_apply' || c.name === 'task_update'));
          await step(old.name, old.args, old.actor); // operation_id replay under a new tool Task
        } else if (mode === 9) {
          const old = pick(history);
          await step(old.name, old.args, old.actor, old.id); // exact durable tool Task replay
        } else if (mode === 10) {
          const old = pick(history);
          await step('team_create', { name: 'changed', purpose: 'Tool identity collision' }, undefined, old.id);
        } else if (mode === 11) {
          const actor = rng(2) ? pick(workers) : undefined;
          const to = actor ? 'team-lead' : pick([...workers, '*', 'absent']), interrupt = rng(2) === 0;
          const captured = rng(2) === 0;
          const carrier = interrupt && ['team-lead', ...workers].includes(to) ? { actor: to, ...(captured ? { taskId: to === 'team-lead' ? 900000 : 900100 + workers.indexOf(to) } : {}) } : undefined;
          const result = await step('alert_send', { ...(actor ? {} : { to }), kind: actor ? 'attention' : pick(['attention', 'announcement']), text: `Random alert ${serial}`, ...(interrupt ? { interrupt: true } : {}), ...(card && rng(2) ? { task_id: card.id, task_version: card.version } : {}) }, actor, undefined, carrier);
          if (interrupt && result?.kind === 'alert_sent' && carrier) coverage.add(`interrupt:${to === 'team-lead' ? 'lead' : 'worker'}:${cards.some(t => t.assignee === to && t.status === 'in_progress') ? 'busy' : 'idle'}:${captured ? 'captured' : 'uncaptured'}`);
        } else if (mode === 12) await step('worker_stop', { worker: pick(workers) });
        else await step('team_sync', { view: pick(['snapshot', 'updates']) });
      }
      // Exercise accepted cancellation, Worker stop guards, re-creation, and next-epoch fencing.
      for (const card of [...cards]) await step('task_update', { task_id: card.id, operation_id: `cancel-${serial}`, expected_version: card.version, transition: 'cancel', evidence: 'End epoch' });
      await step('team_sync', { view: 'updates' });
      await step('team_shutdown', {});
      await step('task_read', { task_id: 't0' }, 'w0'); // old binding after shutdown
      await step('team_create', { name: 'next', purpose: 'New epoch' });
      await step('task_read', { task_id: 't0' }, 'w0'); // old generation cannot cross epochs
      await step('ensure_worker', { name: 'w0', scope: 'Scope w0' });
      await step('task_graph_apply', { operation_id: `new-epoch-${serial}`, tasks: [{ key: 't0', title: 'New', goal: 'New epoch goal', assignee: 'w0' }] });
    }
    if (process.env.ORACLE_TRACE_IDS === '1') console.log(JSON.stringify({ seed, nativeIdPairs: identities.pairs, nativeStrings: [...identities.stringLocations] }));
    return statistics();
  } finally { await oracle.close(); await current.close(); }
}

export async function runDifferential(defaultSeeds = 500) {
  const first = Number(process.env.ORACLE_SEED ?? 1), seeds = Number(process.env.ORACLE_SEEDS ?? defaultSeeds), steps = Number(process.env.ORACLE_STEPS ?? 30);
  assert(Number.isSafeInteger(first) && first >= 0 && Number.isSafeInteger(seeds) && seeds > 0 && Number.isSafeInteger(steps) && steps >= 0, 'Invalid oracle seed/count/steps');
  const start = performance.now(), coverage = new Set<string>(), nativeStringLocations = new Set<string>(); let commands = 0, nativeIdPairs = 0, draftsCompared = 0;
  const failures: { seed: number; message: string; cause: string; actual?: unknown; expected?: unknown }[] = [];
  for (let seed = first; seed < first + seeds; seed++) {
    let run: Awaited<ReturnType<typeof runSeed>> | undefined;
    try { run = await runSeed(seed, steps); }
    catch (error) {
      if (process.env.ORACLE_KEEP_GOING !== '1') throw error;
      const failure = error as Error & { run?: typeof run; cause?: Error & { actual?: unknown; expected?: unknown } };
      run = failure.run;
      const record = { seed, message: failure.message.split('\n')[0], cause: failure.cause?.message ?? failure.message, actual: failure.cause?.actual, expected: failure.cause?.expected };
      failures.push(record); console.error(JSON.stringify({ failure: record }));
    }
    if (!run) continue;
    commands += run.commands; nativeIdPairs += run.nativeIdPairs; draftsCompared += run.draftsCompared;
    for (const item of run.coverage) coverage.add(item);
    for (const location of run.nativeStringLocations) nativeStringLocations.add(location);
  }
  console.log(JSON.stringify({ against: process.env.ORACLE_AGAINST ?? 'current', seeds, passedSeeds: seeds - failures.length, failures, first, stepsPerEpoch: steps, commands, draftsCompared, seconds: +((performance.now() - start) / 1000).toFixed(2), nativeIdPairs, nativeStringLocations: [...nativeStringLocations].sort(), coverage: [...coverage].sort() }));
  if (failures.length) throw new Error(`${failures.length}/${seeds} differential seeds failed: ${failures.map(f => f.seed).join(', ')}`);
}

export async function runTargeted() {
  const start = performance.now(), run = await runSeed(0, 0, true);
  console.log(JSON.stringify({ targeted: true, against: process.env.ORACLE_AGAINST ?? 'current', commands: run.commands, draftsCompared: run.draftsCompared, coverage: run.coverage, seconds: +((performance.now() - start) / 1000).toFixed(2), nativeIdPairs: run.nativeIdPairs, nativeStringLocations: run.nativeStringLocations }));
}

if (process.argv[1] && existsSync(process.argv[1]) && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  if (process.argv.includes('--targeted')) await runTargeted();
  else await runDifferential();
}
