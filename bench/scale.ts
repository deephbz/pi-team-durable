// Scale benchmark for PTB authority on pi-durable. It drives the real lead and Worker tools through one
// deterministic Team workload and reports per-operation latency, storage bytes, heap, and cold-open cost.
//
//   node bench/scale.ts [--tasks 500] [--open 50] [--active 10] [--stopped 20] [--alerts 1000] [--storage sqlite|memory] [--runtime current|oracle]
//
// --runtime oracle runs the frozen rc.7 copy in test/oracle; the default runs src/authority.
//
// The workload restates the complete graph on every apply, as the rc.7 model contract requires.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { parseArgs } from 'node:util';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { createRegistry, Harness, MemoryStorage, type ConversationId, type Storage, type TaskId } from '@earendil-works/pi-durable';
import { openNodeSqliteStorage } from '@earendil-works/pi-durable/storage/sqlite/node';
import type { CoreApi } from '../src/authority/tools.ts';

const { values: flags } = parseArgs({ options: {
  tasks: { type: 'string', default: '500' }, open: { type: 'string', default: '50' },
  active: { type: 'string', default: '10' }, stopped: { type: 'string', default: '20' },
  alerts: { type: 'string', default: '1000' }, batch: { type: 'string', default: '25' },
  storage: { type: 'string', default: 'sqlite' }, runtime: { type: 'string', default: 'current' }, out: { type: 'string' },
} });
const source = flags.runtime === 'oracle' ? '../test/oracle/authority' : '../src/authority';
const { createAuthorityTools } = await import(`${source}/tools.ts`);
const { open } = await import(`${source}/state.ts`);
// open() is the full-history diagnostic reader in the current runtime; command cleanup uses the indexed authority.
const journalOf = flags.runtime === 'oracle' ? async (tx: any) => (await open(tx)).journal
  : async (tx: any) => (await (await import('../src/authority/store.ts')).openAuthority(tx)).journal;
const N = Number(flags.tasks), OPEN = Number(flags.open), ACTIVE = Number(flags.active), STOPPED = Number(flags.stopped);
const ALERTS = Number(flags.alerts), BATCH = Number(flags.batch);

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ptb-scale-'));
const file = path.join(dir, 'harness.sqlite');
const openStorage = async (): Promise<Storage> => flags.storage === 'memory' ? memory : await openNodeSqliteStorage(file);
const memory = new (class extends MemoryStorage { override async close() {} })();
const harnessOptions = { models: createModels(), registry: createRegistry(), now: () => 1000, setTimer: () => () => {} };

let harness = await Harness.open(await openStorage(), harnessOptions, context);
let root = await harness.root(context);
let launches = 0;
const core = createAuthorityTools({
  modelRoles: [{ name: 'builder', use: 'Build', model: { provider: 'fake', modelId: 'fake' } }], defaultModelRole: 'builder',
  createWorker: async (tx) => { const c = await tx.createConversation({ ownership: { kind: 'ownerless' } }); return { conversationId: c.id, anchorId: (900000 + ++launches) as TaskId }; },
  stopWorker: async () => true,
});

type Sample = { op: string; ms: number; at: number };
const samples: Sample[] = [];
let callId = 0, opIndex = 0;
const api = (conversationId: ConversationId): CoreApi => ({ taskId: ++callId as TaskId, conversationId, commit: root.commit.bind(root), now: () => 1000 + callId });
async function call(name: string, input: unknown, role: 'lead' | 'worker' = 'lead', conversationId: ConversationId = root.id): Promise<any> {
  const t0 = performance.now();
  const result = await core.execute(name as any, role, input, api(conversationId), context);
  samples.push({ op: name === 'team_sync' ? `team_sync:${(input as any).view}` : name, ms: performance.now() - t0, at: opIndex++ });
  if (name === 'team_sync') await root.commit(async (tx) => { delete (await journalOf(tx)).reservation; }, context);
  return result;
}

// Team and Workers. Stopped Workers own the early batches; active Workers own the rest.
await call('team_create', { name: 'scale', purpose: 'Scale benchmark' });
const workers = Array.from({ length: ACTIVE + STOPPED }, (_, i) => `w${String(i).padStart(2, '0')}`);
for (const w of workers) await call('ensure_worker', { name: w, scope: `Scope of ${w}` });
const conv = new Map<string, ConversationId>();
await root.commit(async (tx) => { for (const w of Object.values((await open(tx)).team.current!.workers)) conv.set(w.name, w.conversationId); }, context);

const goal = (i: number) => `Implement part ${i} of the feature. Acceptance: tests for part ${i} pass, the change is reviewed, and the evidence names the commit. `.repeat(3);
type Def = { key: string; title: string; goal: string; assignee: string; needs?: string[] };
const defs: Def[] = [];
const stoppedBatches = Math.ceil((N - OPEN) * 0.4 / BATCH);
let graphVersion: string | undefined, alertsSent = 0;
const keep = new Set(Array.from({ length: OPEN }, (_, i) => `t${String(N - OPEN + i).padStart(4, '0')}`));
const versions = new Map<string, string>();

async function syncTick() {
  if (opIndex % 5 === 0) await call('team_sync', { view: 'updates' });
  if (opIndex % 50 === 0) await call('team_sync', { view: 'snapshot' });
}
async function alert(worker: string) {
  if (alertsSent >= ALERTS) return;
  alertsSent++;
  if (alertsSent % 2) await call('alert_send', { kind: 'attention', text: `Progress note ${alertsSent} from ${worker}: finished a step, see Task evidence.` }, 'worker', conv.get(worker)!);
  else await call('alert_send', { to: worker, kind: 'clarification', text: `Lead note ${alertsSent}: keep the change small and record evidence.` });
}
async function work(ready: string[]) {
  // Each ready Task runs: Worker read, claim, two context notes, goal_achieved. Kept Tasks stop part way.
  const queue = [...ready];
  while (queue.length) {
    const id = queue.shift()!;
    const d = defs.find((x) => x.key === id)!, c = conv.get(d.assignee)!;
    const read = await call('task_read', { task_id: id }, 'worker', c);
    if (read.kind !== 'found' || read.task.state.kind !== 'ready') continue;
    let r = await call('task_update', { task_id: id, operation_id: `${id}-claim`, expected_version: read.task.version, transition: 'claim' }, 'worker', c);
    // A kept Task in progress occupies its Worker, so a later kept Task of that Worker stays ready (still open).
    if (r.kind !== 'updated' && keep.has(id) && r.reason === 'worker_occupied') { await syncTick(); continue; }
    if (r.kind !== 'updated') throw new Error(`claim ${id}: ${JSON.stringify(r)}`);
    versions.set(id, r.task.version);
    if (keep.has(id)) { if (Number(id.slice(1)) % 2) await call('task_update', { task_id: id, operation_id: `${id}-block`, expected_version: r.task.version, transition: 'block', current_context: 'Waiting for owner input.' }, 'worker', c); await syncTick(); continue; }
    for (const n of [1, 2]) { r = await call('task_update', { task_id: id, operation_id: `${id}-note${n}`, expected_version: r.task.version, current_context: `Step ${n}: edited files and ran the focused tests.` }, 'worker', c); await syncTick(); }
    r = await call('task_update', { task_id: id, operation_id: `${id}-done`, expected_version: r.task.version, transition: 'goal_achieved', evidence: `Commit abc${id} passes tests.` }, 'worker', c);
    if (r.kind !== 'updated') throw new Error(`done ${id}: ${JSON.stringify(r)}`);
    await alert(d.assignee);
    await syncTick();
    for (const next of r.ready_task_ids as string[]) if (!queue.includes(next) && defs.find((x) => x.key === next)) queue.push(next);
  }
}

const t0 = performance.now();
for (let b = 0; b * BATCH < N; b++) {
  const group = b < stoppedBatches ? workers.slice(ACTIVE) : workers.slice(0, ACTIVE);
  for (let j = 0; j < BATCH && b * BATCH + j < N; j++) {
    const i = b * BATCH + j, key = `t${String(i).padStart(4, '0')}`;
    const prev = i - BATCH >= 0 && (b !== stoppedBatches) ? [`t${String(i - BATCH).padStart(4, '0')}`] : undefined;
    defs.push({ key, title: `Part ${i}`, goal: goal(i), assignee: group[j % group.length], ...(prev ? { needs: prev } : {}) });
  }
  const applied = await call('task_graph_apply', { operation_id: `graph-${b}`, ...(graphVersion ? { expected_graph_version: graphVersion } : {}), tasks: defs });
  if (applied.kind !== 'task_graph_applied') throw new Error(`apply ${b}: ${JSON.stringify(applied).slice(0, 400)}`);
  graphVersion = applied.graph_version;
  await work(applied.ready_task_ids);
  if (b === stoppedBatches - 1) {
    for (const w of workers.slice(ACTIVE)) await call('worker_stop', { worker: w });
    // A restated graph may name only current Workers, so the stopped Workers' finished Tasks leave the graph.
    for (let k = defs.length - 1; k >= 0; k--) if (!workers.slice(0, ACTIVE).includes(defs[k].assignee)) defs.splice(k, 1);
  }
}
while (alertsSent < ALERTS) await alert(workers[alertsSent % ACTIVE]);
const runMs = performance.now() - t0;

// Final state shape.
// Document shape is defined for the rc.7 layout only; storageBytes covers both runtimes.
const shape = flags.runtime !== 'oracle' ? undefined : await root.commit(async (tx) => {
  const s = await open(tx), size = (v: unknown) => JSON.stringify(v ?? null).length;
  return { graph: size(s.graph), receipts: size(s.receipts), outbox: size(s.outbox), journal: size(s.journal), team: size(s.team),
    receiptCount: Object.keys(s.receipts.calls).length, obligations: Object.keys(s.outbox.obligations).length, alerts: Object.keys(s.outbox.alerts).length,
    revisions: s.graph.snapshot!.graphRevisions.length, events: s.graph.snapshot!.events.length };
}, context);
global.gc?.();
const heapMB = process.memoryUsage().heapUsed / 2 ** 20, rssMB = process.memoryUsage().rss / 2 ** 20;
await harness.close(context);
const bytes = flags.storage === 'memory' ? 0 : fs.readdirSync(dir).reduce((sum, f) => sum + fs.statSync(path.join(dir, f)).size, 0);

// Cold open: reopen storage, then the first read and the first mutation.
const c0 = performance.now();
harness = await Harness.open(await openStorage(), harnessOptions, context); root = await harness.root(context);
const openMs = performance.now() - c0;
const r0 = performance.now(); await call('task_read', { task_ids: ['t0000'] }); const firstReadMs = performance.now() - r0;
const lastOpen = [...keep].find((id) => versions.has(id));
const m0 = performance.now(); await call('task_update', { task_id: lastOpen!, operation_id: 'cold-note', expected_version: versions.get(lastOpen!)!, current_context: 'Cold note.' }, 'worker', conv.get(defs.find((d) => d.key === lastOpen)!.assignee)!); const firstWriteMs = performance.now() - m0;
await harness.close(context);

const byOp = new Map<string, Sample[]>();
for (const s of samples) byOp.set(s.op, [...(byOp.get(s.op) ?? []), s]);
const q = (xs: number[], p: number) => { const v = [...xs].sort((a, b) => a - b); return v[Math.min(v.length - 1, Math.floor(p * v.length))] ?? 0; };
const ops = Object.fromEntries([...byOp].map(([op, xs]) => {
  const ms = xs.map((x) => x.ms), tail = xs.slice(-Math.max(1, Math.floor(xs.length / 10))).map((x) => x.ms);
  return [op, { n: xs.length, p50: +q(ms, 0.5).toFixed(2), p99: +q(ms, 0.99).toFixed(2), lastDecileP50: +q(tail, 0.5).toFixed(2), totalMs: +ms.reduce((a, b) => a + b, 0).toFixed(0) }];
}));
const report = { config: { tasks: N, open: OPEN, active: ACTIVE, stopped: STOPPED, alerts: alertsSent, storage: flags.storage, runtime: flags.runtime, source, calls: samples.length },
  runMs: +runMs.toFixed(0), ops, shape, storageBytes: bytes, heapMB: +heapMB.toFixed(1), rssMB: +rssMB.toFixed(1),
  cold: { openMs: +openMs.toFixed(1), firstReadMs: +firstReadMs.toFixed(1), firstWriteMs: +firstWriteMs.toFixed(1) } };
console.log(JSON.stringify(report, null, 1));
if (flags.out) fs.writeFileSync(flags.out, JSON.stringify(report, null, 1));
fs.rmSync(dir, { recursive: true, force: true });
