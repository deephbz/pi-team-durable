// Floor cost of pi-durable commits that a Task authority command cannot avoid: one SQLite transaction that
// changes k small documents (always stored as a base) and appends one entry.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { createRegistry, defineDoc, defineDocFamily, Harness, type JsonObject } from '@earendil-works/pi-durable';
import { openNodeSqliteStorage } from '@earendil-works/pi-durable/storage/sqlite/node';

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ptb-floor-'));
const harness = await Harness.open(await openNodeSqliteStorage(path.join(dir, 'h.sqlite')), { models: createModels(), registry: createRegistry(), now: () => 1000, setTimer: () => () => {} }, context);
const root = await harness.root(context);
const Task = defineDocFamily<JsonObject, null>({ kind: 'floor.task', version: 1, scope: 'session', family: true, initial: () => ({ n: 0, goal: 'x'.repeat(600) }), checkpointWhen: () => true });
const Head = defineDoc<JsonObject>({ kind: 'floor.head', version: 1, scope: 'session', initial: () => ({ seq: 0 }), checkpointWhen: () => true });
const log = await harness.commit(async (tx) => (await tx.createConversation({ ownership: { kind: 'ownerless' } })).id, context);
for (let i = 0; i < 500; i++) await harness.commit(async (tx) => { (await tx.doc(Task, `t${i}`, null)).n = 0; }, context);
const q = (xs: number[], p: number) => [...xs].sort((a, b) => a - b)[Math.floor(p * xs.length)];
for (const k of [0, 1, 3]) {
  const ms: number[] = [];
  for (let i = 0; i < 2000; i++) {
    const t0 = performance.now();
    await harness.commit(async (tx) => {
      (await tx.doc(Head)).seq = i;
      for (let j = 0; j < k; j++) (await tx.doc(Task, `t${(i * 7 + j) % 500}`, null)).n = i;
      await tx.appendEntry(log, { kind: 'floor.event', data: { seq: i, task: `t${i % 500}`, text: 'claimed' } });
    }, context);
    ms.push(performance.now() - t0);
  }
  console.log(`head + ${k} task docs + 1 entry: p50 ${q(ms, 0.5).toFixed(3)} ms, p99 ${q(ms, 0.99).toFixed(3)} ms`);
}
await harness.close(context);
console.log('bytes', fs.readdirSync(dir).reduce((s, f) => s + fs.statSync(path.join(dir, f)).size, 0));
fs.rmSync(dir, { recursive: true, force: true });
void root;
