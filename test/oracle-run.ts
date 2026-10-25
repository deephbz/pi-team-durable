// Run independent oracle seed ranges in bounded child processes. Each seed owns two MemoryStorage Harnesses.
// --against oracle checks the comparator against two frozen runtimes. ORACLE_TRACE_IDS=1 prints each seed's ID map.
import { spawn, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';
import { runTargeted } from './authority/differential.ts';

const { values } = parseArgs({ options: {
  seeds: { type: 'string', default: process.env.ORACLE_SEEDS ?? '500' },
  shards: { type: 'string', default: '4' },
  seed: { type: 'string', default: process.env.ORACLE_SEED ?? '1' },
  steps: { type: 'string', default: process.env.ORACLE_STEPS ?? '30' },
  'targeted-only': { type: 'boolean', default: false },
  against: { type: 'string', default: process.env.ORACLE_AGAINST ?? 'current' },
  'candidate-ref': { type: 'string' },
  'keep-going': { type: 'boolean', default: false },
} });
const seeds = Number(values.seeds), shards = Math.min(Number(values.shards), seeds), first = Number(values.seed), steps = Number(values.steps);
if (![seeds, shards, first, steps].every(Number.isSafeInteger) || seeds < 1 || shards < 1 || first < 0 || steps < 0) throw new Error('Invalid seeds/shards/seed/steps');
if (!['current', 'oracle'].includes(values.against!)) throw new Error('Invalid --against runtime');
process.env.ORACLE_AGAINST = values.against;
process.env.ORACLE_KEEP_GOING = values['keep-going'] ? '1' : '0';
let candidateDirectory: string | undefined, candidateRef: string | undefined;
if (values['candidate-ref']) {
  if (values.against !== 'current') throw new Error('--candidate-ref requires --against current');
  const cwd = fileURLToPath(new URL('..', import.meta.url));
  candidateRef = execFileSync('git', ['rev-parse', '--verify', `${values['candidate-ref']}^{commit}`], { cwd, encoding: 'utf8' }).trim();
  candidateDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'ptb-oracle-candidate-'));
  const archive = execFileSync('git', ['archive', candidateRef, 'src/authority'], { cwd, maxBuffer: 8 * 1024 * 1024 });
  execFileSync('tar', ['-x', '-C', candidateDirectory], { input: archive });
  fs.symlinkSync(fs.realpathSync(new URL('../node_modules', import.meta.url)), path.join(candidateDirectory, 'node_modules'), 'dir');
  fs.writeFileSync(path.join(candidateDirectory, 'package.json'), '{"type":"module","private":true}\n');
  process.env.ORACLE_CANDIDATE_MODULE = pathToFileURL(path.join(candidateDirectory, 'src/authority/runtime.ts')).href;
  process.env.ORACLE_CANDIDATE_REF = candidateRef;
  console.log(JSON.stringify({ candidateRef, candidateModule: process.env.ORACLE_CANDIDATE_MODULE }));
}
const start = performance.now();
const children: ReturnType<typeof spawn>[] = [];
const interrupt = () => { for (const child of children) child.kill('SIGTERM'); };
process.once('SIGINT', interrupt); process.once('SIGTERM', interrupt);
try {
  await runTargeted();
  let failedShards = 0;
  if (!values['targeted-only']) {
    const jobs = Array.from({ length: shards }, (_, shard) => new Promise<void>((resolve, reject) => {
    const count = Math.floor(seeds / shards) + (shard < seeds % shards ? 1 : 0);
    const offset = shard * Math.floor(seeds / shards) + Math.min(shard, seeds % shards);
    const child = spawn(process.execPath, [fileURLToPath(new URL('./authority/differential.ts', import.meta.url))], {
      stdio: 'inherit', env: { ...process.env, ORACLE_SEED: String(first + offset), ORACLE_SEEDS: String(count), ORACLE_STEPS: String(steps) },
    });
    children.push(child);
    child.once('error', reject);
    child.once('exit', (code, signal) => code === 0 ? resolve() : reject(new Error(`Oracle shard ${shard} failed: ${signal ?? code}`)));
    }));
    if (values['keep-going']) {
      const results = await Promise.allSettled(jobs);
      failedShards = results.filter(result => result.status === 'rejected').length;
      if (failedShards) process.exitCode = 1;
    } else await Promise.all(jobs);
  }
  console.log(JSON.stringify({ against: values.against, candidateRef, failedShards, seeds: values['targeted-only'] ? 0 : seeds, shards: values['targeted-only'] ? 0 : shards, first, steps, wallSeconds: +((performance.now() - start) / 1000).toFixed(2) }));
} catch (error) { interrupt(); throw error; }
finally {
  process.removeListener('SIGINT', interrupt); process.removeListener('SIGTERM', interrupt);
  if (candidateDirectory) fs.rmSync(candidateDirectory, { recursive: true, force: true });
}
