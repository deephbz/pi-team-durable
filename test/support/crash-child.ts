// Test-only crash carrier: a fixture child names barriers over IPC, and the test kills it with SIGKILL at one.
// Deadlines only bound a hung child.
import { fork, type ChildProcess } from 'node:child_process';
import { appendFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

export type ChildEvent = { type: string; name?: string; value?: any };
export class CrashChild {
  readonly child: ChildProcess;
  readonly events: ChildEvent[] = [];
  readonly exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  #listeners = new Set<() => void>();
  #ended = false;
  #output = '';
  /** Runs `fixture <caseName> <dir> <mode>` with HOME in `dir`. Output goes to `<dir>/<mode>.log`. */
  constructor(dir: string, caseName: string, mode: string, fixture: URL) {
    this.child = fork(fileURLToPath(fixture), [caseName, dir, mode], { execArgv: [], stdio: ['ignore', 'pipe', 'pipe', 'ipc'], env: { ...process.env, HOME: dir } });
    for (const stream of [this.child.stdout, this.child.stderr]) stream!.on('data', chunk => {
      this.#output += String(chunk);
      appendFileSync(join(dir, `${mode}.log`), chunk);
    });
    this.child.on('message', message => { this.events.push(message as ChildEvent); for (const notify of this.#listeners) notify(); });
    this.exited = new Promise(resolve => this.child.once('close', (code, signal) => {
      this.#ended = true; resolve({ code, signal });
      for (const notify of this.#listeners) notify();
    }));
  }
  /** Resolves with the first event of `type` (and `name`). Rejects on a `failure` event, child exit, or the deadline. */
  wait(type: string, name: string | undefined, timeoutMs: number): Promise<ChildEvent> {
    return new Promise((resolve, reject) => {
      const end = () => { clearTimeout(timer); this.#listeners.delete(check); };
      const check = () => {
        const event = this.events.find(e => e.type === type && (name === undefined || e.name === name));
        const failure = this.events.find(e => e.type === 'failure');
        if (failure) { end(); reject(new Error(`child failure: ${failure.value}`)); }
        else if (event) { end(); resolve(event); }
        else if (this.#ended) { end(); reject(new Error(`child ended before ${type}/${name}: ${this.#output}`)); }
      };
      const timer = setTimeout(() => { end(); reject(new Error(`IPC deadline ${type}/${name}: ${this.#output}`)); }, timeoutMs);
      this.#listeners.add(check);
      check();
    });
  }
  cleanup(): void { if (!this.#ended) this.child.kill('SIGKILL'); }
}
