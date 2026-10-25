// Capture the model tools that Pi Team Bright registers for one role: name, label, description and parameters.
// Usage: node scripts/pi-registration.mjs <pi-team-bright package root> <lead|worker>
// The extension runs against an inert host that records registerTool calls and ignores every other call.
// Run each role in its own process: the extension reads the role from PI_AGENT_NAME when it registers.
import { createJiti } from 'jiti';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

/** Runs the capture for one role in a child process with an empty home and a fixed role identity. */
export function captureRegistration(root, role) {
  if (role !== 'lead' && role !== 'worker') throw new Error(`Unknown role ${role}`);
  const home = mkdtempSync(resolve(tmpdir(), 'ptb-registration-'));
  try {
    const child = spawnSync(process.execPath, [fileURLToPath(import.meta.url), root, role], {
      encoding: 'utf8', timeout: 60000, killSignal: 'SIGKILL', maxBuffer: 16 * 1024 * 1024,
      env: { PATH: process.env.PATH, HOME: home, PI_CODING_AGENT_DIR: resolve(home, 'agent'), ...(role === 'worker' ? { PI_AGENT_NAME: 'builder' } : {}) },
    });
    if (child.status !== 0) throw new Error(`Pi Team Bright ${role} registration capture failed: ${child.error ?? child.stderr}`);
    return JSON.parse(child.stdout);
  } finally { rmSync(home, { recursive: true, force: true }); }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [root, role] = process.argv.slice(2);
  const jiti = createJiti(import.meta.url, { fsCache: false });
  const extension = await jiti.import(resolve(root, 'extensions/index.ts'), { default: true });
  const tools = [];
  const inert = new Proxy(function () {}, { get: (_target, key) => key === 'then' ? undefined : inert, apply: () => undefined });
  const host = new Proxy({}, { get: (_target, key) => key === 'registerTool'
    ? tool => tools.push({ name: tool.name, label: tool.label, description: tool.description, parameters: JSON.parse(JSON.stringify(tool.parameters)) })
    : key === 'then' ? undefined : inert });
  await extension(host);
  if (new Set(tools.map(tool => tool.name)).size !== tools.length) throw new Error(`Pi Team Bright registered a ${role} tool twice`);
  process.stdout.write(JSON.stringify(tools));
}
