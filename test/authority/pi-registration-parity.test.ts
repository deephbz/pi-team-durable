// Durable role tools present what the installed Pi Team Bright registers: names in order, labels, descriptions and
// parameter schemas. The package is resolved by name, so this checks whatever version the host installs.
import { createRequire } from 'node:module';
import { dirname } from 'node:path';
import { expect, test } from 'vitest';
import { captureRegistration } from '../../scripts/pi-registration.mjs';
import { createAuthorityTools } from '../../src/authority/tools.ts';

const root = dirname(dirname(createRequire(import.meta.url).resolve('@hypercarrier/pi-team-bright')));
// Known gap, deferred to 0.1.0-rc.3: Pi's Worker alert_send requires non-empty text and task_id. Durable keeps the
// rc.7 worker-parameters schema, so rc.2 changes model-facing text only.
const WORKER_ALERT_MIN_LENGTH_GAP = ['text', 'task_id'];

for (const role of ['lead', 'worker'] as const) test(`${role} tools carry Pi's registered names, labels, descriptions and parameters`, () => {
  const pi: { name: string; label: string; description: string; parameters: any }[] = captureRegistration(root, role);
  const core = createAuthorityTools({ modelRoles: [] })[role];
  expect(core.map(tool => tool.name)).toEqual(pi.map(tool => tool.name));
  expect(core.map(tool => ({ name: tool.name, label: tool.label, description: tool.description })))
    .toEqual(pi.map(tool => ({ name: tool.name, label: tool.label, description: tool.description })));
  for (const [index, tool] of core.entries()) {
    const expected = structuredClone(pi[index].parameters);
    if (role === 'worker' && tool.name === 'alert_send') for (const field of WORKER_ALERT_MIN_LENGTH_GAP) {
      expect(expected.properties[field].minLength).toBe(1);
      delete expected.properties[field].minLength;
    }
    expect(tool.parameters).toEqual(expected);
  }
}, 120000); // The capture child has its own 60 s kill bound; this bound only stops a hung test.
