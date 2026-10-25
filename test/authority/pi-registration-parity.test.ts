// Durable role tools present what the installed Pi Team Bright registers: names in order, labels, descriptions and
// parameter schemas. The package is resolved by name, so this checks whatever version the host installs.
import { createRequire } from 'node:module';
import { dirname } from 'node:path';
import { expect, test } from 'vitest';
import { captureRegistration } from '../../scripts/pi-registration.mjs';
import { createAuthorityTools } from '../../src/authority/tools.ts';

const root = dirname(dirname(createRequire(import.meta.url).resolve('@hypercarrier/pi-team-bright')));

for (const role of ['lead', 'worker'] as const) test(`${role} tools carry Pi's registered names, labels, descriptions and parameters`, () => {
  const pi: { name: string; label: string; description: string; parameters: any }[] = captureRegistration(root, role);
  const core = createAuthorityTools({ modelRoles: [] })[role];
  expect(core.map(tool => tool.name)).toEqual(pi.map(tool => tool.name));
  expect(core.map(tool => ({ name: tool.name, label: tool.label, description: tool.description })))
    .toEqual(pi.map(tool => ({ name: tool.name, label: tool.label, description: tool.description })));
  for (const [index, tool] of core.entries()) expect(tool.parameters).toEqual(pi[index].parameters);
}, 120000); // The capture child has its own 60 s kill bound; this bound only stops a hung test.
