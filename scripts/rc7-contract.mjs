// Run against the pinned PTB source checkout. Emits executable schemas and each role's tool labels and descriptions;
// it never reads agent-surface.json. Schemas come from the contract modules. Labels and descriptions come from the
// extension's own registration, captured per role through an inert host (pi-registration.mjs).
import { createJiti } from 'jiti';
import { captureRegistration } from './pi-registration.mjs';
import { resolve } from 'node:path';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
const root = resolve(process.argv[2]);
const jiti = createJiti(import.meta.url, { fsCache: false });
const catalog = await jiti.import(resolve(root, 'src/model-tool-contract/catalog.ts'));
const projection = await jiti.import(resolve(root, 'src/model-tool-contract/result-projection.ts'));
const worker = await jiti.import(resolve(root, 'src/model-tool-contract/worker-parameters.ts'));
const parameters = Object.fromEntries(catalog.modelToolCatalog.tools.map(t => [t.name, t.parameters]));
const results = Object.fromEntries(Object.keys(parameters).map(name => [name, projection.ModelResultSchemas[name]]));
const presentation = Object.fromEntries(['lead', 'worker'].map(role => [role,
  Object.fromEntries(captureRegistration(root, role).map(t => [t.name, { label: t.label, description: t.description }]))]));
const sources = ['src/model-tool-contract/catalog.ts', 'src/model-tool-contract/result-projection.ts', 'src/model-tool-contract/worker-parameters.ts', 'src/task-authority/graph-control-schemas.ts', 'src/model-tool-contract/pi-registration.ts', 'extensions/index.ts'];
console.log(JSON.stringify({
  source: { package: '@hypercarrier/pi-team-bright', version: '0.24.0-rc.7', revision: '382950ce62bd38b6cd9f7fb80b91ef5b9140e15a', files: Object.fromEntries(sources.map(p => [p, createHash('sha256').update(readFileSync(resolve(root, p))).digest('hex')])) },
  lead: parameters,
  worker: { task_read: worker.WorkerTaskReadParametersSchema, task_update: parameters.task_update, alert_send: worker.WorkerAlertSendParametersSchema },
  results,
  presentation,
}));
