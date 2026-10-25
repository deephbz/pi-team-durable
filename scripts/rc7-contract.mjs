// Run against the pinned PTB source checkout. Emits only executable schemas;
// it never reads agent-surface.json or imports the extension's runtime.
import { createJiti } from 'jiti';
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
const sources = ['src/model-tool-contract/catalog.ts', 'src/model-tool-contract/result-projection.ts', 'src/model-tool-contract/worker-parameters.ts', 'src/task-authority/graph-control-schemas.ts'];
console.log(JSON.stringify({
  source: { package: '@hypercarrier/pi-team-bright', version: '0.24.0-rc.7', revision: '382950ce62bd38b6cd9f7fb80b91ef5b9140e15a', files: Object.fromEntries(sources.map(p => [p, createHash('sha256').update(readFileSync(resolve(root, p))).digest('hex')])) },
  lead: parameters,
  worker: { task_read: worker.WorkerTaskReadParametersSchema, task_update: parameters.task_update, alert_send: worker.WorkerAlertSendParametersSchema },
  results,
}));
