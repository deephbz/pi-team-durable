// Verify the packed artifact, not the source tree: file list, plain-Node import of every export,
// and a minimal TypeScript consumer. Run `npm run build` first.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const manifest = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'));
if (!fs.existsSync(path.join(root, 'dist'))) throw new Error('dist is missing; run `npm run build` first');

const packed = JSON.parse(execFileSync('npm', ['pack', '--json', '--ignore-scripts'], { cwd: root, encoding: 'utf8' }))[0];
const tarball = path.join(root, packed.filename);
const work = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-team-durable-package-'));
try {
  const files = packed.files.map(file => file.path);
  for (const required of ['package.json', 'LICENSE', 'README.md', 'dist/authority/index.js', 'dist/authority/index.d.ts', 'dist/authority/rc7-contract.json'])
    if (!files.includes(required)) throw new Error(`package lacks ${required}`);
  const stray = files.filter(file => !/^(dist\/|package\.json$|LICENSE$|README\.md$)/.test(file) || /\.test\.|\/test\//.test(file));
  if (stray.length) throw new Error(`package holds files outside dist and the documents: ${stray.join(', ')}`);

  fs.writeFileSync(path.join(work, 'package.json'), '{"name":"consumer","private":true,"type":"module"}\n');
  // A local min-release-age policy must not block the pinned peers; CI has none.
  execFileSync('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', tarball], { cwd: work, stdio: 'inherit', env: { ...process.env, npm_config_min_release_age: '0' } });

  // Every export, imported by plain Node. createTeamRuntime reads rc7-contract.json beside the emitted JavaScript.
  const subpaths = Object.keys(manifest.exports).map(key => `${manifest.name}/${key.slice(2)}`);
  const probe = `
    const names = ${JSON.stringify(subpaths)};
    for (const name of names) {
      const mod = await import(name);
      if (Object.keys(mod).length === 0) throw new Error(name + ' exports nothing');
    }
    const { createTeamRuntime } = await import(${JSON.stringify(`${manifest.name}/authority`)});
    const runtime = createTeamRuntime({ modelRoles: [], abortRun: async () => ({}), stop: async () => [], observationPort: async () => ({ commit() {} }) });
    if (!runtime.extensions?.length) throw new Error('createTeamRuntime returned no extensions');
    console.log('imported', names.join(' '), 'with', runtime.extensions.length, 'extensions');
  `;
  execFileSync(process.execPath, ['--input-type=module', '-e', probe], { cwd: work, stdio: 'inherit' });

  fs.writeFileSync(path.join(work, 'probe.ts'),
    `import { createTeamRuntime, TeamDoc, type StopPlan } from '${manifest.name}/authority';\n` +
    `const runtime = createTeamRuntime({ modelRoles: [], abortRun: async () => ({}) as never, stop: async (_api, plans: StopPlan[]) => plans.map(() => ({}) as never), observationPort: async () => ({ commit: () => undefined as never }) });\n` +
    `void runtime; void TeamDoc;\n`);
  fs.writeFileSync(path.join(work, 'tsconfig.json'), JSON.stringify({ compilerOptions: { module: 'NodeNext', moduleResolution: 'NodeNext', target: 'ES2023', strict: true, noEmit: true, skipLibCheck: true, types: ['node'], typeRoots: [path.join(root, 'node_modules/@types')] }, files: ['probe.ts'] }));
  execFileSync(path.join(root, 'node_modules', '.bin', 'tsc'), ['-p', path.join(work, 'tsconfig.json')], { cwd: work, stdio: 'inherit' });
  console.log('packed artifact: file list, plain-Node import, TypeScript consumer ok');
} finally {
  fs.rmSync(tarball, { force: true });
  fs.rmSync(work, { recursive: true, force: true });
}
