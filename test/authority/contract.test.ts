import { expect, test, vi } from 'vitest';
import { Compile } from 'typebox/compile';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { contracts, checkSchema, TOOL_NAMES } from '../../src/authority/contracts.ts';
import { createAuthorityTools } from '../../src/authority/tools.ts';

vi.mock('typebox/compile',async importOriginal=>{
 const actual=await importOriginal<typeof import('typebox/compile')>();
 return {...actual,Compile:vi.fn(actual.Compile)};
});

// The rc.7 source (revision 382950c) is not on npm. Pi Team Bright 0.25.1 ships the same files; the hash test proves it.
const source=fileURLToPath(new URL('../../node_modules/@hypercarrier/pi-team-bright',import.meta.url));
test('the pinned Pi Team Bright package holds the rc.7 contract source (382950c)',()=>{
 expect(JSON.parse(readFileSync(resolve(source,'package.json'),'utf8')).version).toBe('0.25.1');
 const saved=JSON.parse(readFileSync('src/authority/rc7-contract.json','utf8'));
 expect(saved.source.revision).toBe('382950ce62bd38b6cd9f7fb80b91ef5b9140e15a');
 const installed=Object.fromEntries(Object.keys(saved.source.files).map(file=>[file,createHash('sha256').update(readFileSync(resolve(source,file))).digest('hex')]));
 expect(installed).toEqual(saved.source.files);
});
test('all lead and Worker registrations match executable rc.7 parameters and result schemas',()=>{
 const generated=JSON.parse(execFileSync(process.execPath,['scripts/rc7-contract.mjs',source],{encoding:'utf8',timeout:60000,killSignal:'SIGKILL',env:{...process.env,PI_CODING_AGENT_DIR:'/tmp/ptb-core-synthetic-empty'}}));
 const saved=JSON.parse(readFileSync('src/authority/rc7-contract.json','utf8'));
 expect(saved).toEqual(generated);
 const core=createAuthorityTools({modelRoles:[]});
 for(const role of ['lead','worker'] as const)for(const tool of core[role]){
  expect(tool.parameters).toEqual(generated[role][tool.name]);
  expect(tool.replay).toBe('safe');
 }
 for(const name of TOOL_NAMES)expect(core.contract.results[name]).toEqual(generated.results[name]);
},60000); // The synchronous child-process limit bounds a hang; it does not measure speed.
test('goal limits are configurable without mutating the source contract',()=>{
 expect((contracts(2000).lead.task_graph_apply as any).properties.tasks.items.properties.goal.maxLength).toBe(2000);
 expect((contracts().lead.task_graph_apply as any).properties.tasks.items.properties.goal.maxLength).toBe(1600);
 expect(()=>contracts(0)).toThrow();
});

test('contracts compile lazily and reuse validators for equal schema content',()=>{
 const compile=vi.mocked(Compile);compile.mockClear();
 const first=contracts();
 expect(compile).not.toHaveBeenCalled();
 const second=contracts();
 expect(compile).not.toHaveBeenCalled();
 expect(checkSchema(first.lead.team_create,{name:'synthetic',purpose:'Check'})).toBe(true);
 expect(compile).toHaveBeenCalledTimes(1);
 const reordered=Object.fromEntries(Object.entries(second.lead.team_create).reverse());
 expect(checkSchema(reordered,{name:'synthetic',purpose:'Check',extra:true})).toBe(false);
 expect(checkSchema(second.lead.team_create,{name:'synthetic'})).toBe(false);
 expect(compile).toHaveBeenCalledTimes(1);
 const input={operation_id:'plan',tasks:[{key:'a',title:'A',goal:'a'.repeat(1601),assignee:'builder'}]};
 expect(checkSchema(first.lead.task_graph_apply,input)).toBe(false);
 expect(checkSchema(contracts(2000).lead.task_graph_apply,input)).toBe(true);
 expect(compile).toHaveBeenCalledTimes(3);
 (first.lead.team_create as any).properties.purpose.maxLength=3;
 expect(checkSchema(first.lead.team_create,{name:'synthetic',purpose:'Check'})).toBe(false);
 expect(checkSchema(second.lead.team_create,{name:'synthetic',purpose:'Check'})).toBe(true);
 expect(compile).toHaveBeenCalledTimes(4);
});
