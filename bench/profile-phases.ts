// Process-local wall-time probes for bench/scale.ts. Product source stays unchanged.
// node --import ./bench/profile-phases.ts bench/scale.ts --tasks 500 --alerts 1000
import { registerHooks } from 'node:module';
import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SqliteStorage } from '@earendil-works/pi-durable/storage/sqlite';
type Span={start:number;children:number};
type Meter={command:string;wall:number;inclusive:Record<string,number>;exclusive:Record<string,number>};
const context=new AsyncLocalStorage<{meter:Meter;span?:Span}>(),samples:Meter[]=[],sources:Record<string,string>={};
const probes={
 time<T>(name:string,run:()=>T):T {
  const parent=context.getStore();if(!parent)return run();
  const span={start:performance.now(),children:0};
  const finish=()=>{const ms=performance.now()-span.start;parent.meter.inclusive[name]=(parent.meter.inclusive[name]??0)+ms;parent.meter.exclusive[name]=(parent.meter.exclusive[name]??0)+Math.max(0,ms-span.children);if(parent.span)parent.span.children+=ms;};
  try{const result=context.run({meter:parent.meter,span},run);if(result instanceof Promise)return result.finally(finish) as T;finish();return result;}
  catch(error){finish();throw error;}
 },
 command<T>(command:string,run:()=>Promise<T>):Promise<T>{
  const meter:Meter={command,wall:0,inclusive:{},exclusive:{}},start=performance.now();
  return context.run({meter},()=>probes.time('command',run)).finally(()=>{meter.wall=performance.now()-start;samples.push(meter);});
 },
};
(globalThis as any)[Symbol.for('durable-team.phase-probe')]=probes;
const nativeCommit=SqliteStorage.prototype.commit;
SqliteStorage.prototype.commit=function(writes,c){return probes.time('commit',()=>nativeCommit.call(this,writes,c));};
const replace=(source:string,from:string,to:string)=>{if(source.split(from).length!==2)throw new Error(`Phase probe anchor changed: ${from}`);return source.replace(from,to);};
registerHooks({load(url,options,next){
 const loaded=next(url,options),name=url.split('/').at(-1)!;
 if(!url.includes('/src/authority/')||!['store.ts','tools.ts','contracts.ts'].includes(name))return loaded;
 let source=String(loaded.source);sources[name]=createHash('sha256').update(source).digest('hex');
 source="const __phase=globalThis[Symbol.for('durable-team.phase-probe')];\n"+source;
 const wrap=(declaration:string,name:string,label=name)=>{
  const exported=declaration.startsWith('export ');
  source=replace(source,declaration+name+'(',declaration.replace('export ','')+'raw_'+name+'(');
  source+=`\n${exported?'export ':''}const ${name}=(...args)=>__phase.time('${label}',()=>raw_${name}(...args));\n`;
 };
 if(name==='store.ts'){
  wrap('export async function ','openAuthority');wrap('export async function ','taskRecord');
  wrap('async function ','reconcile');wrap('function ','rendered','cardRender');wrap('async function ','writeTask');wrap('export function ','flushAuthority');
  source=replace(source,'const engine = IncrementalGraphTaskController.restore(head,records,()=>new Date(now()));',"const engine = __phase.time('engine',()=>IncrementalGraphTaskController.restore(head,records,()=>new Date(now())));");
  source=replace(source,"const result = command.kind==='apply' ? engine.applyGraph(command.input) : engine.transition(command.input);","const result = __phase.time('engine',()=>command.kind==='apply' ? engine.applyGraph(command.input) : engine.transition(command.input));");
 }else if(name==='contracts.ts')wrap('export function ','checkSchema','validator');
 else {
  source=replace(source,' async function execute(',' async function executeRaw(');
  source=replace(source,' function tools(role:Role)'," const execute=(name,role,input,api,context)=>__phase.command(name,()=>executeRaw(name,role,input,api,context));\n function tools(role:Role)");
 }
 return {...loaded,source};
}});
const quantile=(values:number[],fraction:number)=>{values.sort((a,b)=>a-b);return +(values[Math.floor((values.length-1)*fraction)]??0).toFixed(4);};
process.once('beforeExit',()=>{
 const commands=Object.fromEntries([...new Set(samples.map(s=>s.command))].map(command=>{
  const rows=samples.filter(s=>s.command===command),phases=[...new Set(rows.flatMap(s=>Object.keys(s.inclusive)))];
  return [command,{n:rows.length,wallP50:quantile(rows.map(s=>s.wall),.5),wallP99:quantile(rows.map(s=>s.wall),.99),phases:Object.fromEntries(phases.map(phase=>[phase,{inclusiveP50:quantile(rows.map(s=>s.inclusive[phase]??0),.5),exclusiveP50:quantile(rows.map(s=>s.exclusive[phase]??0),.5)}]))}];
 }));
 writeFileSync(process.env.PTB_PHASE_OUTPUT??join(tmpdir(),'ptb-phase-profile.json'),JSON.stringify({units:'milliseconds',sources,commands},null,2));
});
