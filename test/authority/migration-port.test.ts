import { expect,test } from 'vitest';
import { mkdtempSync,rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxProvider,fauxAssistantMessage as answer } from '@earendil-works/pi-ai/providers/faux';
import { Harness,createRegistry,LiveDoc,type TaskId,type StorageWrite } from '@earendil-works/pi-durable';
import { openNodeSqliteStorage } from '@earendil-works/pi-durable/storage/sqlite/node';
import { createTeamRuntime as oldRuntime } from '../oracle/authority/runtime.ts';
import { abortExpectedRun as oldAbort } from '../oracle/authority/interrupt.ts';
import { open as oldOpen } from '../oracle/authority/state.ts';
import { createTeamRuntime } from '../../src/authority/runtime.ts';
import { abortExpectedRun,type InterruptTarget } from '../../src/authority/interrupt.ts';
import { open,plain } from '../../src/authority/state.ts';
import { openAuthority,pendingObligations,unscheduledObligations } from '../../src/authority/store.ts';
import type { CoreApi } from '../../src/authority/tools.ts';

const deferred=()=>{let resolve!:()=>void;const promise=new Promise<void>(r=>resolve=r);return {promise,resolve};};
test('P SQLite migration preserves every Alert and obligation field and resumes a captured interrupt phase',async()=>{
 const directory=mkdtempSync(join(tmpdir(),'ptb-p-migration-')),file=join(directory,'team.sqlite');
 const models=createModels(),faux=fauxProvider({provider:'migration-faux',api:'migration-faux',models:[{id:'test'}],tokensPerSecond:0});models.setProvider(faux.provider);
 let h!:Harness,root!:Awaited<ReturnType<Harness['root']>>,serial=1000000,fault=false,hit=false;
 const interrupts:InterruptTarget[]=[];
 const runtime=(legacy:boolean)=>(legacy?oldRuntime:createTeamRuntime)({
  modelRoles:[{name:'builder',use:'Build',model:{provider:'migration-faux',modelId:'test'}}],defaultModelRole:'builder',
  observationPort:async()=>({commit:(fn,c)=>h.commit(fn,c)}),
  abortRun:async(_api,target,c)=>{if(!legacy)interrupts.push({...target});return (legacy?oldAbort:abortExpectedRun)(h,target,c);},
  stop:async()=>[],
 });
 const start=async(legacy:boolean)=>{
  const rt=runtime(legacy),registry=createRegistry();for(const extension of rt.extensions)registry.install(extension);
  const db=await openNodeSqliteStorage(file),commit=db.commit.bind(db);
  db.commit=async(writes:readonly StorageWrite[],c)=>{
   const seq=await commit(writes,c);
   if(fault&&writes.some(w=>w.type==='task'&&w.value.kind==='ptb.deliver'&&'checkpoint' in w.value.state&&typeof w.value.state.checkpoint==='object'&&w.value.state.checkpoint!==null&&!Array.isArray(w.value.state.checkpoint)&&w.value.state.checkpoint.phase==='interrupt')){fault=false;hit=true;throw new Error('Synthetic loss after P interrupt intent');}
   return seq;
  };
  h=await Harness.open(db,{models,registry,now:()=>1000,setTimer:()=>()=>{},onWake:()=>{},onReport:()=>{},settings:{retry:{maxRetries:0},compaction:{enabled:false,keepRecentTokens:0}}},context);
  root=await h.root(context,{agent:{model:{provider:'migration-faux',modelId:'test'},extensions:[rt.mechanics,rt.lead,rt.observation]}});
  return rt;
 };
 const api=():CoreApi=>({taskId:++serial as TaskId,conversationId:root.id,commit:root.commit.bind(root),now:()=>1000,callId:`direct-${serial}`});
 const released=deferred();
 try{
  const legacy=await start(true);
  await legacy.core.execute('team_create','lead',{name:'migration-demo',purpose:'Keep accepted work'},api(),context);
  await legacy.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},api(),context);
  const beforeWorker=await root.commit(async tx=>plain((await oldOpen(tx)).team.current!.workers.builder),context);
  const target=(await h.conversation(beforeWorker.conversationId,context))!,started=deferred();
  faux.setResponses([async(_messages,options)=>{options?.signal?.addEventListener('abort',released.resolve,{once:true});started.resolve();await released.promise;return answer([],{stopReason:'aborted'});}]);
  await target.submit({type:'input',content:'Busy before migration'},context);h.resume();await started.promise;
  const captured=(await h.snapshot(LiveDoc,beforeWorker.conversationId,context))!.run!.taskId;
  const queued=await target.submit({type:'input',content:'Queued owner survives migration',whenBusy:'followUp',requestId:'owner-queue'},context);
  const graph=await legacy.core.execute('task_graph_apply','lead',{operation_id:'plan',tasks:[{key:'sample',title:'Sample',goal:'Works',assignee:'builder'}]},api(),context) as any;
  fault=true;
  await legacy.core.execute('alert_send','lead',{to:'builder',kind:'attention',text:'Preserve all Alert fields',task_id:'sample',task_version:graph.tasks_by_key.sample.version,interrupt:true},api(),context).catch(()=>undefined);
  await h.closed;expect(hit).toBe(true);
  await start(true);
  const prior=await root.commit(async tx=>plain(await oldOpen(tx)),context);
  const accepted=Object.values(prior.outbox.obligations).find(o=>o.kind==='alert')!;
  expect(accepted.interruptRun).toBe(captured);expect(accepted.interrupt).toBe(true);
  expect((await h.getTask(accepted.deliveryTaskId!,context))!.state).toMatchObject({checkpoint:{phase:'interrupt'}});
  await h.close(context);
  await start(false);
  const migrated=await root.commit(async tx=>{
   const s=await openAuthority(tx);
   expect(plain(await pendingObligations(s))).toEqual(Object.values(prior.outbox.obligations).filter(o=>o.state==='pending'));
   expect(plain(await unscheduledObligations(s))).toEqual(Object.values(prior.outbox.obligations).filter(o=>o.state==='pending'&&!o.deliveryTaskId));
   return plain(await open(tx));
  },context);
  expect(migrated.outbox).toEqual(prior.outbox);
  expect((await h.getTask(accepted.deliveryTaskId!,context))!.state).toMatchObject({checkpoint:{phase:'interrupt'}});
  faux.setResponses(Array.from({length:12},()=>answer('Recovered input')));h.resume();
  await h.waitForTask(accepted.deliveryTaskId!,context);
  expect(interrupts).toEqual([{key:accepted.key,conversationId:accepted.conversationId,expectedRunTaskId:captured,interrupt:true,epoch:accepted.epoch,content:JSON.stringify({kind:'alert',alert:prior.outbox.alerts[accepted.alertId!]}),whenBusy:'steer',kind:'alert',sender:'team-lead'}]);
  expect((await (await h.submission(queued.id,context))!.wait(context)).status).toBe('done');
  const settled=await root.commit(async tx=>plain((await open(tx)).outbox.obligations[accepted.key]),context);
  expect(settled.outcome).toBe('answered');expect(['ended','interrupted']).toContain(settled.interruption);
  const entries=(await (await h.conversation(beforeWorker.conversationId,context))!.entries({order:'ascending'},100,undefined,context)).items;
  const notices=entries.filter(e=>e.kind==='team.notice'&&typeof e.data==='object'&&e.data!==null&&!Array.isArray(e.data)&&e.data.deliveryId===accepted.key);
  expect(notices).toHaveLength(1);
  expect(notices[0].data).toEqual({source:'team',deliveryId:accepted.key,epoch:accepted.epoch,delivery:'alert',sender:'team-lead',interrupt:true});
 }finally{released.resolve();await h?.close(context);rmSync(directory,{recursive:true,force:true});}
},20000);
