import { afterEach,expect,test } from 'vitest';
import { mkdtempSync,rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openNodeSqliteStorage } from '@earendil-works/pi-durable/storage/sqlite/node';
import * as legacyState from '../oracle/authority/state.ts';
import { ObservationDoc as LegacyObservationDoc } from '../oracle/authority/observation.ts';
import { contracts as legacyContracts } from '../oracle/authority/contracts.ts';
import { TeamDoc,GraphDoc,JournalDoc,OutboxDoc,ReceiptDoc } from '../../src/authority/state.ts';
import { ObservationDoc } from '../../src/authority/observation-store.ts';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { createRegistry,Harness,MemoryStorage,type StorageWrite,type TaskId } from '@earendil-works/pi-durable';
import { createAuthorityTools,type CoreApi } from '../../src/authority/tools.ts';
import { createAuthorityTools as legacyTools } from '../oracle/authority/tools.ts';
import { open as legacyOpen } from '../oracle/authority/state.ts';
import { open,plain } from '../../src/authority/state.ts';
import { OperationDoc,openAuthority,taskAddress,readGraphHistory } from '../../src/authority/store.ts';
import { readTeamPanel } from '../../src/authority/panel.ts';
import { GraphTaskController } from '../../src/authority/graph-control.ts';
const close:Array<()=>Promise<void>>=[];
afterEach(async()=>{for(const fn of close.splice(0))await fn();});
class Storage extends MemoryStorage {
 fault?:{after:boolean;matches:(writes:readonly StorageWrite[])=>boolean};
 writes:readonly StorageWrite[]=[];
 override async close(){}
 override async commit(writes:readonly StorageWrite[],c:typeof context){
  this.writes=writes;const fault=this.fault?.matches(writes)?this.fault:undefined;if(fault)this.fault=undefined;
  if(fault&&!fault.after)throw new Error('before native commit');
  const seq=await super.commit(writes,c);if(fault)throw new Error('after native commit');return seq;
 }
}
async function fixture(legacy=false){
 const storage=new Storage(),options={models:createModels(),registry:createRegistry(),now:()=>1000,setTimer:()=>()=>{}};
 let h=await Harness.open(storage,options,context),root=await h.root(context),n=1000000;
 const config={modelRoles:[{name:'builder',use:'Build',model:{provider:'fake',modelId:'fake'}}],defaultModelRole:'builder',createWorker:async(tx:any)=>{const c=await tx.createConversation({ownership:{kind:'ownerless'}});return {conversationId:c.id,anchorId:9000000 as TaskId};},stopWorker:async()=>true};
 let core=legacy?legacyTools(config):createAuthorityTools(config);
 const api=(id=++n,conversationId=root.id):CoreApi=>({taskId:id as TaskId,conversationId,commit:root.commit.bind(root),now:()=>1000});
 close.push(()=>h.close(context));
 const call=(name:string,input:unknown,role:'lead'|'worker'='lead',conversationId=root.id)=>core.execute(name as any,role,input,api(undefined,conversationId),context) as Promise<any>;
 await call('team_create',{name:'synthetic',purpose:'Storage checks'});await call('ensure_worker',{name:'builder',scope:'Build'});
 const worker=(await root.commit(async tx=>(await (legacy?legacyOpen:open)(tx)).team.current!.workers.builder.conversationId,context));
 return {storage,call,api,worker,get core(){return core;},get h(){return h;},get root(){return root;},
  cutover:()=>{core=createAuthorityTools(config);},state:()=>h.commit(async tx=>plain(await open(tx)),context),
  reopen:async()=>{await h.close(context);h=await Harness.open(storage,options,context);root=await h.root(context);},
 };
}
const def=(key:string,needs:string[]=[])=>({key,title:key,goal:`Complete ${key}`,assignee:'builder',needs});

test('one-way migration preserves facts, old call bytes, operation replay, and the log fold',async()=>{
 const f=await fixture(true),args={operation_id:'g1',tasks:[def('a'),def('b',['a'])]},api=f.api();
 const applied=await f.core.execute('task_graph_apply','lead',args,api,context) as any;
 const claim=await f.call('task_update',{operation_id:'claim',task_id:'a',expected_version:applied.tasks_by_key.a.version,transition:'claim'},'worker',f.worker);
 await f.call('task_update',{operation_id:'finish',task_id:'a',expected_version:claim.task.version,transition:'goal_achieved',evidence:'Verified'},'worker',f.worker);
 const before=await f.h.commit(async tx=>plain(await legacyOpen(tx)),context);
 f.cutover();await f.reopen();
 expect(await f.state()).toEqual(before);
 expect(await f.core.execute('task_graph_apply','lead',args,{...api,commit:f.root.commit.bind(f.root)},context)).toEqual(applied);
 expect(await f.call('task_graph_apply',args)).toEqual({...applied,replayed:true});
 const snapshot=await f.h.commit(async tx=>readGraphHistory(await openAuthority(tx)),context);
 expect(snapshot).toEqual(before.graph.snapshot);
 expect(GraphTaskController.recover(snapshot).readTasks().map(t=>t.state.kind)).toEqual(['goal_achieved','ready']);
 await f.reopen();expect((await f.call('task_read',{task_ids:['b']})).outcomes[0].task.status).toBe('ready');
},30000);

test('removed keys retain old Attempts and ordinals across a cold reopen',async()=>{
 const f=await fixture(),g=await f.call('task_graph_apply',{operation_id:'g1',tasks:[def('a'),def('b')]});
 await f.call('task_update',{operation_id:'claim-1',task_id:'a',expected_version:g.tasks_by_key.a.version,transition:'claim'},'worker',f.worker);
 const removed=await f.call('task_graph_apply',{operation_id:'remove',expected_graph_version:g.graph_version,tasks:[def('b')]});
 await f.reopen();
 const restored=await f.call('task_graph_apply',{operation_id:'restore',expected_graph_version:removed.graph_version,tasks:[def('a'),def('b')]});
 await f.call('task_update',{operation_id:'claim-2',task_id:'a',expected_version:restored.tasks_by_key.a.version,transition:'claim'},'worker',f.worker);
 const panel=await f.h.commit(tx=>readTeamPanel(tx,'synthetic'),context);
 expect(panel.tasks![0].attempts.map(a=>a.id)).toEqual(['a@1','a@2']);
 const state=await f.state(),oracle=GraphTaskController.recover(state.graph.snapshot!);
 expect(oracle.readTask('a').state).toEqual({kind:'in_progress',attemptId:'a@2'});
 expect(panel.tasks![0].version).toBe(oracle.readTask('a').version);
},30000);

for(const after of [false,true])test(`graph writes remain atomic across storage failure ${after?'after':'before'} commit`,async()=>{
 const f=await fixture(),g=await f.call('task_graph_apply',{operation_id:'g',tasks:[def('a')]});
 const before=await f.state(),args={operation_id:'claim',task_id:'a',expected_version:g.tasks_by_key.a.version,transition:'claim'},api=f.api(undefined,f.worker);
 f.storage.fault={after,matches:writes=>writes.some(w=>w.type==='entry'&&w.value.kind==='ptb.graph-event')};
 await expect(f.core.execute('task_update','worker',args,api,context)).rejects.toThrow();
 await f.reopen();const state=await f.state();
 expect(state.graph.snapshot!.events.length).toBe(before.graph.snapshot!.events.length+(after?1:0));
 const result=await f.core.execute('task_update','worker',args,{...api,commit:f.root.commit.bind(f.root)},context) as any;
 expect(result.kind).toBe('updated');
 const final=await f.state();expect(final.graph.snapshot!.events.filter(e=>e.kind==='attempt_started')).toHaveLength(1);
 expect(Object.keys(final.outbox.obligations)).toEqual(Object.keys(before.outbox.obligations));
},30000);

test('reassignment emits ready Tasks by key and stop notices by Attempt start sequence',async()=>{
 const f=await fixture();await f.call('ensure_worker',{name:'other',scope:'Other'});
 const other=(await f.state()).team.current!.workers.other.conversationId;
 const tasks=[def('a'),{...def('z'),assignee:'other'}];
 const g=await f.call('task_graph_apply',{operation_id:'g',tasks});
 await f.call('task_update',{task_id:'z',operation_id:'z-first',expected_version:g.tasks_by_key.z.version,transition:'claim'},'worker',other);
 await f.call('task_update',{task_id:'a',operation_id:'a-second',expected_version:g.tasks_by_key.a.version,transition:'claim'},'worker',f.worker);
 const before=new Set(Object.keys((await f.state()).outbox.obligations));
 await f.call('task_graph_apply',{operation_id:'swap',expected_graph_version:g.graph_version,tasks:tasks.map(t=>({...t,assignee:t.assignee==='builder'?'other':'builder'}))});
 const created=Object.values((await f.state()).outbox.obligations).filter(o=>!before.has(o.key));
 expect(created.map(o=>[o.kind,o.taskId,o.recipient])).toEqual([
  ['task','a','other'],['task','z','builder'],['stop','z','other'],['stop','a','builder'],
 ]);
},30000);

test('operation receipts detect corruption and a rejected command changes no graph facts',async()=>{
 const f=await fixture(),g=await f.call('task_graph_apply',{operation_id:'g',tasks:[def('a')]}),state=await f.state();
 await f.h.commit(async tx=>{const doc=await tx.doc(OperationDoc,taskAddress(state.team.current!.epoch,'g'),null);const result=JSON.parse(doc.result as string);result.tasks[0].title='Corrupt';doc.result=JSON.stringify(result);},context);
 const result=await f.call('task_graph_apply',{operation_id:'g',tasks:[def('a')]});
 expect(result.kind).toBe('refused');expect(result.reason).toBe('invalid_graph');
 expect(result.message).toBe('Graph authority snapshot has a graph receipt that differs from committed state.');
 expect((await f.call('task_read',{task_ids:['a']})).outcomes[0].task.version).toBe(g.tasks_by_key.a.version);
},30000);

test('accepted prototype-like Task and Worker names retain indexed state',async()=>{
 const f=await fixture();await f.call('ensure_worker',{name:'__proto__',scope:'Synthetic'});
 const worker=(await f.state()).team.current!.workers['__proto__'].conversationId;
 const g=await f.call('task_graph_apply',{operation_id:'g',tasks:[{...def('__proto__'),assignee:'__proto__'}]});
 const claim=await f.call('task_update',{task_id:'__proto__',operation_id:'claim',expected_version:g.tasks_by_key['__proto__'].version,transition:'claim'},'worker',worker);
 const block=await f.call('task_update',{task_id:'__proto__',operation_id:'block',expected_version:claim.task.version,transition:'block',evidence:'Waiting'},'worker',worker);
 expect(block.kind).toBe('updated');
 const stop=await f.call('worker_stop',{worker:'__proto__'});expect(stop.guarding_task_ids).toEqual(['__proto__']);
 const head=await f.h.commit(async tx=>plain((await openAuthority(tx)).head),context);expect(head.attention['__proto__']).toBe('blocked');
},30000);

test('a context-only transition writes one Task base and the head omits topology and projections',async()=>{
 const f=await fixture(),g=await f.call('task_graph_apply',{operation_id:'g',tasks:[def('a'),def('b',['a']),def('c',['b'])]});
 const changed:any[]=[];const off=f.h.subscribeCommits(p=>{changed.push(...p.changes.filter(c=>c.type==='document'));});
 await f.call('task_update',{task_id:'a',operation_id:'context',expected_version:g.tasks_by_key.a.version,current_context:'Current context'},'worker',f.worker);off();
 expect(changed.filter(c=>c.record.kind==='ptb.task')).toHaveLength(1);
 const headChange=changed.find(c=>c.record.kind==='ptb.head');const head=JSON.parse(headChange.value.value);
 expect(head.taskCount).toBe(3);expect(head.graph.topology).toBeUndefined();expect(head.graph.projections).toBeUndefined();
 const docWrites=f.storage.writes.filter(w=>w.type==='document.change');expect(docWrites.every(w=>w.type==='document.change'&&w.content.kind==='base')).toBe(true);
},30000);

for(const migrated of [true,false])test(`rc.7 access fails before writes on a ${migrated?'migrated':'fresh'} SQLite store`,async()=>{
 const dir=mkdtempSync(join(tmpdir(),'ptb-version-fence-')),file=join(dir,'team.sqlite');
 const options={models:createModels(),registry:createRegistry(),now:()=>1000,setTimer:()=>()=>{}};
 let writes:StorageWrite[]=[],calls=1000000;
 const storage=async()=>{
  const db=await openNodeSqliteStorage(file),commit=db.commit.bind(db);
  db.commit=async(w,c)=>{writes.push(...w);return commit(w,c);};return db;
 };
 let h=await Harness.open(await storage(),options,context),root=await h.root(context);
 try{
  const config={modelRoles:[{name:'builder',use:'Build',model:{provider:'fake',modelId:'fake'}}],defaultModelRole:'builder',
   createWorker:async(tx:any)=>{const c=await tx.createConversation({ownership:{kind:'ownerless'}});return {conversationId:c.id,anchorId:9000000 as TaskId};},stopWorker:async()=>true};
  const old=legacyTools(config),current=createAuthorityTools(config),seed=migrated?old:current;
  const api=(conversationId=root.id):CoreApi=>({taskId:++calls as TaskId,conversationId,commit:root.commit.bind(root),now:()=>1000});
  await seed.execute('team_create','lead',{name:'synthetic',purpose:'Fence checks'},api(),context);
  await seed.execute('ensure_worker','lead',{name:'builder',scope:'Build'},api(),context);
  const graph=await seed.execute('task_graph_apply','lead',{operation_id:'plan',tasks:[def('a')]},api(),context) as any;
  const before=await h.commit(async tx=>plain(await (migrated?legacyOpen:open)(tx)),context);
  writes=[];await h.commit(async tx=>{await openAuthority(tx);},context);
  if(migrated)expect(writes.filter(w=>(w.type==='document.create'||w.type==='document.change')&&w.content.version===2&&w.content.kind==='base')).toHaveLength(6);
  const worker=before.team.current!.workers.builder.conversationId;
  const lead={team_create:{name:'other',purpose:'Must fail'},team_sync:{view:'snapshot'},ensure_worker:{name:'intruder',scope:'Must fail'},
   task_graph_apply:{operation_id:'forbidden-plan',expected_graph_version:graph.graph_version,tasks:[def('a')]},task_read:{task_ids:['a']},
   task_update:{task_id:'a',operation_id:'forbidden-update',expected_version:graph.tasks_by_key.a.version,current_context:'Must fail'},
   worker_stop:{worker:'builder'},team_shutdown:{},alert_send:{to:'builder',kind:'attention',text:'Must fail'}};
  const workerArgs={...lead,task_read:{task_id:'a'},alert_send:{kind:'attention',text:'Must fail'}};
  const fences=[GraphDoc,JournalDoc,OutboxDoc,ReceiptDoc,ObservationDoc];
  for(const cold of [false,true]){
   if(cold){await h.close(context);h=await Harness.open(await storage(),options,context);root=await h.root(context);}
   writes=[];
   for(const role of ['lead','worker'] as const)for(const name of Object.keys(legacyContracts()[role])){
    const args=(role==='lead'?lead:workerArgs)[name as keyof typeof lead];
    await expect(old.execute(name as any,role,args,api(role==='lead'?root.id:worker),context),`${role} ${name}`).rejects.toThrow(/has newer version 2 than 1/);
    expect(writes,`${role} ${name} wrote storage`).toEqual([]);
   }
   for(const token of [legacyState.TeamDoc,legacyState.GraphDoc,legacyState.JournalDoc,legacyState.OutboxDoc,legacyState.ReceiptDoc,LegacyObservationDoc]){
    await expect(h.commit(tx=>tx.doc(token),context)).rejects.toThrow(/has newer version 2 than 1/);
    await expect(h.snapshot(token,context)).rejects.toThrow(/has newer version 2 than 1/);
   }
   expect(writes).toEqual([]);
   expect(await h.commit(async tx=>plain(await open(tx)),context)).toEqual(before);
   for(const token of fences)expect(await h.snapshot(token,context)).toEqual({fence:'ptb-authority-v2'});
   expect(await h.snapshot(TeamDoc,context)).toEqual(before.team);
  }
  // A current command must not consult a retained fence as authority.
  const guardedApi=()=>{const guarded=api();guarded.commit=(fn,c)=>root.commit(tx=>fn(new Proxy(tx,{get(target,key){
   if(key==='doc')return (token:any,...args:any[])=>{if(fences.some(f=>f.definition.kind===token.definition.kind))throw new Error('Command read a legacy fence');return (target.doc as any)(token,...args);};
   const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
  }})),c);return guarded;};
  const result=await current.execute('task_read','lead',{task_ids:['a']},guardedApi(),context) as any;
  expect(result.outcomes[0].task.version).toBe(graph.tasks_by_key.a.version);
  const updated=await current.execute('task_update','lead',{task_id:'a',operation_id:'after-fence',expected_version:graph.tasks_by_key.a.version,current_context:'New tools continue'},guardedApi(),context);
  expect(updated.kind).toBe('updated');
  expect((await current.execute('team_sync','lead',{view:'snapshot'},guardedApi(),context)).kind).toBe('snapshot');
  expect((await current.execute('ensure_worker','lead',{name:'builder',scope:'Build'},guardedApi(),context)).effect).toBe('reused');
  expect((await current.execute('alert_send','lead',{to:'builder',kind:'attention',text:'Current tools'},guardedApi(),context)).kind).toBe('alert_sent');
  for(const token of fences)expect(await h.snapshot(token,context)).toEqual({fence:'ptb-authority-v2'});
 }finally{await h.close(context);rmSync(dir,{recursive:true,force:true});}
},30000);
