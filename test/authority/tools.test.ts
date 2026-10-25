import { afterEach, expect, test } from 'vitest';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { createRegistry, Harness, MemoryStorage, type TaskId } from '@earendil-works/pi-durable';
import { createAuthorityTools, type CoreApi } from '../../src/authority/tools.ts';
import { open, plain } from '../../src/authority/state.ts';
const closers: (()=>Promise<void>)[]=[];
afterEach(async()=>{for(const close of closers.splice(0))await close();});
async function fixture(stopEffect?:()=>Promise<boolean>){
 const harness=await Harness.open(new MemoryStorage(),{models:createModels(),registry:createRegistry(),now:()=>1000,setTimer:()=>()=>{}},context);
 closers.push(()=>harness.close(context));
 const root=await harness.root(context);
 let n=0, launches=0, stops=0;
 const core=createAuthorityTools({modelRoles:[{name:'builder',use:'Build',model:{provider:'fake',modelId:'fake'}}],defaultModelRole:'builder',createWorker:async tx=>{launches++;const c=await tx.createConversation({ownership:{kind:'ownerless'}});return {conversationId:c.id,anchorId:(100+launches) as TaskId};},stopWorker:async()=>{stops++;return stopEffect?stopEffect():true;}});
 const api=(id:number,conversationId=root.id):CoreApi=>({taskId:id as TaskId,conversationId,commit:root.commit.bind(root),now:()=>1000});
 async function call(name:string,input:unknown,role:'lead'|'worker'='lead',conversationId=root.id){
  const tool=(role==='lead'?core.lead:core.worker).find(t=>t.name===name)!;
  const bound=api(++n,conversationId);
  const invoke=()=>tool.execute(input,bound as any,context);
  const first=await invoke();expect(await invoke()).toEqual(first);
  return JSON.parse((first.output![0] as any).text);
 }
 return {core,call,api,root,state:()=>root.commit(async tx=>plain(await open(tx)),context),counts:()=>({launches,stops})};
}
test('nine lead and three Worker tools return rc.7 results and replay without repeated effects',async()=>{
 const f=await fixture();expect(f.core.lead).toHaveLength(9);expect(f.core.worker).toHaveLength(3);
 expect((await f.call('team_create',{name:'demo',purpose:'Test'})).kind).toBe('team_created');
 expect((await f.call('ensure_worker',{name:'builder',scope:'Build'})).kind).toBe('worker_ensured');
 const worker=(await f.state()).team.current!.workers.builder.conversationId;
 const graph=await f.call('task_graph_apply',{operation_id:'graph-1',tasks:[{key:'a',title:'A',goal:'Build A',assignee:'builder'}]});
 expect(graph.kind).toBe('task_graph_applied');
 const read=await f.call('task_read',{task_ids:['a']});expect(read.outcomes[0].kind).toBe('found');
 const card=await f.call('task_read',{task_id:'a'},'worker',worker);expect(card.kind).toBe('found');
 const claim=await f.call('task_update',{task_id:'a',operation_id:'claim-1',expected_version:card.task.version,transition:'claim'},'worker',worker);expect(claim.kind).toBe('updated');
 const preserved=await f.call('task_graph_apply',{operation_id:'graph-preserved',expected_graph_version:graph.graph_version,tasks:[{key:'a',title:'A',goal:'Build A',assignee:'builder'}]});
 expect(preserved.tasks_by_key.a.state).toEqual({kind:'in_progress',attempt_id:claim.task.id+'@1'});
 const cancel=await f.call('task_update',{task_id:'a',operation_id:'cancel-1',expected_version:claim.task.version,transition:'cancel',evidence:'Test cleanup'});expect(cancel.kind).toBe('updated');
 expect((await f.call('alert_send',{to:'builder',kind:'attention',text:'Check'})).kind).toBe('alert_sent');
 expect((await f.call('alert_send',{kind:'attention',text:'Checked'},'worker',worker)).kind).toBe('alert_sent');
 expect((await f.call('team_sync',{view:'snapshot'})).kind).toBe('snapshot');
 expect((await f.call('worker_stop',{worker:'builder'})).kind).toBe('worker_stopped');
 expect((await f.call('team_shutdown',{})).kind).toBe('team_shutdown');
 expect(f.counts()).toEqual({launches:1,stops:1});
 const before=await f.state(),old=before.team.current!.epoch;
 await f.call('team_create',{name:'next',purpose:'Next epoch'});
 expect((await f.state()).graph.history![old]).toEqual(before.graph.snapshot);
});

test('refusal receipts survive later state changes; changed invocation input is rejected',async()=>{
 const f=await fixture(), api=f.api(101);
 const args={worker:'builder'};
 const refused=await f.core.execute('worker_stop','lead',args,api,context);
 expect(refused.kind).toBe('unavailable');
 await f.call('team_create',{name:'demo',purpose:'Test'});
 await f.call('ensure_worker',{name:'builder',scope:'Build'});
 expect(await f.core.execute('worker_stop','lead',args,api,context)).toEqual(refused);
 await expect(f.core.execute('worker_stop','lead',{worker:'other'},api,context)).rejects.toThrow('changed input');
 expect(f.counts().stops).toBe(0);
});

test('missing established graph fails closed without resetting authority',async()=>{
 const f=await fixture();await f.call('team_create',{name:'demo',purpose:'Test'});
 await f.root.commit(async tx=>{delete (await open(tx)).graph.snapshot;},context);
 const result=await f.call('task_read',{task_ids:['a']});expect(result.kind).toBe('unavailable');
 expect((await f.state()).graph.snapshot).toBeUndefined();
});

test('stop intent commits before external effect and survives interruption',async()=>{
 let crash=true;
 const f=await fixture(async()=>{if(crash)throw new Error('simulated process interruption');return true;});
 await f.call('team_create',{name:'demo',purpose:'Test'});
 await f.call('ensure_worker',{name:'builder',scope:'Build'});
 const api=f.api(101),args={worker:'builder'};
 await expect(f.core.execute('worker_stop','lead',args,api,context)).rejects.toThrow('simulated');
 const state=await f.state();expect(state.team.current!.workers.builder.state).toBe('stopping');
 expect(Object.values(state.team.stops)[0].confirmed).toBe(false);
 crash=false;
 const result=await f.core.execute('worker_stop','lead',args,api,context);expect(result.kind).toBe('worker_stopped');
 expect(await f.core.execute('worker_stop','lead',args,api,context)).toEqual(result);
 expect(f.counts().stops).toBe(2);
});

test('replayed graph result stays exact after graph revision and retires stale obligations',async()=>{
 const f=await fixture();await f.call('team_create',{name:'demo',purpose:'Test'});await f.call('ensure_worker',{name:'builder',scope:'Build'});
 const api=f.api(101),args={operation_id:'first',tasks:[{key:'a',title:'A',goal:'A',assignee:'builder'}]};
 const first=await f.core.execute('task_graph_apply','lead',args,api,context);
 await f.call('task_graph_apply',{operation_id:'second',expected_graph_version:first.graph_version,tasks:[{...args.tasks[0],goal:'Revised A'}]});
 expect(await f.core.execute('task_graph_apply','lead',args,api,context)).toEqual(first);
 const obligations=Object.values((await f.state()).outbox.obligations);
 expect(obligations.filter(o=>o.state==='retired')).toHaveLength(1);
 expect(obligations.filter(o=>o.state==='pending')).toHaveLength(1);
});
