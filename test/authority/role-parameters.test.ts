import { expect,test } from 'vitest';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxAssistantMessage as answer } from '@earendil-works/pi-ai/providers/faux';
import { Harness,MemoryStorage,createRegistry,type TaskId,type ConversationId } from '@earendil-works/pi-durable';
import { createAuthorityTools,type CoreApi } from '../../src/authority/tools.ts';
import { createAuthorityTools as legacyTools } from '../oracle/authority/tools.ts';
import { contracts,TOOL_NAMES,type Role,type ToolName } from '../../src/authority/contracts.ts';
import { open,plain } from '../../src/authority/state.ts';
import { runtimeFixture,context } from './runtime-fixture.ts';

const deferred=()=>{let resolve!:()=>void;const promise=new Promise<void>(r=>resolve=r);return {promise,resolve};};
for(const role of ['lead','worker'] as Role[])for(const name of TOOL_NAMES.filter(n=>contracts()[role][n]))test(`${role} ${name} ignores undeclared arguments in effects, results and replay`,async()=>{
 async function run(extra:boolean){
  const h=await Harness.open(new MemoryStorage(),{models:createModels(),registry:createRegistry(),now:()=>1000,setTimer:()=>()=>{},onWake:()=>{}},context),root=await h.root(context);
  let serial=1000000,worker:ConversationId=root.id;
  const core=createAuthorityTools({modelRoles:[{name:'builder',use:'Build',model:{provider:'fake',modelId:'test'}}],defaultModelRole:'builder',
   createWorker:async tx=>({conversationId:(await tx.createConversation({ownership:{kind:'ownerless'}})).id,anchorId:9000000 as TaskId}),stopWorker:async()=>true});
  const api=(conversationId=root.id):CoreApi=>({taskId:++serial as TaskId,conversationId,commit:root.commit.bind(root),now:()=>1000});
  try{
   let version:string|undefined;
   if(name!=='team_create'){
    await core.execute('team_create','lead',{name:'parameter-demo',purpose:'Role boundary'},api(),context);
    await core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},api(),context);
    const graph=await core.execute('task_graph_apply','lead',{operation_id:'seed',tasks:[{key:'sample',title:'Sample',goal:'Works',assignee:'builder'}]},api(),context) as any;
    version=graph.tasks_by_key.sample?.version;
    if(name==='worker_stop'||name==='team_shutdown')await core.execute('task_update','lead',{task_id:'sample',operation_id:'cancel',expected_version:version,transition:'cancel'},api(),context);
    worker=await root.commit(async tx=>(await open(tx)).team.current!.workers.builder.conversationId,context);
   }
   const args:Record<ToolName,Record<string,unknown>>={
    team_create:{name:'parameter-demo',purpose:'Role boundary'},team_sync:{view:'snapshot'},ensure_worker:{name:'other',scope:'Build'},
    task_graph_apply:{operation_id:'apply',tasks:[{key:'sample',title:'Sample revised',goal:'Works',assignee:'builder'}]},task_read:role==='lead'?{task_ids:['sample']}:{task_id:'sample'},
    task_update:{task_id:'sample',operation_id:'update',expected_version:version,...(role==='worker'?{transition:'claim'}:{current_context:'Reviewed'})},
    worker_stop:{worker:'builder'},team_shutdown:{},alert_send:{...(role==='lead'?{to:'builder'}:{}),kind:'attention',text:'Declared text'},
   };
   const schema=core.contract[role][name] as {properties?:Record<string,unknown>};
   const foreign={interrupt:true,to:'*',worker:'intruder',transition:'cancel',name:'intruder',scope:'Changed',purpose:'Changed',view:'updates',model_role:'intruder',current_context:'Changed',evidence:'Changed',extra:'Must be inert'};
   const input=extra?{...args[name],...Object.fromEntries(Object.entries(foreign).filter(([key])=>!Object.hasOwn(schema.properties??{},key)))}:args[name];
   if(extra&&name==='task_graph_apply')input.tasks=(input.tasks as Record<string,unknown>[]).map(task=>({...task,extra:'Nested fields must be inert'}));
   const call=api(role==='lead'?root.id:worker),result=await core.execute(name,role,input,call,context);
   if(name!=='task_read')expect(await core.execute(name,role,args[name],call,context)).toEqual(result);
   return {result,state:await root.commit(async tx=>plain(await open(tx)),context)};
  }finally{await h.close(context);}
 }
 expect(await run(true)).toEqual(await run(false));
});

test('a P receipt with undeclared fields replays its accepted result after upgrade',async()=>{
 const h=await Harness.open(new MemoryStorage(),{models:createModels(),registry:createRegistry(),now:()=>1000,setTimer:()=>()=>{},onWake:()=>{}},context),root=await h.root(context);
 try{
  let worker=root.id;
  const options={modelRoles:[{name:'builder',use:'Build',model:{provider:'fake',modelId:'test'}}],defaultModelRole:'builder',
   createWorker:async(tx:import('@earendil-works/pi-durable').Tx)=>({conversationId:worker=(await tx.createConversation({ownership:{kind:'ownerless'}})).id,anchorId:9000000 as TaskId})};
  const legacy=legacyTools(options),current=createAuthorityTools(options);
  const api:CoreApi={taskId:1000000 as TaskId,conversationId:root.id,commit:root.commit.bind(root),now:()=>1000};
  await legacy.execute('team_create','lead',{name:'legacy-demo',purpose:'Accepted work'},api,context);
  await legacy.execute('ensure_worker','lead',{name:'builder',scope:'Build'},{...api,taskId:1000001 as TaskId},context);
  const call={...api,taskId:1000002 as TaskId,conversationId:worker};
  const input={kind:'attention',text:'Accepted Alert',extra:'Old raw fingerprint'};
  const result=await legacy.execute('alert_send','worker',input,call,context);
  expect(await current.execute('alert_send','worker',input,call,context)).toEqual(result);
  expect(Object.keys((await root.commit(async tx=>plain(await open(tx)),context)).outbox.alerts)).toHaveLength(1);
  await expect(current.execute('alert_send','worker',{...input,text:'Changed'},call,context)).rejects.toThrow('Durable tool identity reused with changed input');
 }finally{await h.close(context);}
});

test('a Worker Alert with empty text or task_id fails Pi\'s schema; whitespace text stays refused',async()=>{
 const f=await runtimeFixture();
 try{
  await f.runtime.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},f.api(),context);
  const worker=(await f.state()).team.current!.workers.builder,call=()=>({...f.api(),conversationId:worker.conversationId});
  for(const input of [{kind:'attention',text:''},{kind:'attention',text:'Linked',task_id:''}])
   await expect(f.runtime.core.execute('alert_send','worker',input,call(),context)).rejects.toThrow('Invalid worker alert_send parameters');
  expect(await f.runtime.core.execute('alert_send','worker',{kind:'attention',text:'   '},call(),context)).toMatchObject({kind:'refused',reason:'invalid_fanout'});
  expect((await f.state()).outbox.alerts).toEqual({});
 }finally{await f.close();}
});

test('a Worker extra interrupt cannot abort the busy lead or mark its Alert as interrupting',async()=>{
 let calls=0;const f=await runtimeFixture([],port=>async(...args)=>{calls++;return port(...args);});
 const started=deferred(),release=deferred(),admitted=deferred();let aborted=false;
 try{
  await f.runtime.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},f.api(),context);
  const worker=(await f.state()).team.current!.workers.builder;
  f.faux.setResponses([async(_messages,options)=>{options?.signal?.addEventListener('abort',()=>{aborted=true;release.resolve();},{once:true});started.resolve();await release.promise;return answer('Owner completed');},...Array.from({length:5},()=>answer('Received'))]);
  const owner=await f.root.submit({type:'input',content:'Owner work on lead'},context);await started.promise;
  f.storage.fault=writes=>{if(writes.some(w=>w.type==='submission'&&w.value.requestId?.startsWith('ptb:')))admitted.resolve();return false;};
  const result=await f.runtime.core.execute('alert_send','worker',{kind:'attention',text:'Worker message',interrupt:true},{...f.api(),conversationId:worker.conversationId},context);
  expect(result).toEqual({kind:'alert_sent',accepted_recipients:['team-lead'],failed_recipients:[]});
  f.harness.resume();await admitted.promise;
  const state=await f.state(),o=Object.values(state.outbox.obligations)[0];
  expect(Object.values(state.outbox.alerts)[0].interrupt).toBeUndefined();expect(o.interrupt).toBeUndefined();expect(o.interruptRun).toBeUndefined();
  expect(calls).toBe(0);expect(aborted).toBe(false);
  release.resolve();expect((await owner.wait(context)).status).toBe('done');await f.harness.waitForTask(o.deliveryTaskId!,context);
 }finally{release.resolve();await f.close();}
},10000);
