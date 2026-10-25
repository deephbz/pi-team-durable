import { expect,test } from 'vitest';
import { InboxDoc,LiveDoc } from '@earendil-works/pi-durable';
import { fauxAssistantMessage as answer } from '@earendil-works/pi-ai/providers/faux';
import { open } from '../../src/authority/state.ts';
import { closing } from '../../src/authority/runtime.ts';
import { contracts } from '../../src/authority/contracts.ts';
import { runtimeFixture,context } from './runtime-fixture.ts';

const deferred=()=>{let resolve!:()=>void;const promise=new Promise<void>(r=>resolve=r);return {promise,resolve};};
test('only the lead Alert schema accepts optional interrupt',()=>{
 expect((contracts().lead.alert_send as any).properties.interrupt).toMatchObject({type:'boolean'});
 expect((contracts().lead.alert_send as any).required).not.toContain('interrupt');
 expect((contracts().worker.alert_send as any).properties.interrupt).toBeUndefined();
});

for(const interrupt of [true,false])test(`busy Worker Alert interrupt=${interrupt} preserves owner, Task and Alert identities`,async()=>{
 let before:unknown,portCalls=0;
 const f=await runtimeFixture([],port=>async(api,target,c)=>{
  portCalls++;const result=await port(api,target,c);
  expect((await f.harness.snapshot(InboxDoc,target.conversationId,c))!.items).toEqual(before);
  return result;
 });
 try{
  await f.runtime.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},f.api(),context);
  const worker=(await f.state()).team.current!.workers.builder;
  const target=(await f.harness.conversation(worker.conversationId,context))!;
  const started=deferred(),release=deferred(),admitted=deferred();let aborted=false;
  const requests:string[]=[];
  const response=(messages:unknown)=>{requests.push(JSON.stringify(messages));return answer('Input handled');};
  f.faux.setResponses([async(_messages,options)=>{
   options?.signal?.addEventListener('abort',()=>{aborted=true;release.resolve();},{once:true});
   started.resolve();await release.promise;
   return aborted?answer([],{stopReason:'aborted'}):answer('Original finished');
  },response,response,response,response]);
  const original=await target.submit({type:'input',content:'Original busy work'},context);
  await started.promise;
  const queued=await target.submit({type:'input',content:'Older queued owner',whenBusy:'followUp',requestId:'owner'},context);
  await f.runtime.core.execute('task_graph_apply','lead',{operation_id:'queue-task',tasks:[{key:'task',title:'Queued Task',goal:'Work',assignee:'builder'}]},f.api(),context);
  await f.runtime.core.execute('alert_send','lead',{to:'builder',kind:'attention',text:'Older Alert'},f.api(),context);
  f.harness.resume();
  for(let i=0;i<100;i++){
   before=(await f.harness.snapshot(InboxDoc,worker.conversationId,context))!.items;
   if((before as any[]).length===3)break;
   await new Promise<void>(resolve=>setImmediate(resolve));
  }
  expect(before).toHaveLength(3);
  const prior=(await f.state()).outbox.obligations;
  const oldKeys=new Set(Object.keys(prior));
  f.storage.fault=writes=>{if(writes.some(w=>w.type==='submission'&&w.value.requestId?.startsWith('ptb:')&&!oldKeys.has(w.value.requestId.slice(4))))admitted.resolve();return false;};
  const result=await f.runtime.core.execute('alert_send','lead',{to:'builder',kind:'attention',text:'Urgent correction',...(interrupt?{interrupt:true}:{})},f.api(),context);
  expect(result.kind).toBe('alert_sent');
  f.harness.resume();await admitted.promise;
  expect(aborted).toBe(interrupt);expect(portCalls).toBe(interrupt?1:0);
  if(!interrupt)release.resolve();
  const obligations=Object.values((await f.state()).outbox.obligations);
  await Promise.all(obligations.map(o=>f.harness.waitForTask(o.deliveryTaskId!,context)));
  expect((await original.status(context)).status).toBe(interrupt?'unanswered':'done');
  expect((await queued.wait(context)).status).toBe('done');
  expect(requests.join('\n')).toContain('Urgent correction');
  expect(requests[0]).toContain('Older Alert');
  if(interrupt)expect(requests[0]).not.toContain('Urgent correction');
  for(const item of before as any[]){
   const record=await (await f.harness.submission(item.id,context))!.status(context);
   expect(record.status).toBe('done');
   const entry=(await target.entries({order:'ascending'},100,undefined,context)).items.find(e=>e.id===record.entry)!;
   expect(entry.kind).toBe(item.entry?.kind??'pi.user');
   if(item.entry)expect(entry.data).toEqual(item.entry.data);
  }
  const state=await f.state();
  expect(Object.values(state.outbox.obligations).every(o=>o.outcome==='answered')).toBe(true);
  expect(state.team.current!.workers.builder.state).toBe('active');
  expect((await f.harness.getTask(worker.anchorId,context))!.state.status).not.toBe('terminal');
 }finally{await f.close();}
},10000);

for(const change of ['epoch','generation','conversation','close'] as const)test(`interrupt rechecks recipient after ${change} changes`,async()=>{
 const entered=deferred(),continuePort=deferred();let outcome:string|undefined;
 const f=await runtimeFixture([],port=>async(api,target,c)=>{entered.resolve();await continuePort.promise;outcome=await port(api,target,c);return outcome as Awaited<ReturnType<typeof port>>;});try{
  await f.runtime.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},f.api(),context);
  const worker=(await f.state()).team.current!.workers.builder;
  await f.runtime.core.execute('alert_send','lead',{to:'builder',kind:'attention',text:'Stale recipient',interrupt:true},f.api(),context);
  const o=Object.values((await f.state()).outbox.obligations)[0];
  f.harness.resume();await entered.promise;
  if(change==='close')await closing(f.harness,String(f.root.id),context);
  else await f.harness.commit(async tx=>{
   const s=await open(tx),w=s.team.current!.workers.builder;
   if(change==='epoch')s.team.current!.epoch='replacement';
   else if(change==='generation')w.generation++;
   else w.conversationId=f.root.id;
  },context);
  continuePort.resolve();await f.harness.waitForTask(o.deliveryTaskId!,context);
  expect(outcome).toBe('retired');
  expect((await f.storage.scanSubmissions({conversationId:worker.conversationId},100,undefined,context)).items).toHaveLength(0);
 }finally{continuePort.resolve();await f.close();}
});

test('Close can cancel an interrupt wait without restoring withdrawn inputs',async()=>{
 const entered=deferred(),started=deferred(),release=deferred();
 const f=await runtimeFixture([],port=>async(api,target,c)=>{entered.resolve();return port(api,target,c);});try{
  await f.runtime.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},f.api(),context);
  const worker=(await f.state()).team.current!.workers.builder,target=(await f.harness.conversation(worker.conversationId,context))!;
  f.faux.setResponses([async()=>{started.resolve();await release.promise;return answer('Late provider response');}]);
  await target.submit({type:'input',content:'Uncooperative provider'},context);await started.promise;
  const queued=await target.submit({type:'input',content:'Owner queued',whenBusy:'followUp'},context);
  await f.runtime.core.execute('alert_send','lead',{to:'builder',kind:'attention',text:'Do not resurrect',interrupt:true},f.api(),context);f.harness.resume();await entered.promise;
  const o=Object.values((await f.state()).outbox.obligations)[0];
  const plans=await closing(f.harness,String(f.root.id),context);
  expect(plans.flatMap(p=>p.tasks??[])).toContain(String(o.deliveryTaskId));
  // Native Stop owns this withdrawal. The interrupted delivery must never re-admit it.
  await queued.abort(context);
  await f.harness.abortTask(o.deliveryTaskId!,context);
  expect((await f.harness.waitForTask(o.deliveryTaskId!,context)).state.outcome.status).toBe('aborted');
  expect((await queued.status(context)).status).toBe('unanswered');
  release.resolve();await target.waitForIdle(context);
  expect((await f.storage.scanSubmissions({conversationId:worker.conversationId},100,undefined,context)).items.some(r=>r.requestId===`ptb:${o.key}`)).toBe(false);
 }finally{release.resolve();await f.close();}
},5000);

test('changed run never interrupts its replacement',async()=>{
 const entered=deferred(),continuePort=deferred(),firstStarted=deferred(),finishFirst=deferred(),secondStarted=deferred(),finishSecond=deferred(),admitted=deferred();let replacementAborted=false;
 const f=await runtimeFixture([],port=>async(api,target,c)=>{entered.resolve();await continuePort.promise;return port(api,target,c);});try{
  await f.runtime.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},f.api(),context);
  const worker=(await f.state()).team.current!.workers.builder,target=(await f.harness.conversation(worker.conversationId,context))!;
  f.faux.setResponses([async()=>{firstStarted.resolve();await finishFirst.promise;return answer('First ended');},async(_m,options)=>{
   options?.signal?.addEventListener('abort',()=>{replacementAborted=true;finishSecond.resolve();},{once:true});secondStarted.resolve();await finishSecond.promise;return answer('Replacement ended');
  },answer('Alert handled')]);
  const first=await target.submit({type:'input',content:'First run'},context);await firstStarted.promise;
  const firstRun=(await f.harness.snapshot(LiveDoc,worker.conversationId,context))!.run!.taskId;
  await f.runtime.core.execute('alert_send','lead',{to:'builder',kind:'attention',text:'Bound to first',interrupt:true},f.api(),context);f.harness.resume();await entered.promise;
  finishFirst.resolve();await first.wait(context);
  await target.submit({type:'input',content:'Replacement run'},context);await secondStarted.promise;
  f.storage.fault=writes=>{if(writes.some(w=>w.type==='submission'&&w.value.requestId?.startsWith('ptb:')))admitted.resolve();return false;};
  continuePort.resolve();await admitted.promise;expect(replacementAborted).toBe(false);
  const o=Object.values((await f.state()).outbox.obligations)[0];expect(o.interruptRun).toBe(firstRun);expect(o.interruption).toBe('ended');
  finishSecond.resolve();await f.harness.waitForTask(o.deliveryTaskId!,context);
 }finally{finishFirst.resolve();finishSecond.resolve();continuePort.resolve();await f.close();}
});

test('failed interrupt admits one normal Alert with an explicit unproved notice',async()=>{
 const f=await runtimeFixture([],()=>async()=>{throw new Error('Synthetic interrupt failure');});try{
  await f.runtime.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},f.api(),context);
  f.faux.setResponses([answer('Received')]);
  const api=f.api(),args={to:'builder',kind:'attention',text:'Check once',interrupt:true};
  expect((await f.runtime.core.execute('alert_send','lead',args,api,context)).kind).toBe('alert_sent');
  expect((await f.runtime.core.execute('alert_send','lead',args,api,context)).kind).toBe('alert_sent');
  const o=Object.values((await f.state()).outbox.obligations)[0];
  await f.harness.waitForTask(o.deliveryTaskId!,context);
  const worker=(await f.state()).team.current!.workers.builder;
  const rows=await f.storage.scanSubmissions({conversationId:worker.conversationId},100,undefined,context);
  expect(rows.items).toHaveLength(1);expect(rows.items[0].status).toBe('done');
  const entries=await (await f.harness.conversation(worker.conversationId,context))!.entries({},100,undefined,context);
  expect(JSON.stringify(entries.items)).toContain('Interruption was not proved');
  expect((await f.state()).outbox.obligations[o.key].interruption).toBe('failed');
 }finally{await f.close();}
});
