import { expect,test } from 'vitest';
import { LiveDoc,InboxDoc,type StorageWrite } from '@earendil-works/pi-durable';
import { fauxAssistantMessage as answer } from '@earendil-works/pi-ai/providers/faux';
import { runtimeFixture,context } from './runtime-fixture.ts';
const deferred=()=>{let resolve!:()=>void;const promise=new Promise<void>(r=>resolve=r);return {promise,resolve};};

for(const cut of ['intent','abort','idle','checkpoint','admission','settle'] as const)test(`interrupt recovery after committed ${cut} preserves the queue and one admission`,async()=>{
 const f=await runtimeFixture();try{
  await f.runtime.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},f.api(),context);
  const worker=(await f.state()).team.current!.workers.builder,target=(await f.harness.conversation(worker.conversationId,context))!;
  const started=deferred(),cancelled=deferred();
  f.faux.setResponses([async(_messages,options)=>{
   options?.signal?.addEventListener('abort',cancelled.resolve,{once:true});started.resolve();await cancelled.promise;return answer([],{stopReason:'aborted'});
  }]);
  await target.submit({type:'input',content:'Busy work'},context);await started.promise;
  const run=(await f.harness.snapshot(LiveDoc,worker.conversationId,context))!.run!.taskId;
  const queue=[
   await target.submit({type:'input',content:'Queued owner',whenBusy:'followUp',requestId:'owner'},context),
   await target.submit({type:'input',content:'Queued Task',whenBusy:'followUp',requestId:'task',entry:{kind:'team.notice',data:{source:'team',delivery:'task',deliveryId:'task'}}},context),
   await target.submit({type:'input',content:'Older Alert',whenBusy:'steer',requestId:'older-alert',entry:{kind:'team.notice',data:{source:'team',delivery:'alert',deliveryId:'older-alert'}}},context),
  ];
  const before=(await f.harness.snapshot(InboxDoc,worker.conversationId,context))!.items;
  const fault=(w:StorageWrite)=>{
   if(cut==='admission')return w.type==='submission'&&!!w.value.requestId?.startsWith('ptb:');
   if(w.type!=='task')return false;
   if(cut==='abort')return w.value.id===run&&w.value.abortRequested;
   if(cut==='idle')return w.value.id===run&&w.value.state.status==='terminal';
   if(w.value.kind!=='ptb.deliver'||!('checkpoint' in w.value.state))return false;
   return (w.value.state.checkpoint as any).phase===({intent:'interrupt',checkpoint:'submit',settle:'settle'} as any)[cut];
  };
  let hit=false;f.storage.fault=writes=>writes.some(w=>{if(fault(w)){hit=true;return true;}return false;});
  await f.runtime.core.execute('alert_send','lead',{to:'builder',kind:'attention',text:'Interrupt Alert',interrupt:true},f.api(),context).catch(()=>undefined);
  if(!hit)f.harness.resume();await f.harness.closed;expect(hit).toBe(true);
  await f.reopen();f.faux.setResponses(Array.from({length:8},()=>answer('Recovered input')));
  const obligation=Object.values((await f.state()).outbox.obligations)[0];
  await f.harness.waitForTask(obligation.deliveryTaskId!,context);
  for(const old of queue)expect((await (await f.harness.submission(old.id,context))!.wait(context)).status).toBe('done');
  const rows=(await f.storage.scanSubmissions({conversationId:worker.conversationId},100,undefined,context)).items;
  expect(rows.filter(r=>r.requestId===`ptb:${obligation.key}`)).toHaveLength(1);
  const entries=(await (await f.harness.conversation(worker.conversationId,context))!.entries({order:'ascending'},100,undefined,context)).items;
  for(const item of before){
   const record=rows.find(r=>r.id===item.id)!;expect(record.status).toBe('done');
   const entry=entries.find(e=>e.id===record.entry)!;expect(entry.kind).toBe(item.entry?.kind??'pi.user');
   if(item.entry)expect(entry.data).toEqual(item.entry.data);
  }
  expect((await f.state()).outbox.obligations[obligation.key].outcome).toBe('answered');
 }finally{await f.close();}
},10000);
