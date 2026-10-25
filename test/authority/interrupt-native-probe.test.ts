import { expect,test } from 'vitest';
import { LiveDoc,InboxDoc } from '@earendil-works/pi-durable';
import { fauxAssistantMessage as answer } from '@earendil-works/pi-ai/providers/faux';
import { runtimeFixture,context } from './runtime-fixture.ts';
const deferred=()=>{let resolve!:()=>void;const promise=new Promise<void>(r=>resolve=r);return {promise,resolve};};

for(const mode of ['conversation','run'] as const)test(`native ${mode} abort queue identity and next-input probe`,async()=>{
 const f=await runtimeFixture();try{
  await f.runtime.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},f.api(),context);
  const worker=(await f.state()).team.current!.workers.builder;
  const target=(await f.harness.conversation(worker.conversationId,context))!;
  const started=deferred(),cancelled=deferred();const requests:string[]=[];
  f.faux.setResponses([async(_messages,options)=>{
   options?.signal?.addEventListener('abort',cancelled.resolve,{once:true});started.resolve();await cancelled.promise;return answer([],{stopReason:'aborted'});
  },messages=>{requests.push(JSON.stringify(messages));return answer('Next input handled');},answer('Remaining handled')]);
  const original=await target.submit({type:'input',content:'Original busy work'},context);
  await started.promise;
  const queue=[
   await target.submit({type:'input',content:'Queued owner',whenBusy:'followUp',requestId:'owner'},context),
   await target.submit({type:'input',content:'Queued Task',whenBusy:'followUp',requestId:'task',entry:{kind:'team.notice',data:{source:'team',delivery:'task',deliveryId:'task'}}},context),
   await target.submit({type:'input',content:'Older Alert',whenBusy:'steer',requestId:'older-alert',entry:{kind:'team.notice',data:{source:'team',delivery:'alert',deliveryId:'older-alert'}}},context),
  ];
  const before=await f.harness.snapshot(InboxDoc,worker.conversationId,context);
  const run=(await f.harness.snapshot(LiveDoc,worker.conversationId,context))!.run!.taskId;
  if(mode==='conversation')await target.abort(context);
  else {await f.harness.abortTask(run,context);await f.harness.waitForTask(run,context);}
  expect((await original.status(context)).status).toBe('unanswered');
  for(const submission of queue)expect((await submission.status(context)).status).toBe(mode==='run'?'queued':'unanswered');
  const after=await f.harness.snapshot(InboxDoc,worker.conversationId,context);
  expect(after!.items).toEqual(mode==='run'?before!.items:[]);
  const urgent=await target.submit({type:'input',content:'Interrupt Alert',whenBusy:'steer',requestId:'interrupt-alert',entry:{kind:'team.notice',data:{source:'team',delivery:'alert',deliveryId:'interrupt-alert'}}},context);
  await urgent.wait(context);
  if(mode==='run'){
   expect(requests[0]).toContain('Older Alert');
   expect(requests[0]).not.toContain('Interrupt Alert');
  }else expect(requests[0]).toContain('Interrupt Alert');
 }finally{await f.close();}
},10000);
