import { expect,test,vi } from 'vitest';
import { defineTask,LiveDoc,type Conversation,type EntryRecord } from '@earendil-works/pi-durable';
import { runtimeFixture,context,nextTask } from './runtime-fixture.ts';
import { fauxAssistantMessage as answer } from '@earendil-works/pi-ai/providers/faux';
import { openAuthority as open,obligation,allObligations } from '../../src/authority/store.ts';
import { plain } from '../../src/authority/state.ts';
import { scheduleDeliveries,type TeamNoticeData } from '../../src/authority/delivery.ts';
import { createAuthorityTools } from '../../src/authority/tools.ts';
const deferred=()=>{let resolve!:()=>void;const promise=new Promise<void>(r=>resolve=r);return {promise,resolve};};

type Fixture=Awaited<ReturnType<typeof runtimeFixture>>;
async function worker(f:Fixture){
 await f.runtime.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},f.api(),context);
 return (await f.harness.conversation((await f.state()).team.current!.workers.builder.conversationId,context))!;
}
function holdSelection(f:Fixture){
 const entered=deferred(),release=deferred(),{Deliver,WorkerAnchor,Notify}=f.runtime.tasks;
 const held=defineTask({...Deliver.definition,phases:{...Deliver.definition.phases,interrupt:async(task,runtime,c)=>{
  if(!task.state.checkpoint.selection){entered.resolve();await release.promise;}
  return Deliver.definition.phases.interrupt(task,runtime,c);
 }}});
 f.registry.install({...f.runtime.mechanics,tasks:[WorkerAnchor,held,Notify]});
 return {entered,release};
}
async function send(f:Fixture){
 await f.runtime.core.execute('alert_send','lead',{to:'builder',kind:'attention',text:'Urgent',interrupt:true},f.api(),context);
 return f.root.commit(async tx=>plain((await allObligations(await open(tx)))[0]),context);
}
async function notices(target:Conversation){
 const entries=await target.entries({order:'ascending'},500,undefined,context);
 return entries.items.filter(e=>(e.data as any)?.delivery==='alert').map(e=>({...e,data:e.data as TeamNoticeData}));
}
function body(entry:EntryRecord){const content=entry.model![0].content;return JSON.parse(typeof content==='string'?content:content.filter(b=>b.type==='text').map(b=>b.text).join(''));}
function clock(){
 let time=1000,id=0;const timers=new Map<number,{at:number;callback:()=>void}>();
 return {now:()=>time,setTimer:(callback:()=>void,delay:number)=>{const key=++id;timers.set(key,{at:time+delay,callback});return ()=>{timers.delete(key);};},
  advance:(next:number)=>{time=next;for(const [key,timer] of [...timers])if(timer.at<=time){timers.delete(key);timer.callback();}},
  deadlines:()=>[...timers.values()].map(t=>t.at)};
}

test('delivery selects an owner-started run that began after Alert acceptance',async()=>{
 const firstStarted=deferred(),firstDone=deferred(),secondStarted=deferred();
 const f=await runtimeFixture(),gate=holdSelection(f);
 try{
  const target=await worker(f),abort=vi.spyOn(f.harness,'abortTask');
  f.faux.setResponses([async()=>{firstStarted.resolve();await firstDone.promise;return answer('First done');},async(_m,options)=>{secondStarted.resolve();await new Promise<void>(resolve=>options?.signal?.addEventListener('abort',()=>resolve(),{once:true}));return answer('Aborted');},answer('Alert received')]);
  const first=await target.submit({type:'input',content:'First owner input'},context);await firstStarted.promise;
  const pending=await send(f);expect(pending.interruptAt).toBe('delivery');expect(pending.interruptRun).toBeUndefined();
  f.harness.resume();await gate.entered.promise;
  firstDone.resolve();expect((await first.wait(context)).status).toBe('done');
  const second=await target.submit({type:'input',content:'Second owner input'},context);await secondStarted.promise;
  const run=await f.root.commit(async tx=>(await tx.doc(LiveDoc,target.id)).run!.taskId,context);
  gate.release.resolve();await f.harness.waitForTask(pending.deliveryTaskId!,context);
  expect(abort.mock.calls.map(args=>args[0])).toEqual([run]);
  expect((await second.wait(context)).status).toBe('unanswered');
  const saved=await f.root.commit(async tx=>plain(await obligation(await open(tx),pending.key)),context);
  expect(saved).toMatchObject({interruptRun:run,interruptedRun:run,interruption:'interrupted'});
  const notice=(await notices(target))[0];expect(notice.data?.interruptedRun).toBe(run);
  expect(body(notice).interruption.message).toBe('A run in progress was interrupted to deliver this Alert. Its last input may be unanswered; read it before continuing.');
 }finally{firstDone.resolve();gate.release.resolve();await f.harness.close(context);}
});

test('idle at delivery becomes plain steering without an abort or interruption claim',async()=>{
 const started=deferred(),done=deferred(),f=await runtimeFixture(),gate=holdSelection(f);
 try{
  const target=await worker(f),abort=vi.spyOn(f.harness,'abortTask');
  f.faux.setResponses([async()=>{started.resolve();await done.promise;return answer('Owner done');},answer('Alert received')]);
  const owner=await target.submit({type:'input',content:'Owner work'},context);await started.promise;
  const pending=await send(f);f.harness.resume();await gate.entered.promise;
  done.resolve();await owner.wait(context);gate.release.resolve();await f.harness.waitForTask(pending.deliveryTaskId!,context);
  expect(abort).not.toHaveBeenCalled();
  const notice=(await notices(target))[0];expect(notice.data).not.toHaveProperty('interruptedRun');
  expect(body(notice)).not.toHaveProperty('interruption');
 }finally{done.resolve();gate.release.resolve();await f.harness.close(context);}
});

test('an owner run starting between abort and admission receives a normal steer and no second abort',async()=>{
 const started=deferred(),aborted=deferred(),releasePort=deferred(),nextStarted=deferred(),finishNext=deferred();
 const f=await runtimeFixture([],port=>async(api,input,c)=>{const result=await port(api,input,c);aborted.resolve();await releasePort.promise;return result;});
 try{
  const target=await worker(f),abort=vi.spyOn(f.harness,'abortTask');
  f.faux.setResponses([async(_m,options)=>{started.resolve();await new Promise<void>(resolve=>options?.signal?.addEventListener('abort',()=>resolve(),{once:true}));return answer('Aborted');},async()=>{nextStarted.resolve();await finishNext.promise;return answer('Next owner done');},answer('Alert received')]);
  const first=await target.submit({type:'input',content:'First owner input'},context);await started.promise;
  const oldRun=await f.root.commit(async tx=>(await tx.doc(LiveDoc,target.id)).run!.taskId,context),pending=await send(f);
  f.harness.resume();await aborted.promise;
  const next=await target.submit({type:'input',content:'Next owner input'},context);await nextStarted.promise;
  releasePort.resolve();await nextTask(f.harness,pending.deliveryTaskId!,s=>s?.checkpoint?.phase==='settle');
  expect(abort.mock.calls.map(args=>args[0])).toEqual([oldRun]);
  finishNext.resolve();await f.harness.waitForTask(pending.deliveryTaskId!,context);
  expect((await first.wait(context)).status).toBe('unanswered');expect((await next.wait(context)).status).toBe('done');
  expect((await notices(target))[0].data?.interruptedRun).toBe(oldRun);
 }finally{releasePort.resolve();finishNext.resolve();await f.harness.close(context);}
});

test('restart retains the selected run and absolute settlement deadline',async()=>{
 const time=clock(),started=deferred(),release=deferred(),entered=deferred();
 const f=await runtimeFixture([],port=>async(api,input,c)=>{entered.resolve();return port(api,input,c);},time);
 try{
  const target=await worker(f);
  f.faux.setResponses([async()=>{started.resolve();await release.promise;return answer('Uncooperative owner');}]);
  await target.submit({type:'input',content:'Owner work'},context);await started.promise;
  let selected:any;const failed=deferred();
  f.storage.fault=writes=>{const row=writes.find((w:any)=>w.type==='task'&&w.value.kind==='ptb.deliver'&&w.value.state.checkpoint?.selection) as any;if(row){selected=row.value.state.checkpoint.selection;failed.resolve();return true;}return false;};
  const pending=await send(f);f.harness.resume();await failed.promise;
  expect(selected.deadline).toBe(11000);
  release.resolve();time.advance(9000);await f.reopen();
  const block=deferred();f.faux.setResponses([async()=>{await block.promise;return answer('Owner settles');},answer('Alert received')]);
  try{
   f.harness.resume();await entered.promise;
   expect(time.deadlines()).toContain(11000);expect(time.deadlines()).not.toContain(19000);
   time.advance(11000);await nextTask(f.harness,pending.deliveryTaskId!,s=>s?.checkpoint?.phase==='settle');
   const saved=await f.root.commit(async tx=>plain(await obligation(await open(tx),pending.key)),context);
   expect(saved).toMatchObject({interruptRun:selected.run,interruption:'failed'});
   expect(saved).not.toHaveProperty('interruptedRun');
   block.resolve();await f.harness.waitForTask(pending.deliveryTaskId!,context);
   const notice=(await notices((await f.harness.conversation(target.id,context))!))[0];expect(notice.data).not.toHaveProperty('interruptedRun');
   expect(body(notice).interruption.outcome).toBe('failed');
  }finally{block.resolve();}
 }finally{release.resolve();await f.harness.close(context);}
});

test('an uncooperative provider cannot hold Alert admission beyond the 10 s settlement wait',async()=>{
 const started=deferred(),release=deferred(),admitted=deferred();
 const f=await runtimeFixture([],port=>port,{now:Date.now,setTimer:(callback,delay)=>{const timer=setTimeout(callback,delay);return ()=>clearTimeout(timer);}});
 try{
  const target=await worker(f),abort=vi.spyOn(f.harness,'abortTask');let signalAborted=false;
  f.faux.setResponses([async(_messages,options)=>{options?.signal?.addEventListener('abort',()=>{signalAborted=true;},{once:true});started.resolve();await release.promise;return answer('Provider finally returned');},...Array.from({length:8},()=>answer('Input handled'))]);
  const owner=await target.submit({type:'input',content:'Uncooperative owner request'},context);await started.promise;
  const run=(await f.harness.snapshot(LiveDoc,target.id,context))!.run!.taskId,pending=await send(f);
  f.storage.fault=writes=>{if(writes.some(w=>w.type==='submission'&&w.value.requestId===`ptb:${pending.key}`))admitted.resolve();return false;};
  const start=Date.now();f.harness.resume();await admitted.promise;const elapsed=Date.now()-start;
  expect(elapsed).toBeGreaterThanOrEqual(9900);expect(elapsed).toBeLessThan(15000);
  expect(signalAborted).toBe(true);expect(abort.mock.calls.map(args=>args[0])).toEqual([run]);
  const saved=await f.root.commit(async tx=>plain(await obligation(await open(tx),pending.key)),context);
  expect(saved?.interruption).toBe('failed');expect(saved).not.toHaveProperty('interruptedRun');
  release.resolve();await f.harness.waitForTask(run,context);
  const queued=(await f.storage.scanSubmissions({conversationId:target.id},100,undefined,context)).items.find(row=>row.requestId===`ptb:${pending.key}`)!;
  expect(queued.status).toBe('queued');expect((await f.harness.snapshot(LiveDoc,target.id,context))!.run).toBeUndefined();
  // Native abort leaves queued inputs untouched. A later owner input starts the next run.
  await target.submit({type:'input',content:'Owner resumes after aborted run'},context);
  await f.harness.waitForTask(pending.deliveryTaskId!,context);
  expect((await owner.status(context)).status).toBe('unanswered');
  expect((await notices(target))).toHaveLength(1);
  expect(body((await notices(target))[0]).interruption.outcome).toBe('failed');
  console.log(`Uncooperative-provider admission after ${elapsed} ms`);
 }finally{release.resolve();await f.close();}
},20000);

test('restart never replaces its recorded run with a newer owner run',async()=>{
 const started=deferred(),release=deferred(),entered=deferred(),releasePort=deferred(),nextStarted=deferred(),finishNext=deferred();
 const f=await runtimeFixture([],port=>async(api,input,c)=>{entered.resolve();await releasePort.promise;return port(api,input,c);});
 try{
  const target=await worker(f);
  f.faux.setResponses([async()=>{started.resolve();await release.promise;return answer('Original done');}]);
  const original=await target.submit({type:'input',content:'Original owner'},context);await started.promise;
  const run=(await f.harness.snapshot(LiveDoc,target.id,context))!.run!.taskId;
  let selected:any;f.storage.fault=writes=>{const row=writes.find((w:any)=>w.type==='task'&&w.value.kind==='ptb.deliver'&&w.value.state.checkpoint?.selection) as any;if(row){selected=row.value.state.checkpoint.selection;release.resolve();return true;}return false;};
  const pending=await send(f);f.harness.resume();await f.harness.closed;expect(selected.run).toBe(run);
  release.resolve();await f.reopen();const abort=vi.spyOn(f.harness,'abortTask');
  f.faux.setResponses(Array.from({length:8},()=>async(messages)=>{if(JSON.stringify(messages).includes('New owner')&&!JSON.stringify(messages).includes('Urgent')){nextStarted.resolve();await finishNext.promise;}return answer('Handled');}));
  f.harness.resume();await entered.promise;
  await (await f.harness.submission(original.id,context))!.wait(context);
  const current=(await f.harness.conversation(target.id,context))!,next=await current.submit({type:'input',content:'New owner'},context);await nextStarted.promise;
  releasePort.resolve();await nextTask(f.harness,pending.deliveryTaskId!,s=>s?.checkpoint?.phase==='settle');
  expect(abort).not.toHaveBeenCalled();finishNext.resolve();await f.harness.waitForTask(pending.deliveryTaskId!,context);
  expect((await next.wait(context)).status).toBe('done');
  expect((await f.state()).outbox.obligations[pending.key]).toMatchObject({interruptRun:run,interruption:'ended'});
  const notice=(await notices(current))[0];expect(notice.data).not.toHaveProperty('interruptedRun');
  expect(body(notice).interruption).toEqual({outcome:'ended',message:'The run selected for this Alert already ended or changed.'});
 }finally{release.resolve();releasePort.resolve();finishNext.resolve();await f.close();}
});

// Native run-only abort clears LiveDoc without a final inbox boundary. Keep the eventual-delivery claim visible.
test.fails('incident: a fallback queued before abort settlement should eventually settle without another admission',async()=>{
 const time=clock(),started=deferred(),aborted=deferred(),release=deferred(),admitted=deferred();
 const f=await runtimeFixture([],port=>port,time);let timeout:ReturnType<typeof setTimeout>|undefined;
 try{
  const target=await worker(f);
  f.faux.setResponses([async(_messages,options)=>{options?.signal?.addEventListener('abort',aborted.resolve,{once:true});started.resolve();await release.promise;return answer('Provider returned');},...Array.from({length:8},()=>answer('Input handled'))]);
  await target.submit({type:'input',content:'Uncooperative owner request'},context);await started.promise;
  const run=(await f.harness.snapshot(LiveDoc,target.id,context))!.run!.taskId,pending=await send(f);
  f.storage.fault=writes=>{if(writes.some(w=>w.type==='submission'&&w.value.requestId===`ptb:${pending.key}`))admitted.resolve();return false;};
  f.harness.resume();await aborted.promise;time.advance(11000);await admitted.promise;
  release.resolve();await f.harness.waitForTask(run,context);
  const row=(await f.storage.scanSubmissions({conversationId:target.id},100,undefined,context)).items.find(r=>r.requestId===`ptb:${pending.key}`)!;
  const submission=(await f.harness.submission(row.id,context))!;
  const result=await Promise.race([submission.wait(context),new Promise(resolve=>{timeout=setTimeout(()=>resolve({status:'still_queued'}),500);})]);
  expect(result).toMatchObject({status:'done'});
 }finally{clearTimeout(timeout);release.resolve();await f.close();}
});

test('an unmarked idle-at-send obligation never selects a later owner run',async()=>{
 const started=deferred(),finish=deferred(),f=await runtimeFixture(),gate=holdSelection(f);
 try{
  const target=await worker(f),abort=vi.spyOn(f.harness,'abortTask');
  // Model the old persisted obligation before its delivery task was scheduled.
  await createAuthorityTools({modelRoles:[]}).execute('alert_send','lead',{to:'builder',kind:'attention',text:'Legacy idle Alert',interrupt:true},f.api(),context);
  const pending=await f.root.commit(async tx=>{const s=await open(tx),o=(await allObligations(s))[0];delete o.interruptAt;await scheduleDeliveries(tx,s,f.runtime.tasks.Deliver);return plain(o);},context);
  f.harness.resume();await gate.entered.promise;
  f.faux.setResponses([async()=>{started.resolve();await finish.promise;return answer('Owner done');},answer('Legacy Alert handled')]);
  const owner=await target.submit({type:'input',content:'Later owner run'},context);await started.promise;
  gate.release.resolve();await nextTask(f.harness,pending.deliveryTaskId!,s=>s?.checkpoint?.phase==='settle');
  expect(abort).not.toHaveBeenCalled();finish.resolve();await f.harness.waitForTask(pending.deliveryTaskId!,context);
  expect((await owner.wait(context)).status).toBe('done');
  expect(body((await notices(target))[0]).interruption).toEqual({outcome:'ended',message:'The captured run already ended or changed.'});
 }finally{gate.release.resolve();finish.resolve();await f.close();}
});

test('restart after the recorded deadline admits fallback without another abort or a renewed deadline',async()=>{
 const time=clock(),started=deferred(),release=deferred(),f=await runtimeFixture([],port=>port,time);
 try{
  const target=await worker(f);
  f.faux.setResponses([async()=>{started.resolve();await release.promise;return answer('Owner done');}]);
  await target.submit({type:'input',content:'Owner work'},context);await started.promise;
  f.storage.fault=writes=>{if(writes.some((w:any)=>w.type==='task'&&w.value.kind==='ptb.deliver'&&w.value.state.checkpoint?.selection)){release.resolve();return true;}return false;};
  const pending=await send(f);f.harness.resume();await f.harness.closed;
  time.advance(12000);await f.reopen();const abort=vi.spyOn(f.harness,'abortTask');
  f.faux.setResponses(Array.from({length:8},()=>answer('Recovered input')));f.harness.resume();await f.harness.waitForTask(pending.deliveryTaskId!,context);
  expect(abort).not.toHaveBeenCalled();expect(time.deadlines()).not.toContain(22000);
  expect((await f.state()).outbox.obligations[pending.key].interruption).toBe('failed');
 }finally{release.resolve();await f.close();}
});

test.fails('incident: an ordinary Alert queued before native run-only abort should eventually settle',async()=>{
 const started=deferred(),release=deferred(),admitted=deferred(),f=await runtimeFixture();let timeout:ReturnType<typeof setTimeout>|undefined;
 try{
  const target=await worker(f);
  f.faux.setResponses([async(_messages,options)=>{options?.signal?.addEventListener('abort',release.resolve,{once:true});started.resolve();await release.promise;return answer([],{stopReason:'aborted'});},...Array.from({length:8},()=>answer('Input handled'))]);
  await target.submit({type:'input',content:'Owner work'},context);await started.promise;
  const run=(await f.harness.snapshot(LiveDoc,target.id,context))!.run!.taskId;
  f.storage.fault=writes=>{if(writes.some(w=>w.type==='submission'&&w.value.requestId?.startsWith('ptb:')))admitted.resolve();return false;};
  await f.runtime.core.execute('alert_send','lead',{to:'builder',kind:'attention',text:'Ordinary Alert'},f.api(),context);f.harness.resume();await admitted.promise;
  await f.harness.abortTask(run,context);await f.harness.waitForTask(run,context);
  const row=(await f.storage.scanSubmissions({conversationId:target.id},100,undefined,context)).items.find(r=>r.requestId?.startsWith('ptb:'))!;
  const submission=(await f.harness.submission(row.id,context))!;
  const result=await Promise.race([submission.wait(context),new Promise(resolve=>{timeout=setTimeout(()=>resolve({status:'still_queued'}),500);})]);
  expect(result).toMatchObject({status:'done'});
 }finally{clearTimeout(timeout);release.resolve();await f.close();}
});
