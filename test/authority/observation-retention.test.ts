import { expect,test } from 'vitest';
import { ObservationDoc as LegacyObservationDoc } from '../oracle/authority/observation.ts';
import { performance } from 'node:perf_hooks';
import { fauxAssistantMessage as answer } from '@earendil-works/pi-ai/providers/faux';
import type { TaskId,SubmissionId,Tx } from '@earendil-works/pi-durable';
import { runtimeFixture,context,nextTask } from './runtime-fixture.ts';
import { observe,observations,observationStats,observationExtension } from '../../src/authority/observation.ts';
import { openObservations,reserve,savePage,saveProof,retireProof,readObservationCursor,ObservationDoc,ObservationHeadDoc } from '../../src/authority/observation-store.ts';

test('bodies wait for the contiguous cursor and unsettled proofs; repeated observe reads no retired records',async()=>{
 const f=await runtimeFixture();try{
  const epoch=(await f.state()).team.current!.epoch;
  await f.harness.commit(async tx=>{
   const o=await openObservations(tx);
   for(const [id,from,through] of [['low',0,1],['gap',2,3]] as const){
    const p=await reserve(o,{id,epoch,lead:f.root.id,run:0 as SubmissionId,from,through,carrier:{kind:'tool',callId:id}},{kind:'caught_up'});
    p.state='acknowledged';await savePage(o,p);
   }
   await saveProof(o,{generation:900001 as TaskId,lead:f.root.id,run:900002 as SubmissionId,pages:['low']});
  },context);
  await observe(f.harness,context);
  let snapshot=await f.harness.commit(observations,context);
  expect(snapshot.acknowledged).toBe(1);expect(snapshot.pages.low.body).toBeDefined();expect(snapshot.pages.gap.body).toBeDefined();
  await f.harness.commit(async tx=>{
   const o=await openObservations(tx);await retireProof(o,'900001');
   const p=await reserve(o,{id:'bridge',epoch,lead:f.root.id,run:0 as SubmissionId,from:1,through:2,carrier:{kind:'tool',callId:'bridge'}},{kind:'caught_up'});
   p.state='acknowledged';await savePage(o,p);
  },context);
  await observe(f.harness,context);await f.reopen();
  snapshot=await f.harness.commit(observations,context);
  expect(snapshot.acknowledged).toBe(3);expect(Object.values(snapshot.pages).every(p=>p.pruned&&p.body===undefined)).toBe(true);
  const stats=await f.harness.commit(observationStats,context);
  expect(stats.pages.count).toBe(3);expect(stats.bodies.count).toBe(0);expect(stats.openProofs.count).toBe(0);expect(stats.pages.currentRecordBytes).toBeGreaterThan(0);
  const reads:string[]=[],commit=f.harness.commit.bind(f.harness);
  (f.harness as any).commit=(fn:(tx:Tx)=>Promise<unknown>,c:typeof context)=>commit(tx=>fn(new Proxy(tx,{get(target,key){
   if(key==='doc')return (token:any,...args:any[])=>{reads.push(token.definition.kind);return (target.doc as any)(token,...args);};
   const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
  }})),c);
  await observe(f.harness,context);
  expect(reads.filter(kind=>['ptb.observation.page','ptb.observation.body','ptb.observation.proof'].includes(kind))).toEqual([]);
 }finally{await f.close();}
},10000);

test('legacy observation migration preserves bodies, cursor, and request-independent proof evidence',async()=>{
 const f=await runtimeFixture();try{
  const epoch=(await f.state()).team.current!.epoch;
  await f.harness.commit(async tx=>{
   // Recreate a pre-cutover observation store; current fixtures already carry the fence.
   await tx.retireDoc(ObservationHeadDoc);await tx.retireDoc(ObservationDoc);
   const old=await tx.doc(LegacyObservationDoc);old.epoch=epoch;old.acknowledged=4;
   old.pages={old:{id:'old',epoch,lead:f.root.id,run:900002,from:0,through:4,body:'{"kind":"caught_up"}',digest:'synthetic',carrier:{kind:'tool',callId:'old'},state:'acknowledged',scanThrough:9}};
   old.proofs={'900001':{generation:900001,lead:f.root.id,run:900002,requestDigest:'unused',responseDigest:'kept',pages:['old']}};
  },context);
  await f.reopen();const snapshot=await f.harness.commit(observations,context);
  expect(await f.harness.commit(tx=>readObservationCursor(tx,epoch),context)).toEqual({acknowledged:4,scanThrough:9});
  expect(snapshot.acknowledged).toBe(4);expect(snapshot.pages.old.body).toBe('{"kind":"caught_up"}');
  expect(snapshot.proofs['900001'].responseDigest).toBe('kept');expect(snapshot.proofs['900001']).not.toHaveProperty('requestDigest');
  expect(await f.harness.snapshot(ObservationDoc,context)).toEqual({fence:'ptb-authority-v2'});
  await observe(f.harness,context);expect((await f.harness.commit(observationStats,context)).bodies.count).toBe(1);
 }finally{await f.close();}
},10000);

test('beforeRequest cost stays flat with a 4 MB transcript prefix and touches only the newest matching message',async()=>{
 const f=await runtimeFixture();try{
  const epoch=(await f.state()).team.current!.epoch,run=900002 as SubmissionId;
  const p=await f.harness.commit(async tx=>reserve(await openObservations(tx),{id:'recent',epoch,lead:f.root.id,run,from:0,through:1,carrier:{kind:'tool',callId:'recent'}},{kind:'caught_up'}),context);
  const before=(observationExtension(async()=>({commit:f.harness.commit.bind(f.harness)})).hooks![0].handlers as any).beforeRequest;
  const api={conversationId:f.root.id,taskId:900001,snapshot:async()=>({run:{inputs:[run]}})};
  const medians:number[]=[];
  for(const count of [0,100,4000]){
   let historicalReads=0;
   const messages=new Proxy([...Array(count).fill({role:'assistant',content:'x'.repeat(1024)}),{role:'toolResult',toolCallId:'recent',content:[{type:'text',text:p.body}]}],{get(target,key,receiver){
    if(typeof key==='string'&&/^\d+$/.test(key)&&Number(key)<count)historicalReads++;
    return Reflect.get(target,key,receiver);
   }});
   const samples:number[]=[];
   for(let i=0;i<35;i++){const start=performance.now();await before({messages},api,context);if(i>=5)samples.push(performance.now()-start);}
   medians.push(samples.sort((a,b)=>a-b)[Math.floor(samples.length/2)]);expect(historicalReads).toBe(0);
  }
  console.log(JSON.stringify({observationBeforeRequest:{prefixMessages:[0,100,4000],p50Ms:medians}}));
  expect(medians[2]).toBeLessThan(medians[0]*3+2);
  const snapshot=await f.harness.commit(observations,context);expect(snapshot.proofs['900001'].pages).toEqual(['recent']);expect(snapshot.proofs['900001']).not.toHaveProperty('requestDigest');
 }finally{await f.close();}
},10000);

test('a settled notifier page stays replayable through submit and settle, then loses its body',async()=>{
 const f=await runtimeFixture();let releaseSubmit!:()=>void,releaseSettle!:()=>void;
 const submitBarrier=new Promise<void>(resolve=>releaseSubmit=resolve),settleBarrier=new Promise<void>(resolve=>releaseSettle=resolve);
 try{
  const definition=f.runtime.tasks.Notify.definition as any,submit=definition.phases.submit,settle=definition.phases.settle;
  definition.phases.submit=async(task:any,runtime:any,c:any)=>{
   let commits=0;
   const proxy=new Proxy(runtime,{get(target,key){
    if(key==='commit')return async(fn:any,ctx:any)=>{if(++commits===2)await submitBarrier;return target.commit(fn,ctx);};
    const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
   }});
   return submit(task,proxy,c);
  };
  definition.phases.settle=async(...args:any[])=>{await settleBarrier;return settle(...args);};
  const notifier=(await f.state()).team.current!.notifierId!,sleeping=nextTask(f.harness,notifier,s=>s.status==='sleeping');
  f.harness.resume();await sleeping;f.setNow(31000);f.faux.setResponses([answer('Observed')]);
  let off=()=>{};
  const done=new Promise<void>(resolve=>{off=f.harness.subscribeCommits(publication=>{if(publication.changes.some(c=>c.type==='submission'&&c.value.requestId?.includes(':notify:')&&c.value.status==='done')){off();resolve();}});});
  f.harness.resume();await done;
  await observe(f.harness,context);
  let snapshot=await f.harness.commit(observations,context),p=Object.values(snapshot.pages)[0];
  expect(p.state).toBe('acknowledged');expect(p.body).toBeDefined();expect(snapshot.proofs).toEqual({});
  const settling=nextTask(f.harness,notifier,s=>s.status==='running'&&s.checkpoint.phase==='settle');releaseSubmit();await settling;
  await observe(f.harness,context);expect((await f.harness.commit(observationStats,context)).bodies.count).toBe(1);
  const finished=nextTask(f.harness,notifier,s=>s.status==='sleeping'&&s.checkpoint.ordinal===2);releaseSettle();await finished;
  await observe(f.harness,context);snapshot=await f.harness.commit(observations,context);p=Object.values(snapshot.pages)[0];
  expect(p.pruned).toBe(true);expect(p.body).toBeUndefined();expect(f.faux.state.callCount).toBe(1);
  expect((await f.storage.scanSubmissions({conversationId:f.root.id},100,undefined,context)).items).toHaveLength(1);
 }finally{releaseSubmit();releaseSettle();await f.close();}
},15000);
