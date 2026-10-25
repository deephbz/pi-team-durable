import { expect,test } from 'vitest';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { Harness,MemoryStorage,createRegistry,type TaskId,type SubmissionId,type EntryId } from '@earendil-works/pi-durable';
import { createAuthorityTools,type CoreApi } from '../../src/authority/tools.ts';
import { open,plain } from '../../src/authority/state.ts';
import { openAuthority,flushAuthority } from '../../src/authority/store.ts';
import { openObservations,observations,reserve,savePage,page,advanceAcknowledged,retireBody,readObservationCursor,startObservationEpoch,ObservationHeadDoc,ObservationDoc,PageDoc,PageBodyDoc } from '../../src/authority/observation-store.ts';
import { updates,pageEvents } from '../../src/authority/sync.ts';
import { pageEvents as referencePageEvents,updates as referenceUpdates } from '../oracle/authority/sync.ts';
import { ObservationDoc as PObservationDoc } from '../oracle/authority/observation.ts';

// P's page formula is independent of the indexed cursor and the indexed update projection.
// Each seed acknowledges pages out of order across a gap, rebuilds the observation slice as P documents,
// and starts a second epoch. The test compares the current update pages with P's pages after every acknowledgement.
const SEEDS=Number(process.env.CURSOR_PARITY_SEEDS??200);
for(const seed of Array.from({length:SEEDS},(_,i)=>i+1))test(`indexed cursor and update pages match P across acknowledgement gaps, seed ${seed}`,async()=>{
 const h=await Harness.open(new MemoryStorage(),{models:createModels(),registry:createRegistry(),now:()=>1000,setTimer:()=>()=>{}},context);
 try{
  const root=await h.root(context);let id=1000000;
  const core=createAuthorityTools({modelRoles:[{name:'builder',use:'Build',model:{provider:'fake',modelId:'fake'}}],defaultModelRole:'builder',stopWorker:async()=>true,createWorker:async (tx:any)=>({conversationId:(await tx.createConversation({ownership:{kind:'owner'}} as any)).id,anchorId:0 as TaskId} as any)} as any);
  const call=(name:any,input:unknown)=>core.execute(name,'lead',input,{taskId:++id as TaskId,conversationId:root.id,commit:root.commit.bind(root),now:()=>1000} as CoreApi,context);
  await call('team_create',{name:'cursor-demo',purpose:'Read without consuming'});
  await call('ensure_worker',{name:'alice',scope:'Build'});
  let plan=await call('task_graph_apply',{operation_id:'plan',tasks:[{key:'sample',title:'Exact owner text',goal:'Works',assignee:'alice'}]});
  let epoch=await h.commit(async tx=>(await openAuthority(tx)).team.current!.epoch,context);
  const seedPages=()=>h.commit(async tx=>{
   const s=await openAuthority(tx),o=await openObservations(tx);startObservationEpoch(o,epoch);
   const anchors:EntryId[]=[];
   for(let seq=1;seq<=80;seq++){
    const entry=await tx.appendEntry(root.id,{kind:'ptb.event',data:{seq,epoch,actor:'alice',kind:'task_transitioned',taskId:'sample',at:'2030-01-01T00:00:00Z',text:`Update ${seq} · 保留`}});
    anchors.push(entry.id);
   }
   s.journal.seq=80;s.journal.events=[];flushAuthority(s);
   for(let n=0;n<=12;n++)await reserve(o,{id:`${epoch}:page-${n}`,epoch,lead:root.id,run:0 as SubmissionId,from:n,through:n+1,scanThrough:anchors[(n*37)%80],carrier:{kind:'tool',callId:`page-${n}`}},{kind:'caught_up'});
   const baseline=(await page(o,`${epoch}:page-0`))!;baseline.state='acknowledged';await savePage(o,baseline);await advanceAcknowledged(o);
  },context);
  await seedPages();
  const migratePages=async()=>{
   const saved=await h.commit(observations,context);
   // Rebuild the observation slice as real P documents. Retired bodies did not exist in P.
   await h.commit(async tx=>{
    for(const p of Object.values(saved.pages)){await tx.retireDoc(PageDoc,p.id);if(!p.pruned)await tx.retireDoc(PageBodyDoc,p.id);}
    await tx.retireDoc(ObservationHeadDoc);await tx.retireDoc(ObservationDoc);
   },context);
   await h.commit(async tx=>{
    const pages=Object.fromEntries(Object.values(saved.pages).map(({pruned:_pruned,...p})=>[p.id,{...p,body:JSON.stringify({kind:'caught_up'})}]));
    Object.assign(await tx.doc(PObservationDoc),plain({...saved,pages}));
   },context);
   await h.commit(async tx=>{await openObservations(tx);},context);
  };
  let random=seed>>>0;const next=()=>{random=(Math.imul(random,1664525)+1013904223)>>>0;return random;};
  const order=Array.from({length:11},(_,i)=>i+2);
  for(let n=order.length-1;n>0;n--){const at=next()%(n+1);[order[n],order[at]]=[order[at],order[n]];}
  // A page beyond the gap always acknowledges first. The missing bridge arrives partway through.
  order.splice(5,0,1);
  const sequence=[...order.slice(0,6),...order];let previousEpoch:string|undefined;
  for(let step=0;step<sequence.length;step++){
   const n=sequence[step];
   if(step===3||step===10)await migratePages();
   if(step===6){
    previousEpoch=epoch;
    await call('task_update',{task_id:'sample',operation_id:'cancel',expected_version:(plan.tasks_by_key as any).sample.version,transition:'cancel'});
    await call('team_shutdown',{});
    await call('team_create',{name:'cursor-demo',purpose:'Read without consuming'});
    await call('ensure_worker',{name:'alice',scope:'Build'});
    plan=await call('task_graph_apply',{operation_id:'plan',tasks:[{key:'sample',title:'Exact owner text',goal:'Works',assignee:'alice'}]});
    epoch=await h.commit(async tx=>(await openAuthority(tx)).team.current!.epoch,context);
    await seedPages();
   }
   await h.commit(async tx=>{
    const o=await openObservations(tx),p=(await page(o,`${epoch}:page-${n}`))!;p.state='acknowledged';await savePage(o,p);
    if(previousEpoch&&step<12){const old=(await page(o,`${previousEpoch}:page-${order[step]}`))!;old.state='acknowledged';await savePage(o,old);}
    await advanceAcknowledged(o);
    // Body retirement must leave only the scalar anchor and retained page metadata.
    for(const retired of Object.values((await observations(tx)).pages))if(retired.state==='acknowledged'&&retired.through<=(o.head.ackByEpoch[retired.epoch]??0)&&!retired.pruned)await retireBody(o,retired);
   },context);
   await h.commit(async tx=>{
    const s=await open(tx),o=await observations(tx),from=o.acknowledged;
    const after=Object.values(o.pages).filter(p=>p.epoch===epoch&&p.state==='acknowledged'&&p.through<=from&&p.scanThrough!==undefined).sort((a,b)=>Number(b.scanThrough)-Number(a.scanThrough))[0]?.scanThrough;
    expect(await readObservationCursor(tx,epoch)).toEqual({acknowledged:from,...(after===undefined?{}:{scanThrough:after})});
    const expected=from>=s.journal.seq?{events:[]}:await referencePageEvents(tx,s as any,from,after);
    const current=await openAuthority(tx);
    const actual=from>=s.journal.seq?{events:[]}:await pageEvents(tx,current as any,from,after);
    expect(actual.events).toEqual(expected.events);
    expect(await updates(current as any,actual.events)).toEqual(referenceUpdates(s as any,expected.events));
   },context);
  }
  await h.commit(async tx=>{const o=await openObservations(tx);startObservationEpoch(o,'next-epoch');expect(o.head.acknowledgedScanThrough).toBeUndefined();},context);
 }finally{await h.close(context);}
},30000);
