// Synthetic SQLite carrier for a version-1 reservation and its version-2 migration.
import { readFileSync,writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxProvider,fauxAssistantMessage as answer } from '@earendil-works/pi-ai/providers/faux';
import { createRegistry,Harness,LiveDoc,type TaskId,type StorageWrite } from '@earendil-works/pi-durable';
import { openNodeSqliteStorage } from '@earendil-works/pi-durable/storage/sqlite/node';
import { createTeamRuntime } from '../../src/authority/runtime.ts';
import { createAuthorityTools } from '../../src/authority/tools.ts';
import { abortExpectedRun } from '../../src/authority/interrupt.ts';
import { scheduleDeliveries } from '../../src/authority/delivery.ts';
import { openAuthority,allObligations } from '../../src/authority/store.ts';
import { plain } from '../../src/authority/state.ts';
import { createDeliver as oldDeliver,deliveryDraft as oldDraft } from '../compat/rc2/delivery.ts';

const [_case,dir,mode]=process.argv.slice(2),idsFile=join(dir,'ids.json');
const send=(type:string,name?:string,value?:unknown)=>process.send!({type,...(name?{name}:{}),...(value===undefined?{}:{value})});
const barrier=async(name:string)=>{send('barrier',name);await new Promise<void>(()=>{});};
let h!:Harness,serial=1000000,oldAbortCalls=0,seedIds:any;
const models=createModels(),faux=fauxProvider({provider:'version-faux',api:'version-faux',models:[{id:'test'}],tokensPerSecond:0});models.setProvider(faux.provider);
faux.setResponses(Array.from({length:12},()=>answer('Recovered input')));
const rt=createTeamRuntime({modelRoles:[{name:'builder',use:'Build',model:{provider:'version-faux',modelId:'test'}}],defaultModelRole:'builder',
 observationPort:async()=>({commit:(fn,c)=>h.commit(fn,c)}),stop:async()=>[],abortRun:(_api,target,c)=>abortExpectedRun(h,target,c)});
const old=oldDeliver(async()=>{oldAbortCalls++;throw new Error('The old reader must not execute this delivery');});
const registry=createRegistry();for(const extension of rt.extensions)registry.install(extension);
if(mode==='seed'||mode==='old-reader')registry.install({...rt.mechanics,tasks:[rt.tasks.WorkerAnchor,old,rt.tasks.Notify]});
const db=await openNodeSqliteStorage(join(dir,'store.sqlite')),commit=db.commit.bind(db);
let cut=false;
db.commit=async(writes:readonly StorageWrite[],c)=>{
 const seq=await commit(writes,c);
 const row=writes.find(w=>w.type==='task'&&w.value.kind==='ptb.deliver'&&w.value.version===(mode==='seed'?1:2));
 if(!cut&&row?.type==='task'&&(['seed','new-seed','migrate'].includes(mode))){
  cut=true;
  if(mode==='seed'||mode==='new-seed')writeFileSync(idsFile,JSON.stringify({...seedIds,id:row.value.id,input:row.value.input,checkpoint:(row.value.state as any).checkpoint}));
  await barrier(mode==='seed'?'v1-seeded':mode==='new-seed'?'v2-seeded':'v2-migrated');
 }
 return seq;
};
try{
 h=await Harness.open(db,{models,registry,now:()=>1000,setTimer:()=>()=>{},onWake:()=>{},settings:{retry:{maxRetries:0},compaction:{enabled:false}}},context);
 const root=await h.root(context,{agent:{model:{provider:'version-faux',modelId:'test'},extensions:[rt.mechanics,rt.lead,rt.observation]}});
 const api=()=>({taskId:++serial as TaskId,conversationId:root.id,commit:root.commit.bind(root),now:()=>1000});
 if(mode==='seed'||mode==='new-seed'){
  await rt.core.execute('team_create','lead',{name:'version-demo',purpose:'Keep the recorded choice'},api(),context);
  await rt.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},api(),context);
  const worker=await root.commit(async tx=>plain((await openAuthority(tx)).team.current!.workers.builder),context),target=(await h.conversation(worker.conversationId,context))!;
  let started!:()=>void;const running=new Promise<void>(r=>started=r);
  faux.setResponses([async()=>{started();await new Promise<void>(()=>{});return answer('Never reached');}]);
  await target.submit({type:'input',content:'Original owner work'},context);await running;
  const captured=(await h.snapshot(LiveDoc,worker.conversationId,context))!.run!.taskId;
  const queued=await target.submit({type:'input',content:'Queued owner survives migration',whenBusy:'followUp',requestId:'owner-queue'},context);
  await createAuthorityTools({modelRoles:[]}).execute('alert_send','lead',{to:'builder',kind:'attention',text:'Legacy interrupt',interrupt:true},api(),context);
  await root.commit(async tx=>{
   const s=await openAuthority(tx),o=(await allObligations(s))[0];if(mode==='seed'){delete o.interruptAt;o.interruptRun=captured;}
   seedIds={key:o.key,worker:worker.conversationId,captured,queued:queued.id};
   await scheduleDeliveries(tx,s,mode==='seed'?old:rt.tasks.Deliver);
  },context);
 }else{
  const ids=JSON.parse(readFileSync(idsFile,'utf8'));
  const before=await h.getTask(ids.id,context);
  if(mode==='migrate'){h.resume();await new Promise<void>(()=>{});}
  if(mode==='old-reader'){
   h.resume();const inspection=await h.inspect(context),diagnostic=inspection.tasks.find(t=>t.record.id===ids.id)!.state;
   const state=await root.commit(async tx=>plain((await allObligations(await openAuthority(tx)))[0]),context);
   const submissions=(await db.scanSubmissions({conversationId:ids.worker},100,undefined,context)).items.filter(s=>s.requestId===`ptb:${ids.key}`);
   const after=await h.getTask(ids.id,context);
   await h.close(context);send('result',undefined,{diagnostic,state:state.state,submissions:submissions.length,oldAbortCalls,before,after});
  }else if(mode==='recovered'){
   h.resume();await h.waitForTask(ids.id,context);
   const state=await root.commit(async tx=>plain((await allObligations(await openAuthority(tx)))[0]),context);
   const rows=(await db.scanSubmissions({conversationId:ids.worker},100,undefined,context)).items;
   const target=(await h.conversation(ids.worker,context))!,entries=(await target.entries({order:'ascending'},100,undefined,context)).items;
   const notice=entries.find(e=>(e.data as any)?.deliveryId===ids.key)!;
   const content=notice.model![0].content,text=typeof content==='string'?content:content.filter(b=>b.type==='text').map(b=>b.text).join('');
   const legacy=oldDraft(ids.input,state.interruption);
   const version=(await h.getTask(ids.id,context))?.version;
   await h.close(context);send('result',undefined,{before,ids,selected:state.interruptRun,version,submissions:rows.filter(s=>s.requestId===`ptb:${ids.key}`).length,queued:rows.find(s=>s.id===ids.queued)?.status,content:text,legacyContent:legacy.content,data:notice.data,legacyData:legacy.entry.data});
  }
  process.disconnect!();
 }
}catch(error){send('failure',undefined,String((error as Error).stack));await h?.close(context).catch(()=>{});process.exitCode=1;process.disconnect?.();}
