import type { Context } from '@earendil-works/chord/context';
import type { Message,AssistantMessage } from '@earendil-works/pi-ai';
import { GenerationTask,hook,LiveDoc,type SubmissionId,type Harness,type Tx,type HookApi,type Extension } from '@earendil-works/pi-durable';
import { openAuthority as open,flushAuthority } from './store.ts';
import { openObservations,pages,savePage,openProofs,proof,saveProof,retireProof,retireBody,advanceAcknowledged,digest,type Page } from './observation-store.ts';
export { ObservationDoc,ObservationHeadDoc,PageDoc,PageBodyDoc,ProofDoc,observations,observationStats,openObservations,reservationHead,reserve,startObservationEpoch,digest,type Page,type Observations } from './observation-store.ts';

function texts(message:Message):string[] {
 const content=(message as any).content;
 return typeof content==='string'?[content]:Array.isArray(content)?content.filter(c=>c.type==='text').map(c=>c.text):[];
}
function visible(page:Page,messages:readonly Message[]):boolean {
 const carrier=page.carrier;
 for(let i=messages.length-1;i>=0;i--){
  const message=messages[i];
  if(carrier.kind==='tool'
   ?message.role==='toolResult'&&(message as any).toolCallId===carrier.callId&&texts(message).some(text=>text===page.body)
   :message.role==='user'&&texts(message).some(text=>text===carrier.text))return true;
 }
 return false;
}
export interface ObservationPort { commit<T>(fn:(tx:Tx)=>Promise<T>,context:Context):Promise<T> }
/** Install last among message-replacing hooks. No hook advances the acknowledged cursor. */
export type ObservationPortResolver=(api:HookApi,context:Context)=>Promise<ObservationPort>;
export function observationExtension(resolvePort:ObservationPortResolver):Extension {
 return {name:'ptb.observation',hooks:[hook(GenerationTask,{
  beforeRequest:async(request,api,context)=>{
   const port=await resolvePort(api,context);
   const live=await api.snapshot(LiveDoc,api.conversationId,context),run=live?.run?.inputs[0];
   if(run===undefined)return;
   await port.commit(async tx=>{
    const o=await openObservations(tx),ids:string[]=[];
    for(const p of await pages(o,'openPages',true)){
     if(p.lead!==api.conversationId||(p.run!==run&&p.run!==0))continue;
     if(visible(p,request.messages)){
      ids.push(p.id);
      if(p.run!==run||p.state!=='presented'){p.run=run;p.state='presented';await savePage(o,p);}
     }else if(p.state==='reserved'&&p.run===run){p.state='released';await savePage(o,p);}
    }
    // Replace on every attempt. An earlier response cannot attest a later request.
    if(ids.length)await saveProof(o,{generation:api.taskId,lead:api.conversationId,run,pages:ids});
    else await retireProof(o,String(api.taskId));
   },context);
  },
  afterResponse:async(message,api,context)=>{
   await recordResponse(await resolvePort(api,context),api,message,context);
  },
 })]};
}
export async function recordResponse(port:ObservationPort,api:Pick<HookApi,'taskId'>,message:AssistantMessage,context:Context){
 await port.commit(async tx=>{
  const o=await openObservations(tx),p=await proof(o,String(api.taskId));
  if(p&&['stop','length','toolUse'].includes(message.stopReason)){p.responseDigest=digest(message);await saveProof(o,p);}
 },context);
}

/** Native persistence and settled runs permit ACK. Proofs never scan transcript history. */
export async function observe(harness:Harness,context:Context):Promise<void> {
 const snapshot=await harness.commit(async tx=>{
  const o=await openObservations(tx);
  return {pages:await pages(o,'openPages'),proofs:await openProofs(o)};
 },context);
 const byPage=new Map(snapshot.pages.map(p=>[p.id,p])),byProof=new Map(snapshot.proofs.map(p=>[String(p.generation),p]));
 const witnessed=new Set<string>();
 for(const p of snapshot.proofs){
  if(!p.responseDigest||!p.pages.length)continue;
  const native=await harness.getTask(p.generation,context),state=native?.state as any;
  const entryId=state?.checkpoint?.assistant??(state?.outcome?.status==='completed'?state.outcome.result?.entryId:undefined);
  if(entryId===undefined)continue;
  const entry=await harness.commit(tx=>tx.entry(entryId),context);
  if(entry?.byTaskId===p.generation&&entry.kind==='pi.assistant'&&entry.model?.some(m=>digest(m)===p.responseDigest))for(const id of p.pages)witnessed.add(id);
 }
 const runs=new Map<SubmissionId,'live'|'success'|'failure'>();
 for(const run of new Set([...snapshot.pages.map(p=>p.run),...snapshot.proofs.map(p=>p.run)])){
  const submission=await harness.submission(run,context),record=await submission?.status(context);
  runs.set(run,record?.status==='done'?'success':record?.status==='unanswered'?'failure':'live');
 }
 await harness.commit(async tx=>{
  const o=await openObservations(tx),currentProofs=await openProofs(o);
  for(const p of await pages(o,'openPages')){
   if(JSON.stringify(p)!==JSON.stringify(byPage.get(p.id)))continue;
   const outcome=runs.get(p.run);
   if(outcome!=='success'&&outcome!=='failure')continue;
   const stableProof=currentProofs.some(candidate=>candidate.pages.includes(p.id)&&JSON.stringify(candidate)===JSON.stringify(byProof.get(String(candidate.generation))));
   if(outcome==='success'&&witnessed.has(p.id)&&stableProof)p.state='acknowledged';
   else {p.state='released';if(outcome==='failure')o.head.paused=true;}
   await savePage(o,p);
  }
  await advanceAcknowledged(o);
  // A settled, processed proof cannot attest a later request. Drop its body and index.
  for(const p of currentProofs){
   const outcome=runs.get(p.run);
   if((outcome==='success'||outcome==='failure')&&JSON.stringify(p)===JSON.stringify(byProof.get(String(p.generation))))await retireProof(o,String(p.generation));
  }
  const pinned=new Set(currentProofs.filter(p=>!runs.has(p.run)||runs.get(p.run)==='live').flatMap(p=>p.pages));
  for(const p of await pages(o,'prunePages')){
   if(p.through>(o.head.ackByEpoch[p.epoch]??0)||pinned.has(p.id))continue;
   if(p.carrier.kind==='input'){
    // Admission replay still needs the exact input until its notifier leaves settle.
    // Unknown legacy ownership cannot prove that the body is unused.
    if(p.notifierId===undefined)continue;
    const native=await tx.task(p.notifierId),checkpoint=(native?.state as any)?.checkpoint;
    if(!native||native.kind!=='ptb.notify'||(native.input as any)?.epoch!==p.epoch)continue;
    if(native.state.status!=='terminal'){
     if(!checkpoint||!['tick','submit','settle'].includes(checkpoint.phase))continue;
     if(['submit','settle'].includes(checkpoint.phase)&&checkpoint.pageId===p.id)continue;
    }
   }
   await retireBody(o,p);
  }
  const s=await open(tx);
  if(s.team.current?.epoch===o.head.epoch){s.journal.acknowledged=o.head.acknowledged;flushAuthority(s);}
 },context);
}
