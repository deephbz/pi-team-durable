import { createHash } from 'node:crypto';
import type { Context } from '@earendil-works/chord/context';
import type { Message,AssistantMessage } from '@earendil-works/pi-ai';
import { defineDoc,GenerationTask,hook,LiveDoc,type JsonObject,type ConversationId,type TaskId,type SubmissionId,type EntryId,type Harness,type Tx,type HookApi,type Extension } from '@earendil-works/pi-durable';
import { open,plain,type Result } from './state.ts';

export interface Page { id:string; epoch:string; lead:ConversationId; run:SubmissionId; from:number; through:number; body:string; digest:string; carrier:{kind:'tool';callId:string}|{kind:'input';text:string}; state:'reserved'|'presented'|'released'|'acknowledged'|'replaced'; scanThrough?:EntryId; submissionId?:SubmissionId }
interface Proof { generation:TaskId; lead:ConversationId; run:SubmissionId; requestDigest:string; pages:string[]; responseDigest?:string }
export interface Observations { pages:Record<string,Page>; proofs:Record<string,Proof>; acknowledged:number; epoch?:string; paused?:boolean; resumedOwnerRun?:SubmissionId }
export const ObservationDoc=defineDoc<JsonObject>({kind:'ptb.observation',version:1,scope:'session',initial:()=>({pages:{},proofs:{},acknowledged:0}),checkpointWhen:(_v,_o,i)=>i.deltasSinceBase>=31});
export const digest=(value:unknown)=>createHash('sha256').update(typeof value==='string'?value:JSON.stringify(value)).digest('hex');
export const observations=async(tx:Tx)=>await tx.doc(ObservationDoc) as unknown as Observations;
export function reservationHead(o:Observations,epoch:string,run:SubmissionId):number {
 return Math.max(o.epoch===epoch?o.acknowledged:0,...Object.values(o.pages).filter(p=>p.epoch===epoch&&p.run===run&&(p.state==='reserved'||p.state==='presented')).map(p=>p.through));
}
export function reserve(o:Observations,input:Omit<Page,'body'|'digest'|'state'>,result:Result):Page {
 if(o.epoch!==input.epoch){o.epoch=input.epoch;o.acknowledged=0;o.paused=false;}
 if(o.pages[input.id])return o.pages[input.id];
 const body=JSON.stringify(result),page:Page={...input,body,digest:digest(body),state:'reserved'};
 o.pages[input.id]=page;return page;
}
function texts(message:Message):string[] {
 const content=(message as any).content;
 return typeof content==='string'?[content]:Array.isArray(content)?content.filter(c=>c.type==='text').map(c=>c.text):[];
}
function visible(page:Page,messages:readonly Message[]):boolean {
 const carrier=page.carrier;
 return messages.some(message=>carrier.kind==='tool'
  ?message.role==='toolResult'&&(message as any).toolCallId===carrier.callId&&texts(message).some(text=>text===page.body)
  :message.role==='user'&&texts(message).some(text=>text===carrier.text));
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
    const o=await observations(tx),ids:string[]=[];
    for(const p of Object.values(o.pages)){
     if(p.lead!==api.conversationId||(p.run!==run&&p.run!==0)||!['reserved','presented'].includes(p.state))continue;
     if(visible(p,request.messages)){p.run=run;ids.push(p.id);p.state='presented';}
     else if(p.state==='reserved'&&p.run===run)p.state='released';
    }
    // Replace on every attempt. An earlier response cannot attest a later request.
    o.proofs[String(api.taskId)]={generation:api.taskId,lead:api.conversationId,run,requestDigest:digest(request.messages),pages:ids};
   },context);
  },
  afterResponse:async(message,api,context)=>{
   await recordResponse(await resolvePort(api,context),api,message,context);
  },
 })]};
}
export async function recordResponse(port:ObservationPort,api:Pick<HookApi,'taskId'>,message:AssistantMessage,context:Context){
 await port.commit(async tx=>{
  const o=await observations(tx),proof=o.proofs[String(api.taskId)];
  if(proof&&['stop','length','toolUse'].includes(message.stopReason))proof.responseDigest=digest(message);
 },context);
}

/** Later observer: native response persistence and run settlement are both required. */
export async function observe(harness:Harness,context:Context):Promise<void> {
 const snapshot=await harness.commit(async tx=>plain(await observations(tx)),context);
 const proofs=Object.values(snapshot.proofs);
 const witnessed=new Set<string>();
 // Generation checkpoints/results name their persisted assistant entry. No transcript scan.
 for(const proof of proofs){
  if(!proof.responseDigest||!proof.pages.length)continue;
  const native=await harness.getTask(proof.generation,context),state=native?.state as any;
  const entryId=state?.checkpoint?.assistant??(state?.outcome?.status==='completed'?state.outcome.result?.entryId:undefined);
  if(entryId===undefined)continue;
  const entry=await harness.commit(tx=>tx.entry(entryId),context);
  if(entry?.byTaskId===proof.generation&&entry.kind==='pi.assistant'&&entry.model?.some(m=>digest(m)===proof.responseDigest))for(const id of proof.pages)witnessed.add(id);
 }
 const runs=new Map<SubmissionId,'live'|'success'|'failure'>();
 for(const page of Object.values(snapshot.pages)){
  if(!['reserved','presented'].includes(page.state)||runs.has(page.run))continue;
  const submission=await harness.submission(page.run,context),record=await submission?.status(context);
  runs.set(page.run,record?.status==='done'?'success':record?.status==='unanswered'?'failure':'live');
 }
 await harness.commit(async tx=>{
  const o=await observations(tx),s=await open(tx);
  for(const p of Object.values(o.pages)){
   if(!['reserved','presented'].includes(p.state))continue;
   // Only unchanged persisted evidence may settle this reservation.
   if(JSON.stringify(p)!==JSON.stringify(snapshot.pages[p.id]))continue;
   const outcome=runs.get(p.run);
   if(outcome==='live')continue;
   const stableProof=Object.values(o.proofs).some(proof=>proof.pages.includes(p.id)&&JSON.stringify(proof)===JSON.stringify(snapshot.proofs[String(proof.generation)]));
   if(outcome==='success'&&witnessed.has(p.id)&&stableProof){
    p.state='acknowledged';
   }else if(outcome==='failure'||outcome==='success'){p.state='released';if(outcome==='failure')o.paused=true;}
  }
  for(let advanced=true;advanced;){
   advanced=false;
   for(const p of Object.values(o.pages))if(p.epoch===o.epoch&&p.state==='acknowledged'&&p.from<=o.acknowledged&&p.through>o.acknowledged){o.acknowledged=p.through;advanced=true;}
  }
  if(s.team.current?.epoch===o.epoch)s.journal.acknowledged=o.acknowledged;
 },context);
}
