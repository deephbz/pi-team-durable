// Observation authority separates current indexes, immutable page bodies, and proofs.
// Settled pages retain metadata. Only the observer can retire bodies and proofs.
import { defineDoc,defineDocFamily,type JsonObject,type Tx,type ConversationId,type TaskId,type SubmissionId,type EntryId } from '@earendil-works/pi-durable';
import { createHash } from 'node:crypto';
import { fenceLegacyDoc,hasLegacyFence,type Result } from './state.ts';
export type Carrier={kind:'tool';callId:string}|{kind:'input';text?:string};
export interface Page {id:string;epoch:string;lead:ConversationId;run:SubmissionId;from:number;through:number;body?:string;digest:string;carrier:Carrier;state:'reserved'|'presented'|'released'|'acknowledged'|'replaced';scanThrough?:EntryId;submissionId?:SubmissionId;notifierId?:TaskId;pruned?:boolean}
export interface Proof {generation:TaskId;lead:ConversationId;run:SubmissionId;pages:string[];responseDigest?:string}
export interface Observations {pages:Record<string,Page>;proofs:Record<string,Proof>;acknowledged:number;epoch?:string;paused?:boolean;resumedOwnerRun?:SubmissionId}
export const ObservationDoc=defineDoc<JsonObject>({kind:'ptb.observation',version:2,migrate:value=>value,scope:'session',initial:()=>({pages:{},proofs:{},acknowledged:0}),checkpointWhen:()=>true});
interface Head {initialized:boolean;legacyFenced?:boolean;epoch?:string;acknowledged:number;paused?:boolean;resumedOwnerRun?:SubmissionId;ordinal:number;pageTail?:string;openPages:Record<string,number>;releasedPages:Record<string,number>;waitingAck:Record<string,number>;prunePages:Record<string,number>;openProofs:Record<string,true>;ackByEpoch:Record<string,number>;scanByEpoch:Record<string,EntryId>;acknowledgedScanThrough?:EntryId}
export const ObservationHeadDoc=defineDoc<JsonObject>({kind:'ptb.observation.head',version:1,scope:'session',initial:()=>({initialized:false,acknowledged:0,ordinal:0,openPages:{},releasedPages:{},waitingAck:{},prunePages:{},openProofs:{},ackByEpoch:{},scanByEpoch:{}}),checkpointWhen:()=>true});
const family=(kind:string)=>defineDocFamily<JsonObject,null>({kind,version:1,scope:'session',family:true,initial:()=>({}),checkpointWhen:()=>true});
export const PageDoc=family('ptb.observation.page'),PageBodyDoc=family('ptb.observation.body'),ProofDoc=family('ptb.observation.proof');
export interface ObservationStore {tx:Tx;head:Head}
const transactions=new WeakMap<Tx,ObservationStore>();
export const digest=(value:unknown)=>createHash('sha256').update(typeof value==='string'?value:JSON.stringify(value)).digest('hex');
const copyPage=(page:Page):Page=>({...page,carrier:{...page.carrier}});
const copyProof=(proof:Proof):Proof=>({...proof,pages:[...proof.pages]});
export function startObservationEpoch(s:ObservationStore,epoch:string){
 const h=s.head;if(h.epoch===epoch)return;
 if(h.epoch)h.ackByEpoch[h.epoch]=h.acknowledged;
 h.epoch=epoch;h.acknowledged=h.ackByEpoch[epoch]??0;delete h.acknowledgedScanThrough;h.paused=false;
}
export async function openObservations(tx:Tx):Promise<ObservationStore>{
 const cached=transactions.get(tx);if(cached)return cached;
 const head=await tx.doc(ObservationHeadDoc) as unknown as Head,s={tx,head};transactions.set(tx,s);
 if(!head.initialized){
  const old=await tx.doc(ObservationDoc) as unknown as Observations;
  if(hasLegacyFence(old))throw new Error('Observation index is missing after authority cutover.');
  if(old.epoch){head.epoch=old.epoch;head.ackByEpoch[old.epoch]=old.acknowledged;}head.acknowledged=old.acknowledged;
  if(old.paused!==undefined)head.paused=old.paused;
  if(old.resumedOwnerRun!==undefined)head.resumedOwnerRun=old.resumedOwnerRun;
  for(const value of Object.values(old.pages)){
   const page=copyPage(value);
   if(typeof page.body!=='string')throw new Error('Legacy observation page body is missing.');
   if(page.carrier.kind==='input'){
    const prefix=`${page.epoch}:notify:`,suffix=page.id.startsWith(prefix)?page.id.slice(prefix.length).split(':'):[];
    if(suffix.length===2&&/^\d+$/.test(suffix[0])&&/^\d+$/.test(suffix[1]))page.notifierId=Number(suffix[0]) as TaskId;
   }
   await createPage(s,page);
  }
  for(const value of Object.values(old.proofs)){
   const {generation,lead,run,pages,responseDigest}=value;
   if(pages.length)await saveProof(s,{generation,lead,run,pages:[...pages],...(responseDigest?{responseDigest}:{})});
  }
  await advanceAcknowledged(s);
  if(old.epoch){head.ackByEpoch[old.epoch]=Math.max(head.ackByEpoch[old.epoch]??0,old.acknowledged);head.acknowledged=head.ackByEpoch[old.epoch];}
  head.initialized=true;
 }
 if(!head.legacyFenced){fenceLegacyDoc(await tx.doc(ObservationDoc));head.legacyFenced=true;}
 return s;
}
export async function page(s:ObservationStore,id:string,withBody=false):Promise<Page|undefined>{
 const record=await s.tx.doc(PageDoc,id,null) as unknown as {value?:Page};if(!record.value)return;
 const value=copyPage(record.value);
 if(withBody&&!value.pruned){
  const body=await s.tx.doc(PageBodyDoc,id,null) as unknown as {body?:string;carrier?:Carrier};
  if(typeof body.body!=='string'||!body.carrier)throw new Error('Observation page body is missing.');
  value.body=body.body;value.carrier={...body.carrier};
 }
 return value;
}
export async function pages(s:ObservationStore,index:'openPages'|'releasedPages'|'waitingAck'|'prunePages',withBody=false):Promise<Page[]>{
 const ids=Object.entries(s.head[index]).sort((a,b)=>a[1]-b[1]).map(([id])=>id);
 return (await Promise.all(ids.map(id=>page(s,id,withBody)))).filter((p):p is Page=>!!p);
}
export async function savePage(s:ObservationStore,value:Page){
 const doc=await s.tx.doc(PageDoc,value.id,null),h=s.head;
 if(!doc.value){if(h.pageTail)doc.previous=h.pageTail;doc.order=++h.ordinal;h.pageTail=value.id;}
 const {body:_body,carrier,...meta}=value;
 doc.value={...meta,carrier:carrier.kind==='input'?{kind:'input'}:{...carrier}} as unknown as JsonObject;
 const order=Number(doc.order);
 for(const name of ['openPages','releasedPages','waitingAck','prunePages'] as const)delete h[name][value.id];
 if(value.state==='reserved'||value.state==='presented')h.openPages[value.id]=order;
 if(value.state==='released')h.releasedPages[value.id]=order;
 if(value.state==='acknowledged'){
  if(value.through>(h.ackByEpoch[value.epoch]??0))h.waitingAck[value.id]=order;
  if(value.scanThrough!==undefined){
   h.scanByEpoch[value.epoch]=Math.max(h.scanByEpoch[value.epoch]??0,value.scanThrough) as EntryId;
   if(value.epoch===h.epoch&&value.through<=(h.ackByEpoch[value.epoch]??0))h.acknowledgedScanThrough=Math.max(h.acknowledgedScanThrough??0,value.scanThrough) as EntryId;
  }
 }
 if(!value.pruned&&(value.state==='acknowledged'||value.state==='replaced'))h.prunePages[value.id]=order;
}
async function createPage(s:ObservationStore,value:Page){
 const doc=await s.tx.doc(PageBodyDoc,value.id,null);doc.body=value.body!;doc.carrier={...value.carrier};
 await savePage(s,value);
}
export async function reserve(s:ObservationStore,input:Omit<Page,'body'|'digest'|'state'|'pruned'>,result:Result):Promise<Page>{
 startObservationEpoch(s,input.epoch);
 const prior=await page(s,input.id,true);if(prior)return prior;
 const body=JSON.stringify(result),value:Page={...input,body,digest:digest(body),state:'reserved'};
 await createPage(s,value);return value;
}
export async function reservationHead(s:ObservationStore,epoch:string,run:SubmissionId):Promise<number>{
 return Math.max(s.head.epoch===epoch?s.head.acknowledged:0,...(await pages(s,'openPages')).filter(p=>p.epoch===epoch&&p.run===run).map(p=>p.through));
}
export async function advanceAcknowledged(s:ObservationStore){
 const waiting=await pages(s,'waitingAck'),h=s.head;
 for(let advanced=true;advanced;){
  advanced=false;
  for(const p of waiting){const cursor=h.ackByEpoch[p.epoch]??0;if(p.from<=cursor&&p.through>cursor){h.ackByEpoch[p.epoch]=p.through;advanced=true;}}
 }
 // Fold newly reachable anchors before removing their pages from the waiting index.
 for(const p of waiting)if(p.through<=(h.ackByEpoch[p.epoch]??0)){
  if(p.epoch===h.epoch&&p.scanThrough!==undefined)h.acknowledgedScanThrough=Math.max(h.acknowledgedScanThrough??0,p.scanThrough) as EntryId;
  delete h.waitingAck[p.id];
 }
 if(h.epoch)h.acknowledged=h.ackByEpoch[h.epoch]??0;
}
export async function proof(s:ObservationStore,id:string):Promise<Proof|undefined>{
 if(!s.head.openProofs[id])return;
 const value=(await s.tx.doc(ProofDoc,id,null)).value as unknown as Proof|undefined;
 return value?copyProof(value):undefined;
}
export async function openProofs(s:ObservationStore):Promise<Proof[]>{
 return (await Promise.all(Object.keys(s.head.openProofs).map(id=>proof(s,id)))).filter((p):p is Proof=>!!p);
}
export async function saveProof(s:ObservationStore,value:Proof){
 (await s.tx.doc(ProofDoc,String(value.generation),null)).value=copyProof(value) as unknown as JsonObject;
 s.head.openProofs[String(value.generation)]=true;
}
export async function retireProof(s:ObservationStore,id:string){
 if(!s.head.openProofs[id])return;delete s.head.openProofs[id];await s.tx.retireDoc(ProofDoc,id);
}
export async function retireBody(s:ObservationStore,value:Page){
 await s.tx.retireDoc(PageBodyDoc,value.id);value.pruned=true;delete value.body;
 if(value.carrier.kind==='input')delete value.carrier.text;
 await savePage(s,value);
}
/** Diagnostic UTF-8 JSON bytes. Excludes native record metadata and history. */
export async function observationStats(tx:Tx){
 const s=await openObservations(tx),bytes=(value:unknown)=>Buffer.byteLength(JSON.stringify(value));
 const result={pages:{count:0,currentRecordBytes:0},bodies:{count:0,currentRecordBytes:0},openProofs:{count:0,currentRecordBytes:0},headBytes:bytes(s.head),totalBytes:0};
 let id=s.head.pageTail;
 while(id){
  const record=await tx.doc(PageDoc,id,null),value=record.value as unknown as Page;
  result.pages.count++;result.pages.currentRecordBytes+=bytes(record);
  if(!value.pruned){result.bodies.count++;result.bodies.currentRecordBytes+=bytes(await tx.doc(PageBodyDoc,id,null));}
  id=record.previous as string|undefined;
 }
 for(const id of Object.keys(s.head.openProofs)){result.openProofs.count++;result.openProofs.currentRecordBytes+=bytes(await tx.doc(ProofDoc,id,null));}
 result.totalBytes=result.headBytes+result.pages.currentRecordBytes+result.bodies.currentRecordBytes+result.openProofs.currentRecordBytes;
 return result;
}

/** Explicit diagnostic projection. Runtime paths read the current indexes. */
export async function observations(tx:Tx):Promise<Observations>{
 const s=await openObservations(tx),all:Page[]=[];let id=s.head.pageTail;
 while(id){const record=await tx.doc(PageDoc,id,null);const value=await page(s,id,true);if(value)all.push(value);id=record.previous as string|undefined;}
 return {pages:Object.fromEntries(all.reverse().map(p=>[p.id,p])),proofs:Object.fromEntries((await openProofs(s)).map(p=>[String(p.generation),p])),acknowledged:s.head.acknowledged,...(s.head.epoch?{epoch:s.head.epoch}:{}),...(s.head.paused===undefined?{}:{paused:s.head.paused}),...(s.head.resumedOwnerRun===undefined?{}:{resumedOwnerRun:s.head.resumedOwnerRun})};
}

/** Owner reads use only pages at or below the acknowledged cursor. This read never advances it. */
export async function readObservationCursor(tx:Tx,epoch:string):Promise<{acknowledged:number;scanThrough?:EntryId}>{
 const head=await tx.doc(ObservationHeadDoc) as unknown as Head;
 return head.epoch===epoch?{acknowledged:head.acknowledged,...(head.acknowledgedScanThrough===undefined?{}:{scanThrough:head.acknowledgedScanThrough})}:{acknowledged:0};
}
