import { defineTask,LiveDoc,type ConversationId,type SubmissionId,type Tx } from '@earendil-works/pi-durable';
import { openObservations,reserve,pages,page as loadPage,savePage,type Page } from './observation-store.ts';
import { type ModelRoleCatalog } from './state.ts';
import { openAuthority as open } from './store.ts';
import { snapshotResult } from './projection.ts';
import { pageEvents,updates } from './sync.ts';
import type { TeamNoticeData } from './delivery.ts';
export const NOTIFY_RECHECK_MS=1000;
export const NOTIFY_DELAY_MS=30000;
/** The input a notifier page submits to the lead: a Team update that durable-team itself writes. */
export function pageDraft(page:Page){
 const data:TeamNoticeData={source:'team',pageId:page.id,epoch:page.epoch,delivery:'update',sender:'durable-team'};
 const content=page.carrier.kind==='input'?page.carrier.text:page.body;if(content===undefined)throw new Error('PTB notifier page body is missing');
 return {type:'input' as const,content,whenBusy:'followUp' as const,requestId:page.id,entry:{kind:'team.notice',data}};
}
const draft=pageDraft;

export function createNotifier(rolesOf:(tx:Tx,lead:ConversationId)=>Promise<ModelRoleCatalog>){
 return defineTask<{epoch:string},{phase:'tick';ordinal:number;due?:number}|{phase:'submit';pageId:string;ordinal:number}|{phase:'settle';pageId:string;ordinal:number},null>({
  name:'ptb.notify',version:1,initial:()=>({phase:'tick',ordinal:1}),
  phases:{
   tick:async(task,runtime,context)=>{
    await runtime.commit(async tx=>{
     const s=await open(tx),team=s.team.current,o=await openObservations(tx);
     if(!team||team.epoch!==task.input.epoch||team.lifecycle!=='active'||team.shutdownPending)return {status:'terminal',outcome:{status:'completed',result:null}};
     const live=await tx.doc(LiveDoc,team.lead);
     const pending=(await pages(o,'openPages')).some(p=>p.epoch===team.epoch);
     const checkpoint=task.state.checkpoint,due=checkpoint.due??runtime.now()+NOTIFY_DELAY_MS;
     if(o.head.paused||pending||live.run||s.journal.seq<=o.head.acknowledged||runtime.now()<due)return {status:'sleeping',until:runtime.now()+NOTIFY_RECHECK_MS,checkpoint:{...checkpoint,due}};
     const from=o.head.epoch===team.epoch?o.head.acknowledged:0;
     const previous=o.head.scanByEpoch[team.epoch];
     const change=from?await pageEvents(tx,s,from,previous):undefined;
     const roles=change?undefined:await rolesOf(tx,team.lead);
     const result=change?await updates(s,change.events):await snapshotResult(s,roles!.modelRoles,roles!.defaultModelRole);
     const id=`${team.epoch}:notify:${runtime.taskId}:${checkpoint.ordinal}`;
     const text=JSON.stringify({kind:'team_update',page_id:id,result});
     const scanThrough=change?.scanThrough??s.journal.events.at(-1)?.entryId;
     await reserve(o,{id,epoch:team.epoch,lead:team.lead,run:0 as SubmissionId,from,through:change?change.events.at(-1)?.seq??from:s.journal.seq,carrier:{kind:'input',text},notifierId:runtime.taskId,...(scanThrough?{scanThrough}:{})},result);
     return {status:'running',checkpoint:{phase:'submit',pageId:id,ordinal:checkpoint.ordinal}};
    },context);
   },
   submit:async(task,runtime,context)=>{
    let page:Page|undefined;
    await runtime.commit(async tx=>{const s=await open(tx),o=await openObservations(tx),p=await loadPage(o,task.state.checkpoint.pageId,true);
     if(!p||s.team.current?.epoch!==p.epoch||s.team.current?.shutdownPending||s.team.current?.lifecycle!=='active')return {status:'terminal',outcome:{status:'completed',result:null}};
     page=p;return undefined;
    },context);
    if(!page)return;
    const target=await runtime.conversation(page.lead,context);if(!target)throw new Error('PTB lead is missing');
    const submission=await target.submit(draft(page),context);
    await runtime.commit(async tx=>{const o=await openObservations(tx),p=(await loadPage(o,page!.id))!;p.submissionId=submission.id;await savePage(o,p);return {status:'running',checkpoint:{...task.state.checkpoint,phase:'settle'}};},context);
   },
   settle:async(task,runtime,context)=>{
    let page!:Page;await runtime.commit(async tx=>{page=(await loadPage(await openObservations(tx),task.state.checkpoint.pageId,true))!;return undefined;},context);
    const target=await runtime.conversation(page.lead,context);if(!target)throw new Error('PTB lead is missing');
    const submission=await target.submit(draft(page),context);
    if(submission.id!==page.submissionId)throw new Error('PTB notifier admission identity changed');
    const settled=await submission.wait(context);
    await runtime.commit(async tx=>{
     const o=await openObservations(tx),p=(await loadPage(o,page.id))!;
     if(p.run===0)p.run=submission.id;
     if(settled.status!=='done'){p.state='released';o.head.paused=true;}
     await savePage(o,p);
     return {status:'sleeping',until:runtime.now()+NOTIFY_RECHECK_MS,checkpoint:{phase:'tick',ordinal:task.state.checkpoint.ordinal+1}};
    },context);
   },
  },
  abort:async(task,runtime,context)=>runtime.commit(async tx=>{const o=await openObservations(tx);for(const p of await pages(o,'openPages'))if(p.epoch===task.input.epoch&&p.carrier.kind==='input'){p.state='released';await savePage(o,p);}return {status:'terminal',outcome:{status:'aborted'}};},context),
 });
}
