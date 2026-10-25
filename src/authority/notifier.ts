import { defineTask,LiveDoc,type ConversationId,type SubmissionId,type Tx } from '@earendil-works/pi-durable';
import { observations,reserve,type Page } from './observation.ts';
import { open,plain,type State,type ModelRoleCatalog } from './state.ts';
import { snapshotResult } from './projection.ts';
import { pageEvents,updates } from './sync.ts';
import type { TeamNoticeData } from './delivery.ts';
export const NOTIFY_RECHECK_MS=1000;
export const NOTIFY_DELAY_MS=30000;
/** The input a notifier page submits to the lead: a Team update that durable-team itself writes. */
export function pageDraft(page:Page){
 const data:TeamNoticeData={source:'team',pageId:page.id,epoch:page.epoch,delivery:'update',sender:'durable-team'};
 return {type:'input' as const,content:page.carrier.kind==='input'?page.carrier.text:page.body,whenBusy:'followUp' as const,requestId:page.id,entry:{kind:'team.notice',data}};
}
const draft=pageDraft;

export function createNotifier(rolesOf:(tx:Tx,lead:ConversationId)=>Promise<ModelRoleCatalog>){
 return defineTask<{epoch:string},{phase:'tick';ordinal:number;due?:number}|{phase:'submit';pageId:string;ordinal:number}|{phase:'settle';pageId:string;ordinal:number},null>({
  name:'ptb.notify',version:1,initial:()=>({phase:'tick',ordinal:1}),
  phases:{
   tick:async(task,runtime,context)=>{
    await runtime.commit(async tx=>{
     const s=await open(tx),team=s.team.current,o=await observations(tx);
     if(!team||team.epoch!==task.input.epoch||team.lifecycle!=='active'||team.shutdownPending)return {status:'terminal',outcome:{status:'completed',result:null}};
     const live=await tx.doc(LiveDoc,team.lead);
     const pending=Object.values(o.pages).some(p=>p.epoch===team.epoch&&['reserved','presented'].includes(p.state));
     const checkpoint=task.state.checkpoint,due=checkpoint.due??runtime.now()+NOTIFY_DELAY_MS;
     if(o.paused||pending||live.run||s.journal.seq<=o.acknowledged||runtime.now()<due)return {status:'sleeping',until:runtime.now()+NOTIFY_RECHECK_MS,checkpoint:{...checkpoint,due}};
     const from=o.epoch===team.epoch?o.acknowledged:0;
     const previous=Object.values(o.pages).filter(p=>p.epoch===team.epoch&&p.state==='acknowledged'&&p.scanThrough!==undefined).sort((a,b)=>Number(b.scanThrough)-Number(a.scanThrough))[0];
     const change=from?await pageEvents(tx,s,from,previous?.scanThrough):undefined;
     const roles=change?undefined:await rolesOf(tx,team.lead);
     const result=change?updates(s,change.events):snapshotResult(s,roles!.modelRoles,roles!.defaultModelRole);
     const id=`${team.epoch}:notify:${runtime.taskId}:${checkpoint.ordinal}`;
     const text=JSON.stringify({kind:'team_update',page_id:id,result});
     const scanThrough=change?.scanThrough??s.journal.events.at(-1)?.entryId;
     reserve(o,{id,epoch:team.epoch,lead:team.lead,run:0 as SubmissionId,from,through:change?change.events.at(-1)?.seq??from:s.journal.seq,carrier:{kind:'input',text},...(scanThrough?{scanThrough}:{})},result);
     return {status:'running',checkpoint:{phase:'submit',pageId:id,ordinal:checkpoint.ordinal}};
    },context);
   },
   submit:async(task,runtime,context)=>{
    let page:Page|undefined;
    await runtime.commit(async tx=>{const s=await open(tx),o=await observations(tx),p=o.pages[task.state.checkpoint.pageId];
     if(!p||s.team.current?.epoch!==p.epoch||s.team.current?.shutdownPending||s.team.current?.lifecycle!=='active')return {status:'terminal',outcome:{status:'completed',result:null}};
     page=plain(p);return undefined;
    },context);
    if(!page)return;
    const target=await runtime.conversation(page.lead,context);if(!target)throw new Error('PTB lead is missing');
    const submission=await target.submit(draft(page),context);
    await runtime.commit(async tx=>{(await observations(tx)).pages[page!.id].submissionId=submission.id;return {status:'running',checkpoint:{...task.state.checkpoint,phase:'settle'}};},context);
   },
   settle:async(task,runtime,context)=>{
    let page!:Page;await runtime.commit(async tx=>{page=plain((await observations(tx)).pages[task.state.checkpoint.pageId]);return undefined;},context);
    const target=await runtime.conversation(page.lead,context);if(!target)throw new Error('PTB lead is missing');
    const submission=await target.submit(draft(page),context);
    if(submission.id!==page.submissionId)throw new Error('PTB notifier admission identity changed');
    const settled=await submission.wait(context);
    await runtime.commit(async tx=>{
     const o=await observations(tx),p=o.pages[page.id];
     if(p.run===0)p.run=submission.id;
     if(settled.status!=='done'){p.state='released';o.paused=true;}
     return {status:'sleeping',until:runtime.now()+NOTIFY_RECHECK_MS,checkpoint:{phase:'tick',ordinal:task.state.checkpoint.ordinal+1}};
    },context);
   },
  },
  abort:async(task,runtime,context)=>runtime.commit(async tx=>{const o=await observations(tx);for(const p of Object.values(o.pages))if(p.epoch===task.input.epoch&&p.carrier.kind==='input'&&['reserved','presented'].includes(p.state))p.state='released';return {status:'terminal',outcome:{status:'aborted'}};},context),
 });
}
