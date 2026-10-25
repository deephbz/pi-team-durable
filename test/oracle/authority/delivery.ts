import { withAbortSignal } from '@earendil-works/chord/context';
import type { AbortRun,InterruptTarget } from './interrupt.ts';
import { defineTask, type SubmissionId, type Tx } from '@earendil-works/pi-durable';
import { GraphTaskController } from './graph-control.ts';
import { projectGraphTaskCards } from './graph-card.ts';
import { open,plain,event,type State,type Obligation } from './state.ts';

/** `kind` and `sender` name the delivery in its queue entry; inputs persisted before they existed lack them. */
export interface DeliveryInput extends InterruptTarget { interrupt?:true; epoch:string; content:string; whenBusy:'steer'|'followUp'; kind?:Obligation['kind']; sender?:string }
/**
 * The structured data of a Team input's `team.notice` entry. A reader names the input from these fields, never from
 * its text: `delivery` is the kind (`update` for a notifier page), `sender` the author, at most SENDER_LIMIT characters.
 * `interrupt` marks an Alert sent to interrupt the recipient's run; the interruption outcome stays in the model-facing content.
 * Entries written before `delivery` existed carry only `source` and `deliveryId` or `pageId`.
 */
export type TeamNoticeData={source:'team';epoch:string;deliveryId?:string;pageId?:string;delivery?:Obligation['kind']|'update';sender?:string;interrupt?:true};
export const SENDER_LIMIT=64;
export function eligible(s:State,o:Obligation):boolean {
 const team=s.team.current;
 if(!team||team.epoch!==o.epoch||team.lifecycle!=='active'||team.shutdownPending||o.state!=='pending')return false;
 if(o.recipient==='team-lead')return team.lead===o.conversationId;
 const worker=Object.hasOwn(team.workers,o.recipient)?team.workers[o.recipient]:undefined;
 if(!worker||worker.state!=='active'||worker.generation!==o.generation||worker.conversationId!==o.conversationId)return false;
 if(o.kind!=='task')return true;
 if(!s.graph.snapshot||s.graph.epoch!==team.epoch)return false;
 const task=GraphTaskController.recover(plain(s.graph.snapshot)).readTasks().find(t=>t.id===o.taskId);
 return !!task&&task.version===o.version&&task.assignee===o.recipient&&task.state.kind==='ready';
}
export function deliveryInput(s:State,o:Obligation):DeliveryInput {
 let payload:unknown;
 if(o.kind==='alert')payload={kind:'alert',alert:plain(s.outbox.alerts[o.alertId!])};
 else if(o.kind==='stop')payload={kind:'task_changed',task_id:o.taskId,message:'The previous Attempt was superseded or cancelled. Read current Task authority before continuing.'};
 else {
  const graph=GraphTaskController.recover(plain(s.graph.snapshot!));
  payload={kind:'task',task:projectGraphTaskCards(graph).find(t=>t.id===o.taskId)};
 }
 // An Alert names its author; durable-team sends Tasks and stops on the Team authority's behalf, as team-lead.
 const sender=(o.kind==='alert'?s.outbox.alerts[o.alertId!]?.from:'team-lead')?.slice(0,SENDER_LIMIT);
 return {key:o.key,epoch:o.epoch,conversationId:o.conversationId,...(o.interrupt?{interrupt:true as const,...(o.interruptRun!==undefined?{expectedRunTaskId:o.interruptRun}:{})}:{}),content:JSON.stringify(payload),whenBusy:o.kind==='task'?'followUp':'steer',kind:o.kind,...(sender?{sender}:{})};
}
/** The input a delivery submits. The request ID names the Obligation, so a retry reacquires the same admission. */
export function deliveryDraft(input:DeliveryInput,interruption?:Obligation['interruption']){
 const data:TeamNoticeData={source:'team',deliveryId:input.key,epoch:input.epoch,...(input.kind?{delivery:input.kind}:{}),...(input.sender?{sender:input.sender}:{}),...(input.interrupt&&input.kind==='alert'?{interrupt:true as const}:{})};
 const content=interruption?JSON.stringify({...JSON.parse(input.content),interruption:{outcome:interruption,message:interruption==='interrupted'?'The captured run was interrupted.':interruption==='ended'?'The captured run already ended or changed.':'Interruption was not proved; this Alert uses normal delivery.'}}):input.content;
 return {type:'input' as const,content,whenBusy:input.whenBusy,requestId:`ptb:${input.key}`,entry:{kind:'team.notice',data}};
}
const draft=deliveryDraft;

export const Deliver=defineTask<DeliveryInput,{phase:'interrupt'}|{phase:'submit'}|{phase:'settle';submissionId:SubmissionId},null>({
 name:'ptb.deliver',version:1,initial:input=>({phase:input.interrupt?'interrupt':'submit'}),
 phases:{
  interrupt:async()=>{throw new Error('The host must supply the run-only interrupt port');},
  submit:async(task,runtime,context)=>{
   let valid=false,interruption:Obligation['interruption'];
   await runtime.commit(async tx=>{
    const s=await open(tx),o=s.outbox.obligations[task.input.key];
    valid=!!o&&eligible(s,o);interruption=o?.interruption;
    if(!valid){if(o)o.state='retired';return {status:'terminal',outcome:{status:'completed',result:null}};}
    return undefined;
   },context);
   if(!valid)return;
   const target=await runtime.conversation(task.input.conversationId,context);
   if(!target)throw new Error('PTB delivery recipient is missing');
   const submission=await target.submit(draft(task.input,interruption),context);
   await runtime.commit(async tx=>{
    const s=await open(tx),o=s.outbox.obligations[task.input.key];
    if(!o)throw new Error('PTB delivery obligation is missing');
    o.submissionId=submission.id;o.submittedAt??=runtime.now();
    return {status:'running',checkpoint:{phase:'settle',submissionId:submission.id}};
   },context);
   // The settle phase rechecks revisions that raced with admission.
  },
  settle:async(task,runtime,context)=>{
   const target=await runtime.conversation(task.input.conversationId,context);
   if(!target)throw new Error('PTB admitted recipient is missing');
   // Same request ID reacquires the durable admission after a process interruption.
   let interruption:Obligation['interruption'];
   await runtime.commit(async tx=>{interruption=(await open(tx)).outbox.obligations[task.input.key]?.interruption;return undefined;},context);
   const submission=await target.submit(draft(task.input,interruption),context);
   if(submission.id!==task.state.checkpoint.submissionId)throw new Error('PTB admission identity changed');
   let obsolete=false;
   await runtime.commit(async tx=>{const s=await open(tx),o=s.outbox.obligations[task.input.key];obsolete=!o||!eligible(s,o);return undefined;},context);
   if(obsolete)await submission.abort(context);
   const settled=await submission.wait(context);
   await runtime.commit(async tx=>{
    const s=await open(tx),o=s.outbox.obligations[task.input.key];
    if(o){o.settledAt=runtime.now();o.outcome=settled.status==='done'?'answered':`unanswered:${settled.reason}`;if(o.state==='pending')o.state='settled';}
    if(s.team.current?.epoch===task.input.epoch)await event(tx,s,'authority','delivery_settled',{at:new Date(runtime.now()).toISOString()});
    return {status:'terminal',outcome:{status:'completed',result:null}};
   },context);
  },
 },
 abort:async(task,runtime,context)=>{
  await runtime.commit(async tx=>{const o=(await open(tx)).outbox.obligations[task.input.key];if(o){o.state='retired';o.outcome='stopped';}return {status:'terminal',outcome:{status:'aborted'}};},context);
 },
});

/** Keep the shared submit/settle mechanics; only the host-owned interruption needs a port. */
export function createDeliver(abortRun:AbortRun){
 return defineTask({...Deliver.definition,phases:{
  ...Deliver.definition.phases,
  submit:(task,runtime,context)=>Deliver.definition.phases.submit(task,runtime,context),
  interrupt:async(task,runtime,context)=>{
   const bound=withAbortSignal(runtime.signal,context);
   let outcome;
   try{outcome=await abortRun(runtime,task.input,bound);}
   catch(error){if(runtime.signal.aborted)throw error;runtime.report(error);outcome='failed' as const;}
   await runtime.commit(async tx=>{
    const s=await open(tx),o=s.outbox.obligations[task.input.key];
    if(!o||!eligible(s,o)||outcome==='retired'){
     if(o)o.state='retired';return {status:'terminal',outcome:{status:'completed',result:null}};
    }
    o.interruption=outcome;
    return {status:'running',checkpoint:{phase:'submit'}};
   },bound);
  },
 }});
}
export async function scheduleDeliveries(tx:Tx,s:State,delivery:typeof Deliver):Promise<void> {
 const team=s.team.current;if(!team||team.lifecycle!=='active')return;
 for(const o of Object.values(s.outbox.obligations)){
  if(o.deliveryTaskId||!eligible(s,o))continue;
  o.deliveryTaskId=await tx.createTask(delivery,deliveryInput(s,o),{conversationId:team.lead,ownership:{kind:'conversation'},background:true,abandonOnRestart:false});
 }
}
