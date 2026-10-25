import { withAbortSignal,withCancel,awaitWithContext } from '@earendil-works/chord/context';
import type { AbortRun,InterruptTarget } from './interrupt.ts';
import { defineTask,LiveDoc,type TaskId,type SubmissionId,type Tx } from '@earendil-works/pi-durable';
import { plain,event,type Obligation } from './state.ts';
import { openAuthority as open,flushAuthority,markScheduled,unscheduledObligations,taskRecord,alertRecord,obligation,setObligationState,type Authority as State } from './store.ts';

/** `kind` and `sender` name the delivery in its queue entry; inputs persisted before they existed lack them. */
export interface DeliveryInput extends InterruptTarget { interrupt?:true; interruptAt?:'delivery'; epoch:string; content:string; whenBusy:'steer'|'followUp'; kind?:Obligation['kind']; sender?:string }
/**
 * The structured data of a Team input's `team.notice` entry. A reader names the input from these fields, never from
 * its text: `delivery` is the kind (`update` for a notifier page), `sender` the author, at most SENDER_LIMIT characters.
 * `interrupt` marks an Alert sent to interrupt the recipient's run. `interruptedRun` identifies a confirmed aborted run.
 * The interruption outcome stays in the model-facing content.
 * Entries written before `delivery` existed carry only `source` and `deliveryId` or `pageId`.
 */
export type TeamNoticeData={source:'team';epoch:string;deliveryId?:string;pageId?:string;delivery?:Obligation['kind']|'update';sender?:string;interrupt?:true;interruptedRun?:TaskId};
export const SENDER_LIMIT=64;
export async function eligible(s:State,o:Obligation):Promise<boolean> {
 const team=s.team.current;
 if(!team||team.epoch!==o.epoch||team.lifecycle!=='active'||team.shutdownPending||o.state!=='pending')return false;
 if(o.recipient==='team-lead')return team.lead===o.conversationId;
 const worker=Object.hasOwn(team.workers,o.recipient)?team.workers[o.recipient]:undefined;
 if(!worker||worker.state!=='active'||worker.generation!==o.generation||worker.conversationId!==o.conversationId)return false;
 if(o.kind!=='task')return true;
 if(!s.head.graph||s.head.epoch!==team.epoch)return false;
 const task=(await taskRecord(s,o.taskId!))?.card;
 return !!task&&task.version===o.version&&task.assignee===o.recipient&&task.status==='ready';
}
export async function deliveryInput(s:State,o:Obligation):Promise<DeliveryInput> {
 let payload:unknown;
 if(o.kind==='alert')payload={kind:'alert',alert:plain(await alertRecord(s,o.alertId!))};
 else if(o.kind==='stop')payload={kind:'task_changed',task_id:o.taskId,message:'The previous Attempt was superseded or cancelled. Read current Task authority before continuing.'};
 else {
  payload={kind:'task',task:plain((await taskRecord(s,o.taskId!))?.card)};
 }
 // An Alert names its author; durable-team sends Tasks and stops on the Team authority's behalf, as team-lead.
 const sender=(o.kind==='alert'?(await alertRecord(s,o.alertId!))?.from:'team-lead')?.slice(0,SENDER_LIMIT);
 return {key:o.key,epoch:o.epoch,conversationId:o.conversationId,...(o.interrupt?{interrupt:true as const,...(o.interruptAt?{interruptAt:o.interruptAt}:{}),...(o.interruptRun!==undefined?{expectedRunTaskId:o.interruptRun}:{})}:{}),content:JSON.stringify(payload),whenBusy:o.kind==='task'?'followUp':'steer',kind:o.kind,...(sender?{sender}:{})};
}
/** The input a delivery submits. The request ID names the Obligation, so a retry reacquires the same admission. */
export function deliveryDraft(input:DeliveryInput,interruption?:Obligation['interruption'],interruptedRun?:TaskId){
 const data:TeamNoticeData={source:'team',deliveryId:input.key,epoch:input.epoch,...(input.kind?{delivery:input.kind}:{}),...(input.sender?{sender:input.sender}:{}),...(input.interrupt&&input.kind==='alert'?{interrupt:true as const}:{}),...(interruptedRun!==undefined?{interruptedRun}:{})};
 const content=interruption?JSON.stringify({...JSON.parse(input.content),interruption:{outcome:interruption,message:interruption==='interrupted'?(input.interruptAt==='delivery'?'A run in progress was interrupted to deliver this Alert. Its last input may be unanswered; read it before continuing.':'The captured run was interrupted.'):interruption==='ended'?(input.interruptAt==='delivery'?'The run selected for this Alert already ended or changed.':'The captured run already ended or changed.'):'Interruption was not proved; this Alert uses normal delivery.'}}):input.content;
 return {type:'input' as const,content,whenBusy:input.whenBusy,requestId:`ptb:${input.key}`,entry:{kind:'team.notice',data}};
}
const draft=deliveryDraft;

export const INTERRUPT_SETTLE_TIMEOUT_MS=10_000;
type InterruptCheckpoint={phase:'interrupt';selection?:{run?:TaskId;deadline:number}};
type DeliveryCheckpoint=InterruptCheckpoint|{phase:'submit'}|{phase:'settle';submissionId:SubmissionId};
export const Deliver=defineTask<DeliveryInput,DeliveryCheckpoint,null>({
 name:'ptb.deliver',version:2,initial:input=>({phase:input.interrupt?'interrupt':'submit'}),
 migrate:(input,checkpoint,fromVersion)=>{
  if(fromVersion!==1)throw new Error('PTB delivery migration supports only version 1');
  return {input:input as unknown as DeliveryInput,checkpoint:checkpoint as DeliveryCheckpoint};
 },
 phases:{
  interrupt:async()=>{throw new Error('The host must supply the run-only interrupt port');},
  submit:async(task,runtime,context)=>{
   let valid=false,interruption:Obligation['interruption'],interruptedRun:TaskId|undefined;
   await runtime.commit(async tx=>{
    const s=await open(tx),o=await obligation(s,task.input.key);
    valid=!!o&&await eligible(s,o);interruption=o?.interruption;interruptedRun=o?.interruptedRun;
    if(!valid){if(o)await setObligationState(s,o,'retired');flushAuthority(s);return {status:'terminal',outcome:{status:'completed',result:null}};}
    return undefined;
   },context);
   if(!valid)return;
   const target=await runtime.conversation(task.input.conversationId,context);
   if(!target)throw new Error('PTB delivery recipient is missing');
   const submission=await target.submit(draft(task.input,interruption,interruptedRun),context);
   await runtime.commit(async tx=>{
    const s=await open(tx),o=await obligation(s,task.input.key);
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
   let interruption:Obligation['interruption'],interruptedRun:TaskId|undefined;
   await runtime.commit(async tx=>{const o=await obligation(await open(tx),task.input.key);interruption=o?.interruption;interruptedRun=o?.interruptedRun;return undefined;},context);
   const submission=await target.submit(draft(task.input,interruption,interruptedRun),context);
   if(submission.id!==task.state.checkpoint.submissionId)throw new Error('PTB admission identity changed');
   let obsolete=false;
   await runtime.commit(async tx=>{const s=await open(tx),o=await obligation(s,task.input.key);obsolete=!o||!await eligible(s,o);return undefined;},context);
   if(obsolete)await submission.abort(context);
   const settled=await submission.wait(context);
   await runtime.commit(async tx=>{
    const s=await open(tx),o=await obligation(s,task.input.key);
    if(o){o.settledAt=runtime.now();o.outcome=settled.status==='done'?'answered':`unanswered:${settled.reason}`;if(o.state==='pending')await setObligationState(s,o,'settled');}
    if(s.team.current?.epoch===task.input.epoch)await event(tx,s,'authority','delivery_settled',{at:new Date(runtime.now()).toISOString()});
    return {status:'terminal',outcome:{status:'completed',result:null}};
   },context);
  },
 },
 abort:async(task,runtime,context)=>{
  await runtime.commit(async tx=>{const s=await open(tx),o=await obligation(s,task.input.key);if(o){await setObligationState(s,o,'retired');o.outcome='stopped';}flushAuthority(s);return {status:'terminal',outcome:{status:'aborted'}};},context);
 },
});

/** Keep the shared submit/settle mechanics; only the host-owned interruption needs a port. */
export function createDeliver(abortRun:AbortRun){
 return defineTask({...Deliver.definition,phases:{
  ...Deliver.definition.phases,
  submit:(task,runtime,context)=>Deliver.definition.phases.submit(task,runtime,context),
  interrupt:async(task,runtime,context)=>{
   const bound=withAbortSignal(runtime.signal,context);
   const selection=task.state.checkpoint.selection;
   if(!selection)return runtime.commit(async tx=>{
    const s=await open(tx),o=await obligation(s,task.input.key);
    if(!o||!await eligible(s,o)){if(o)await setObligationState(s,o,'retired');flushAuthority(s);return {status:'terminal',outcome:{status:'completed',result:null}};}
    // Absence of the marker is a recorded P choice, including an idle recipient. Never reinterpret it.
    const run=task.input.interruptAt==='delivery'?(await tx.doc(LiveDoc,task.input.conversationId)).run?.taskId:task.input.expectedRunTaskId;
    if(task.input.interruptAt==='delivery'){if(run===undefined)delete o.interruptRun;else o.interruptRun=run;}
    return {status:'running',checkpoint:{phase:'interrupt',selection:{...(run!==undefined?{run}:{}),deadline:runtime.now()+INTERRUPT_SETTLE_TIMEOUT_MS}}};
   },bound);
   let outcome:Obligation['interruption']|'retired';
   const remaining=selection.deadline-runtime.now();
   if(remaining<=0)outcome=selection.run===undefined?'ended':'failed';
   else{
    const wait=withCancel(bound),stop=runtime.setTimer(()=>wait.cancel(new Error('Interrupt settlement deadline reached')),remaining);
    try{outcome=await awaitWithContext(abortRun(runtime,{...task.input,expectedRunTaskId:selection.run},wait.context),wait.context);}
    catch(error){if(runtime.signal.aborted)throw error;runtime.report(error);outcome='failed';}
    finally{stop();wait.cancel();}
   }
   await runtime.commit(async tx=>{
    const s=await open(tx),o=await obligation(s,task.input.key);
    if(!o||!await eligible(s,o)||outcome==='retired'){
     if(o)await setObligationState(s,o,'retired');flushAuthority(s);return {status:'terminal',outcome:{status:'completed',result:null}};
    }
    if(task.input.interruptAt==='delivery'&&selection.run===undefined&&outcome==='ended')delete o.interruption;else o.interruption=outcome;
    if(task.input.interruptAt==='delivery'&&selection.run!==undefined&&outcome==='interrupted')o.interruptedRun=selection.run;
    return {status:'running',checkpoint:{phase:'submit'}};
   },bound);
  },
 }});
}
export async function scheduleDeliveries(tx:Tx,s:State,delivery:typeof Deliver):Promise<void> {
 const team=s.team.current;if(!team||team.lifecycle!=='active')return;
 for(const o of await unscheduledObligations(s)){
  const key=o.key;
  if(o.deliveryTaskId){await markScheduled(s,key);continue;}
  if(!await eligible(s,o))continue;
  o.deliveryTaskId=await tx.createTask(delivery,await deliveryInput(s,o),{conversationId:team.lead,ownership:{kind:'conversation'},background:true,abandonOnRestart:false});
  await markScheduled(s,key);
 }
}
