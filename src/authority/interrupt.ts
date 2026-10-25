// Interrupt only the run captured at Alert acceptance. Never withdraw or re-admit queued submissions.
import type { Context } from '@earendil-works/chord/context';
import { LiveDoc,type Harness,type TaskId,type ConversationId,type DocumentReader } from '@earendil-works/pi-durable';
import { openAuthority as open,obligation } from './store.ts';
import { eligible } from './delivery.ts';

export interface InterruptTarget { key:string; conversationId:ConversationId; expectedRunTaskId?:TaskId }
export type InterruptOutcome='interrupted'|'ended'|'failed'|'retired';
export type AbortRun=(api:Pick<DocumentReader,'snapshot'>,target:InterruptTarget,context:Context)=>Promise<InterruptOutcome>;

/** Host primitive: exact native identity, cancellable wait, and no conversation-wide abort. */
export async function abortExpectedRun(harness:Harness,target:InterruptTarget,context:Context):Promise<InterruptOutcome>{
 const check=await harness.commit(async tx=>{
  const s=await open(tx),o=await obligation(s,target.key);
  if(!o||!await eligible(s,o)||o.conversationId!==target.conversationId||o.interruptRun!==target.expectedRunTaskId)return 'retired';
  if(target.expectedRunTaskId===undefined)return 'ended';
  const task=await tx.task(target.expectedRunTaskId),live=await tx.doc(LiveDoc,target.conversationId);
  if(!task||task.conversationId!==target.conversationId)return 'failed';
  if(task.state.status==='terminal')return task.state.outcome.status==='aborted'?'interrupted':'ended';
  if(live.run?.taskId!==target.expectedRunTaskId)return 'ended';
  return 'abort';
 },context);
 if(check!=='abort')return check;
 await harness.abortTask(target.expectedRunTaskId!,context);
 const task=await harness.waitForTask(target.expectedRunTaskId!,context);
 return task.state.outcome.status==='aborted'?'interrupted':'failed';
}
