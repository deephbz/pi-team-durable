import { defineTask, type ConversationId, type TaskId, type Tx } from '@earendil-works/pi-durable';

// A live background root owns the Worker conversation. A terminal placeholder
// cannot be an abort boundary because abortTask returns early for terminal tasks.
export const ANCHOR_SLEEP_MS = 24 * 60 * 60 * 1000;
export const WorkerAnchor = defineTask<null, {phase:'hold'}, null>({
 name:'ptb.worker-anchor',version:1,initial:()=>({phase:'hold'}),
 phases:{hold:async(_task,runtime,context)=>{
  await runtime.sleepUntil(runtime.now()+ANCHOR_SLEEP_MS,{phase:'hold'},context);
 }},
 abort:async(_task,runtime,context)=>{
  await runtime.commit(()=>({status:'terminal',outcome:{status:'aborted'}}),context);
 },
});

/** The caller commits this binding with Membership in the same transaction. */
export async function createAnchoredConversation(tx:Tx,lead:ConversationId):Promise<{conversationId:ConversationId;anchorId:TaskId}> {
 const anchorId=await tx.createTask(WorkerAnchor,null,{conversationId:lead,ownership:{kind:'conversation'},background:true,abandonOnRestart:false});
 const conversation=await tx.createConversation({ownership:{kind:'task',taskId:anchorId}});
 return {conversationId:conversation.id,anchorId};
}
