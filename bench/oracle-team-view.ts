import { GraphTaskController, plain, type State } from '../test/oracle/authority/index.ts';

/** Native Team-panel projection. Authority and delivery facts remain separate. */
export function teamView(s:State|undefined,storeId:string) {
 const team=s?.team.current;if(!s||!team)return {team:null};
 const snapshot=s.graph.epoch===team.epoch?s.graph.snapshot:undefined;
 const graph=snapshot?GraphTaskController.recover(plain(snapshot)):undefined;
 const events=snapshot?.events??[];
 const tasks=(graph?.readTasks()??[]).map(t=>{
  const attempts=(graph?.readAttempts(t.id)??[]).map(a=>{
   const start=events.find(e=>e.kind==='attempt_started'&&e.attemptId===a.id);
   const end=events.find(e=>['attempt_completed','attempt_superseded','attempt_cancelled'].includes(e.kind)&&'attemptId' in e&&e.attemptId===a.id);
   return {id:a.id,ordinal:a.ordinal,...(start?.recorded_at?{startedAt:Date.parse(start.recorded_at)}:{}),...(end?.recorded_at?{endedAt:Date.parse(end.recorded_at)}:{}),...(a.outcome?{outcome:a.outcome}:{}),...(a.evidence?{evidence:a.evidence}:{})};
  });
  const evidence=t.state.kind==='blocked'?t.state.evidence:attempts.at(-1)?.evidence;
  return {key:t.id,title:t.title,goal:t.goal,assignee:t.assignee,needs:t.needs,version:t.version,current_context:t.currentContext,attempts,derived:t.state.kind,status:['ready','dependency_waiting'].includes(t.state.kind)?'pending':t.state.kind,...('reason' in t.state?{derivedReason:t.state.reason}:{}),...(evidence?{evidence}:{})};
 });
 return {
  team:{name:team.name,purpose:team.purpose,status:team.lifecycle,createdAt:team.createdAt,leadConversationId:String(team.lead)},
  leadThreadId:`${storeId}.${team.lead}`,graphVersion:snapshot?.graphRevisions.at(-1)?.version??'',tasks,
  workers:Object.values(team.workers).map(w=>({name:w.name,scope:w.scope,modelRole:w.role.name,createdAt:w.createdAt,threadId:`${storeId}.${w.conversationId}`,membership:{id:`${team.epoch}:${w.name}:g${w.generation}`,generation:w.generation,conversationId:String(w.conversationId),active:w.state==='active',...(w.state!=='active'?{deactivationReason:w.state}:{})}})),
  events:s.journal.events.filter(e=>e.epoch===team.epoch).slice(-80).map(e=>({seq:e.seq,...(e.at?{ts:Date.parse(e.at)}:{}),actor:e.actor,kind:e.kind,...(e.taskId?{task:e.taskId}:{}),...(e.worker?{worker:e.worker}:{}),...(e.text?{detail:e.text}:{})})),
  alerts:Object.values(s.outbox.alerts).slice(-30).map(a=>({id:a.id,ts:Date.parse(a.receivedAt),from:a.from,to:a.recipients.map(r=>r.name).join(', '),kind:a.kind,text:a.text,...(a.task_id?{task_id:a.task_id}:{})})),
  deliveries:Object.values(s.outbox.obligations).filter(o=>o.epoch===team.epoch).slice(-60).map(o=>({id:o.key,kind:o.kind==='stop'?'update':o.kind,ref:o.taskId??o.alertId??'',to:o.recipient,toConversationId:String(o.conversationId),acceptedAt:o.acceptedAt,...(o.submittedAt!==undefined?{submittedAt:o.submittedAt}:{}),...(o.submissionId!==undefined?{submissionId:String(o.submissionId)}:{}),...(o.settledAt!==undefined?{settledAt:o.settledAt}:{}),...(o.outcome?{settled:o.outcome}:o.state==='retired'&&o.submissionId===undefined?{settled:'retired'}:{})})),
 };
}
