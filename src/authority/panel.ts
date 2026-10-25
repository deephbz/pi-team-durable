// Operator projection over current authority records. No controller-history fold.
import type { Tx } from '@earendil-works/pi-durable';
import { plain, type Obligation } from './state.ts';
import { openAuthority, taskRecord, taskIds, recentEvents, obligation } from './store.ts';
import { currentAlerts } from './projection.ts';

export async function readTeamSummary(tx: Tx) {
 const s=await openAuthority(tx);
 return {team:plain(s.team),attention:plain(s.head.attention),taskCount:s.head.taskCount};
}

export async function readTeamPanel(tx: Tx, storeId: string) {
 const s=await openAuthority(tx),team=s.team.current;
 if(!team)return {team:null};
 const tasks=(await Promise.all((await taskIds(s)).map(async id=>(await taskRecord(s,id))!.operator)))
  .map(plain).sort((a,b)=>a.key.localeCompare(b.key));
 const alerts=await currentAlerts(s,s.head.recentAlerts);
 const deliveries=(await Promise.all(s.head.recentObligations.map(key=>obligation(s,key)))).filter((o):o is Obligation=>!!o&&o.epoch===team.epoch);
 const alertById=new Map(alerts.map(a=>[a.id,a]));
 const missing=[...new Set(deliveries.filter(o=>o.kind==='alert'&&o.alertId&&!alertById.has(o.alertId)).map(o=>o.alertId!))];
 for(const a of await currentAlerts(s,missing))alertById.set(a.id,a);
 const journal=(await recentEvents(s)).filter(e=>e.epoch===team.epoch).slice(-80);
 const records=[
  ...alerts.map((a)=>({kind:'alert' as const,id:a.id,at:Number.isFinite(Date.parse(a.receivedAt))?Date.parse(a.receivedAt):null,from:a.from,to:a.recipients.map(r=>r.name),alertKind:a.kind,text:a.text,...(a.task_id?{task:a.task_id}:{}),status:'accepted'})),
  ...deliveries.map((o)=>({kind:'delivery' as const,id:o.key,at:o.settledAt??o.submittedAt??o.acceptedAt??null,
   from:o.kind==='alert'?(alertById.get(o.alertId??'')?.from??null):'authority',to:[o.recipient],
   deliveryKind:o.kind,ref:o.alertId??o.taskId??'',status:o.state==='retired'?'retired':o.state==='settled'?'settled':o.submissionId!==undefined||o.submittedAt!==undefined?'submitted':'accepted',
   conversationId:String(o.conversationId),generation:o.generation,
   ...(o.acceptedAt!==undefined?{acceptedAt:o.acceptedAt}:{}),...(o.submittedAt!==undefined?{submittedAt:o.submittedAt}:{}),
   ...(o.settledAt!==undefined?{settledAt:o.settledAt}:{}),...(o.submissionId!==undefined?{submissionId:String(o.submissionId)}:{}),...(o.outcome?{outcome:o.outcome}:{})})),
  ...journal.filter(e=>['team_created','team_stopped','worker_created','worker_stopped','graph_applied','task_transitioned'].includes(e.kind)).map((e)=>({
   kind:'lead-update' as const,id:String(e.seq),at:e.at?Date.parse(e.at):null,from:e.actor,to:['team-lead'],eventKind:e.kind,
   ...(e.text?{text:e.text}:{}),...(e.taskId?{task:e.taskId}:{}),...(e.taskIds?{tasks:e.taskIds}:{}),...(e.worker?{worker:e.worker}:{}),status:'recorded'})),
 ];
 const messages={availability:'available' as const,source:`durable:${storeId}`,epoch:team.epoch,limits:{alerts:30,deliveries:60,events:80},records};
 return {
  messages,
  team:{name:team.name,purpose:team.purpose,status:team.lifecycle,createdAt:team.createdAt,leadConversationId:String(team.lead)},
  leadThreadId:`${storeId}.${team.lead}`,graphVersion:s.head.graph?.revision?.version??'',tasks,
  workers:Object.values(team.workers).map(w=>({name:w.name,scope:w.scope,modelRole:w.role.name,createdAt:w.createdAt,threadId:`${storeId}.${w.conversationId}`,membership:{id:`${team.epoch}:${w.name}:g${w.generation}`,generation:w.generation,conversationId:String(w.conversationId),active:w.state==='active',...(w.state!=='active'?{deactivationReason:w.state}:{})}})),
  events:journal.map(e=>({seq:e.seq,...(e.at?{ts:Date.parse(e.at)}:{}),actor:e.actor,kind:e.kind,...(e.taskId?{task:e.taskId}:{}),...(e.worker?{worker:e.worker}:{}),...(e.text?{detail:e.text}:{})})),
  alerts:alerts.map(a=>({id:a.id,ts:Date.parse(a.receivedAt),from:a.from,to:a.recipients.map(r=>r.name).join(', '),kind:a.kind,text:a.text,...(a.task_id?{task_id:a.task_id}:{})})),
  deliveries:deliveries.map(o=>({id:o.key,kind:o.kind==='stop'?'update':o.kind,ref:o.taskId??o.alertId??'',to:o.recipient,toConversationId:String(o.conversationId),acceptedAt:o.acceptedAt,...(o.submittedAt!==undefined?{submittedAt:o.submittedAt}:{}),...(o.submissionId!==undefined?{submissionId:String(o.submissionId)}:{}),...(o.settledAt!==undefined?{settledAt:o.settledAt}:{}),...(o.outcome?{settled:o.outcome}:o.state==='retired'&&o.submissionId===undefined?{settled:'retired'}:{})})),
 };
}
