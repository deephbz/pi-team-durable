import { type Result,type ModelRole,type Alert } from './state.ts';
import { taskCards,alertRecord,type Authority as State } from './store.ts';
/** Read only the requested Alert records in the current Team epoch. */
export async function currentAlerts(s:State,ids:readonly string[]):Promise<Alert[]> {
 const epoch=s.team.current?.epoch;if(epoch===undefined)return [];
 return (await Promise.all(ids.map(id=>alertRecord(s,id)))).filter((a):a is Alert=>!!a&&a.id.startsWith(`${epoch}:`));
}
export async function snapshotResult(s:State,modelRoles:ModelRole[],defaultModelRole?:string):Promise<Result> {
 const team=s.team.current!,cards=await taskCards(s);
 const received=await currentAlerts(s,[...s.head.leadAlerts].reverse());
 return {kind:'snapshot',team:{name:team.name,purpose:team.purpose,lifecycle:'active'},model_roles:modelRoles.map(r=>({name:r.name,use:r.use})),...(defaultModelRole?{default_model_role:defaultModelRole}:{}),
  workers:Object.values(team.workers).map(w=>({name:w.name,scope:w.scope,carrier:w.state==='active'?'connected':'absent',model_role:w.role.name,nonterminal_task_ids:cards.filter(t=>t.assignee===w.name&&!['goal_achieved','goal_failed','cancelled'].includes(t.status)).map(t=>t.id)})),
  tasks:cards,alerts:received.slice(0,10).map(a=>{const chars=Array.from(a.text);return {alert_id:a.id,from:a.from,kind:a.kind,...(a.task_id?{task_id:a.task_id}:{}),...(a.task_version?{task_version:a.task_version}:{}),received_at:a.receivedAt,text:chars.slice(0,2000).join(''),...(chars.length>2000?{truncated:true,text_length:chars.length}:{})};}),alerts_omitted:Math.max(0,s.head.leadAlertCount-10)};
}
