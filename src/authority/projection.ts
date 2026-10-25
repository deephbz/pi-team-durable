import { GraphTaskController } from './graph-control.ts';
import { projectGraphTaskCards } from './graph-card.ts';
import { plain,type State,type Result,type ModelRole } from './state.ts';
/** Alert IDs carry their Team epoch; consumers do not parse that identity. */
export function currentAlerts(s:State) {
 const epoch=s.team.current?.epoch;
 return epoch===undefined?[]:Object.values(s.outbox.alerts).filter(a=>a.id.startsWith(`${epoch}:`));
}
export function snapshotResult(s:State,modelRoles:ModelRole[],defaultModelRole?:string):Result {
 const team=s.team.current!,graph=GraphTaskController.recover(plain(s.graph.snapshot!));
 const received=currentAlerts(s).filter(a=>a.recipients.some(r=>r.name==='team-lead'&&r.conversationId===team.lead)).reverse();
 return {kind:'snapshot',team:{name:team.name,purpose:team.purpose,lifecycle:'active'},model_roles:modelRoles.map(r=>({name:r.name,use:r.use})),...(defaultModelRole?{default_model_role:defaultModelRole}:{}),
  workers:Object.values(team.workers).map(w=>({name:w.name,scope:w.scope,carrier:w.state==='active'?'connected':'absent',model_role:w.role.name,nonterminal_task_ids:graph.readTasks().filter(t=>t.assignee===w.name&&!['goal_achieved','goal_failed','cancelled'].includes(t.state.kind)).map(t=>t.id)})),
  tasks:projectGraphTaskCards(graph),alerts:received.slice(0,10).map(a=>{const chars=Array.from(a.text);return {alert_id:a.id,from:a.from,kind:a.kind,...(a.task_id?{task_id:a.task_id}:{}),...(a.task_version?{task_version:a.task_version}:{}),received_at:a.receivedAt,text:chars.slice(0,2000).join(''),...(chars.length>2000?{truncated:true,text_length:chars.length}:{})};}),alerts_omitted:Math.max(0,received.length-10)};
}
